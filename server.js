const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');
const fs = require('fs');

const app = express();
const server = http.createServer(app);
const PORT = process.env.PORT || 3000;
const STATE_FILE = '/var/data/deriv_state.json';

app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json());

// ---------- SSE ----------
const sseClients = new Set();
let logId = 1;

function addLog(msg) {
  const entry = { id: logId++, time: new Date().toISOString(), message: msg };
  state.logs.unshift(entry);
  if (state.logs.length > 200) state.logs.pop();
  broadcastSSE({ logs: [entry], state: sanitizeState() });
}

function broadcastSSE(payload) {
  sseClients.forEach(c => c.write(`data: ${JSON.stringify(payload)}\n\n`));
}

function sanitizeState() {
  const { logs, ...rest } = state;
  return rest;
}

// ---------- Market ----------
const MARKET = { sym: 'R_75', name: 'Volatility 75 Index', dp: 4 };
const FIXED_BARRIER = 5;
const MIN_CONFIDENCE = 100;
const DIGIT_SUM_TARGET = 23;
const HOUSE_EDGE = 0.98;
const TREND_FILTER = true;

const BASE_STAKE = 0.35;
const MARTINGALE = 1.5;
const COOLDOWN_TICKS = 15;
const SETTLE_TICKS = 15;
const WARMUP_TICKS = 500;

const TP_PERCENT = 5;
const SL_PERCENT = 10;

// ---------- Analyzer ----------
class Analyzer {
  constructor(dp) {
    this.dp = dp;
    this.shortTicks = [];
    this.shortCount = 0;
    this.shortMean = new Array(10).fill(0);
    this.shortM2 = new Array(10).fill(0);

    this.longTicks = [];
    this.longCount = 0;
    this.longMean = new Array(10).fill(0);
    this.longM2 = new Array(10).fill(0);

    this.prices = [];
  }

  feed(price) {
    const digit = parseInt(parseFloat(price).toFixed(this.dp).slice(-1));
    this.shortTicks.push(digit);
    if (this.shortTicks.length > 1000) this.shortTicks.shift();
    if (this.shortTicks.length >= 100) {
      const recent = this.shortTicks.slice(-100);
      const freq = {};
      for (let d = 0; d < 10; d++) freq[d] = recent.filter(x => x === d).length / 100;
      this.shortCount++;
      for (let d = 0; d < 10; d++) {
        const delta = freq[d] - this.shortMean[d];
        this.shortMean[d] += delta / this.shortCount;
        this.shortM2[d] += delta * (freq[d] - this.shortMean[d]);
      }
    }

    this.longTicks.push(digit);
    if (this.longTicks.length > 2000) this.longTicks.shift();
    if (this.longTicks.length >= 500) {
      const recent = this.longTicks.slice(-500);
      const freq = {};
      for (let d = 0; d < 10; d++) freq[d] = recent.filter(x => x === d).length / 500;
      this.longCount++;
      for (let d = 0; d < 10; d++) {
        const delta = freq[d] - this.longMean[d];
        this.longMean[d] += delta / this.longCount;
        this.longM2[d] += delta * (freq[d] - this.longMean[d]);
      }
    }

    this.prices.push(price);
    if (this.prices.length > 500) this.prices.shift();
  }

  getAnalysis() {
    if (this.shortCount < 5 || this.longCount < 2) return null;
    const shortRecent = this.shortTicks.slice(-100);
    const shortFreq = {};
    for (let d = 0; d < 10; d++) shortFreq[d] = shortRecent.filter(x => x === d).length / 100;

    const longRecent = this.longTicks.slice(-500);
    const longFreq = {};
    for (let d = 0; d < 10; d++) longFreq[d] = longRecent.filter(x => x === d).length / 500;

    const zShort = {}, zLong = {};
    for (let d = 0; d < 10; d++) {
      const vs = this.shortM2[d] / (this.shortCount - 1), ss = Math.sqrt(vs) || 0.01;
      zShort[d] = (shortFreq[d] - this.shortMean[d]) / ss;
      const vl = this.longM2[d] / (this.longCount - 1), sl = Math.sqrt(vl) || 0.01;
      zLong[d] = (longFreq[d] - this.longMean[d]) / sl;
    }

    let underConf = 0;
    if (zShort[8] < 0 && zShort[9] < 0 && zLong[8] < 0 && zLong[9] < 0) {
      const rareShort = Math.min(3, Math.max(0, -Math.min(zShort[8], zShort[9]))) / 3;
      const rareLong  = Math.min(3, Math.max(0, -Math.min(zLong[8], zLong[9]))) / 3;
      underConf = (rareShort * 0.4 + rareLong * 0.6 + 0.4) * 100;
    }

    let slope = 0;
    if (this.prices.length >= 20) {
      const rp = this.prices.slice(-20);
      const n = rp.length;
      let sx = 0, sy = 0, sxy = 0, sx2 = 0;
      for (let i = 0; i < n; i++) {
        sx += i; sy += rp[i]; sxy += i * rp[i]; sx2 += i * i;
      }
      slope = (n * sxy - sx * sy) / (n * sx2 - sx * sx);
    }

    return { underConf, slope };
  }
}

const analyzer = new Analyzer(MARKET.dp);

// ---------- State ----------
const state = {
  active: false,
  balance: null,
  currency: 'USD',
  dailyStartBalance: null,
  dailyPnl: 0,
  locked: false,
  lockReason: '',
  warmupComplete: false,
  warmupTicksFed: 0,
  liveSubscribed: false,
  accountType: 'Unknown', // Added to trace demo vs real context

  tradeInProgress: false,
  activeRealTrade: null,
  settleTicksRemaining: 0,
  currentStake: BASE_STAKE,
  cooldownTicksLeft: 0,

  logs: [],
  sessionAlreadyUsedToday: false
};

// ---------- Persistence ----------
function saveState() {
  try {
    const dir = path.dirname(STATE_FILE);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(STATE_FILE, JSON.stringify({
      date: new Date().toISOString().slice(0,10),
      dailyStartBalance: state.dailyStartBalance,
      dailyPnl: state.dailyPnl,
      locked: state.locked,
      lockReason: state.lockReason,
      sessionActive: state.active
    }));
  } catch(e) {}
}

function loadState() {
  try {
    if (fs.existsSync(STATE_FILE)) {
      const saved = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
      const today = new Date().toISOString().slice(0,10);
      if (saved.date === today && saved.sessionActive) {
        state.sessionAlreadyUsedToday = true;
        state.locked = true;
        state.lockReason = 'Session already used today.';
        addLog(state.lockReason);
      }
      if (saved.date === today) {
        state.dailyStartBalance = saved.dailyStartBalance;
        state.dailyPnl = saved.dailyPnl || 0;
        if (saved.locked) { state.locked = true; state.lockReason = saved.lockReason || ''; }
      }
    }
  } catch(e) {}
}

// ---------- Helpers ----------
function getTP() {
  return state.dailyStartBalance ? (state.dailyStartBalance * TP_PERCENT / 100) : 0;
}

function getSL() {
  return state.dailyStartBalance ? (state.dailyStartBalance * SL_PERCENT / 100) : 0;
}

function checkDailyLimits() {
  if (!state.dailyStartBalance) return false;
  if (state.dailyPnl >= getTP()) {
    state.locked = true;
    state.lockReason = `Take-profit $${getTP().toFixed(2)} (${TP_PERCENT}%) reached.`;
    addLog(state.lockReason);
    return true;
  }
  if (state.dailyPnl <= -getSL()) {
    state.locked = true;
    state.lockReason = `Stop-loss $${getSL().toFixed(2)} (${SL_PERCENT}%) hit.`;
    addLog(state.lockReason);
    return true;
  }
  return false;
}

function digitSum(price, dp) {
  const formatted = parseFloat(price).toFixed(dp);
  const dec = formatted.split('.')[1] || '';
  let sum = 0;
  for (const ch of dec) sum += parseInt(ch);
  return sum;
}

// ---------- Real trade settlement ----------
function settleRealTrade() {
  if (!state.activeRealTrade || state.balance == null) return;
  const profit = state.balance - state.activeRealTrade.balanceBefore;
  state.dailyPnl += profit;
  const result = profit > 0 ? 'WIN' : (profit < 0 ? 'LOSS' : 'DRAW');
  addLog(`REAL ${result}: ${profit.toFixed(2)} | Daily P&L: ${state.dailyPnl.toFixed(2)}`);

  if (profit > 0) {
    state.currentStake = BASE_STAKE;
  } else if (profit < 0) {
    state.currentStake = Math.min(state.currentStake * MARTINGALE, state.balance);
  }

  state.tradeInProgress = false;
  state.activeRealTrade = null;
  state.settleTicksRemaining = 0;
  state.cooldownTicksLeft = COOLDOWN_TICKS;

  checkDailyLimits();
  saveState();
  broadcastSSE({ state: sanitizeState() });
}

// ---------- Tick processing ----------
function processTick(price) {
  analyzer.feed(price);

  const analysis = analyzer.getAnalysis();
  if (!state.active || state.locked || !state.warmupComplete) return;

  if (state.settleTicksRemaining > 0) {
    state.settleTicksRemaining--;
    if (state.settleTicksRemaining === 0) settleRealTrade();
    broadcastSSE({ state: sanitizeState() });
    return;
  }

  if (state.cooldownTicksLeft > 0) { state.cooldownTicksLeft--; return; }
  if (state.tradeInProgress) return;

  if (!analysis || analysis.underConf < MIN_CONFIDENCE) return;
  if (digitSum(price, MARKET.dp) <= DIGIT_SUM_TARGET) return;
  const lastDigit = parseInt(parseFloat(price).toFixed(MARKET.dp).slice(-1));
  if (lastDigit < 5) return;
  if (TREND_FILTER && analysis.slope > 0.0001) return;

  state.tradeInProgress = true;
  const rawStake = Math.min(state.currentStake, state.balance);
  const stake = Math.round(rawStake * 100) / 100;
  addLog(`REAL entry – stake $${stake.toFixed(2)} (conf ${analysis.underConf.toFixed(1)}%, sum ${digitSum(price, MARKET.dp)})`);

  state.activeRealTrade = { stake, balanceBefore: state.balance };
  
  send({
    proposal: 1, amount: stake, basis: 'stake', currency: state.currency || 'USD',
    duration: 1, duration_unit: 't', symbol: MARKET.sym,
    contract_type: 'DIGITUNDER', barrier: FIXED_BARRIER, req_id: ++reqId
  });

  broadcastSSE({ state: sanitizeState() });
}

// ---------- WebSocket connection ----------
let derivWs = null;
let reqId = 0;

function send(msg) {
  if (derivWs && derivWs.readyState === WebSocket.OPEN) derivWs.send(JSON.stringify(msg));
}

async function connectDeriv() {
  if (!state.active) return;
  
  const appId = process.env.DERIV_APP_ID;
  const token = process.env.DERIV_PAT;

  if (!appId || !token) {
    addLog('Connection error: Missing DERIV_APP_ID or DERIV_PAT configuration.');
    return;
  }

  try {
    addLog('Connecting to Deriv WebSocket production endpoint...');
    derivWs = new WebSocket(`wss://ws.derivws.com/websockets/v3?app_id=${appId}`);

    derivWs.on('open', () => {
      addLog('WebSocket open. Sending authorization request...');
      send({ authorize: token, req_id: ++reqId });
    });

    derivWs.on('message', data => { try { handleMessage(JSON.parse(data)); } catch(e) {} });
    
    derivWs.on('close', () => {
      addLog('WebSocket closed.');
      if (state.active) setTimeout(connectDeriv, 5000);
    });
    
    derivWs.on('error', err => addLog(`WebSocket error: ${err.message}`));
  } catch (e) {
    addLog(`Connection error: ${e.message}. Retrying in 10s…`);
    if (state.active) setTimeout(connectDeriv, 10000);
  }
}

function handleMessage(msg) {
  if (msg.error) {
    addLog(`Deriv error: ${msg.error.message}`);
    if (state.tradeInProgress) {
      state.tradeInProgress = false; state.activeRealTrade = null; state.settleTicksRemaining = 0;
      addLog('Trade aborted due to API error – bot will retry on next signal.');
    }
    return;
  }

  // FIXED: Detect context environment (Demo vs Real) dynamically 
  if (msg.msg_type === 'authorize') {
    state.accountType = msg.authorize.is_virtual ? '🧪 DEMO / VIRTUAL' : '⚠️ LIVE / REAL ACCOUNT';
    addLog(`✅ Session Authorized for account: ${msg.authorize.loginid} (${state.accountType})`);
    
    send({ balance: 1, subscribe: 1, req_id: ++reqId });
    send({ ticks_history: MARKET.sym, count: WARMUP_TICKS, end: 'latest', req_id: ++reqId });
  }
  else if (msg.msg_type === 'balance') {
    state.balance = parseFloat(msg.balance.balance);
    state.currency = msg.balance.currency;
    broadcastSSE({ state: sanitizeState() });
  }
  else if (msg.msg_type === 'history') {
    if (msg.history && msg.history.prices) {
      state.warmupTicksFed += msg.history.prices.length;
      addLog(`Warming up (${state.warmupTicksFed} / ${WARMUP_TICKS} ticks)...`);
      for (const p of msg.history.prices) analyzer.feed(p);
      if (state.warmupTicksFed >= WARMUP_TICKS && !state.liveSubscribed) {
        state.warmupComplete = true;
        state.liveSubscribed = true;
        addLog('✅ Warm‑up complete – subscribing to live ticks.');
        send({ ticks: MARKET.sym, req_id: ++reqId });
      }
      broadcastSSE({ state: sanitizeState() });
    }
  }
  else if (msg.msg_type === 'tick') {
    if (msg.tick.symbol !== MARKET.sym) return;
    processTick(parseFloat(msg.tick.quote));
    broadcastSSE({ state: sanitizeState() });
  }
  else if (msg.msg_type === 'proposal') {
    send({ buy: msg.proposal.id, price: msg.proposal.ask_price, req_id: ++reqId });
  }
  else if (msg.msg_type === 'buy') {
    addLog(`Contract bought – ID ${msg.buy.contract_id}`);
    if (state.activeRealTrade) state.settleTicksRemaining = SETTLE_TICKS;
  }
}

// ---------- API ----------
app.get('/api/logs', (req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', Connection: 'keep-alive' });
  res.write('\n'); sseClients.add(res);
  res.write(`data: ${JSON.stringify({ state: sanitizeState() })}\n\n`);
  req.on('close', () => sseClients.delete(res));
});

app.get('/api/state', (req, res) => res.json({ ...state, logs: undefined }));

app.post('/api/control', (req, res) => {
  const { action } = req.body;
  if (action === 'start') {
    if (state.sessionAlreadyUsedToday) return res.status(403).json({ error: 'Session already used today.' });
    state.active = true; state.locked = false;
    state.dailyStartBalance = state.balance; state.dailyPnl = 0; state.lockReason = '';
    state.warmupComplete = false; state.warmupTicksFed = 0; state.liveSubscribed = false;
    state.currentStake = BASE_STAKE; state.cooldownTicksLeft = 0;
    state.tradeInProgress = false; state.activeRealTrade = null; state.settleTicksRemaining = 0;
    addLog(`Under‑6 bot started (TP ${TP_PERCENT}%, SL ${SL_PERCENT}%).`);
    connectDeriv();
    saveState();
  } else if (action === 'stop') {
    state.active = false;
    if (derivWs) derivWs.close();
    state.tradeInProgress = false; state.activeRealTrade = null; state.settleTicksRemaining = 0;
    addLog('Trading stopped.');
    saveState();
  }
  broadcastSSE({ state: sanitizeState() }); res.json({ success: true });
});

loadState();
server.listen(PORT, () => console.log(`Under‑6 PAT bot running on port ${PORT}`));
