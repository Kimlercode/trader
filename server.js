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

// ---------- Markets ----------
const MARKETS = {
  R_10:  { name: 'Volatility 10 Index',  dp: 3 },
  R_25:  { name: 'Volatility 25 Index',  dp: 3 },
  R_50:  { name: 'Volatility 50 Index',  dp: 4 },
  R_75:  { name: 'Volatility 75 Index',  dp: 4 },
  R_100: { name: 'Volatility 100 Index', dp: 2 },
};

const COOLDOWN_TICKS = 15;
const WARMUP_TICKS = 1000;
const TREND_FILTER = true;

// ---------- Analyzer (multi‑market) ----------
class Analyzer {
  constructor() {
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

  feed(price, dp) {
    const digit = parseInt(parseFloat(price).toFixed(dp).slice(-1));
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

    let overConf = 0;
    if (zShort[0] < 0 && zShort[1] < 0 && zLong[0] < 0 && zLong[1] < 0) {
      const rareShort = Math.min(3, Math.max(0, -Math.min(zShort[0], zShort[1]))) / 3;
      const rareLong  = Math.min(3, Math.max(0, -Math.min(zLong[0], zLong[1]))) / 3;
      overConf = (rareShort * 0.4 + rareLong * 0.6 + 0.4) * 100;
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

    return { overConf, slope };
  }
}

const analyzers = {};
for (const sym of Object.keys(MARKETS)) analyzers[sym] = new Analyzer();

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
  warmupMarketsRemaining: 0,
  liveSubscribed: false,

  marketStates: {},
  realTradeInProgress: false,
  activeRealTrade: null, // Now stores { market, stake, expectedProfit, isBought, tickCount }

  marketSignals: {},
  logs: [],
  sessionAlreadyUsedToday: false
};

for (const sym of Object.keys(MARKETS)) {
  state.marketStates[sym] = {
    cooldownTicksLeft: 0,
  };
}

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
      sessionActive: state.active,
      marketStates: state.marketStates
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
        if (saved.locked) {
          state.locked = true;
          state.lockReason = saved.lockReason || '';
        }
      }
    }
  } catch(e) {}
}

// ---------- Helpers ----------
function checkDailyLimits() {
  if (!state.dailyStartBalance) return false;
  
  const targetProfit = state.dailyStartBalance * 0.05;
  const stopLoss = state.dailyStartBalance * 0.10;

  // Added -0.001 to handle Javascript floating point inaccuracies safely
  if (state.dailyPnl >= (targetProfit - 0.001)) {
    state.locked = true;
    state.lockReason = `Daily 5% profit target ($${targetProfit.toFixed(2)}) reached.`;
    addLog(state.lockReason);
    return true;
  }
  if (state.dailyPnl <= -(stopLoss - 0.001)) {
    state.locked = true;
    state.lockReason = `Daily 10% stop-loss ($${stopLoss.toFixed(2)}) hit.`;
    addLog(state.lockReason);
    return true;
  }
  return false;
}

// ---------- Real trade settlement ----------
function settleRealTrade(exitDigit) {
  if (!state.activeRealTrade) return;
  const trade = state.activeRealTrade;
  const marketName = MARKETS[trade.market].name;
  
  // DIGITUNDER 6: Win if digit is 0, 1, 2, 3, 4, 5
  const isWin = exitDigit < 6; 
  const profit = isWin ? trade.expectedProfit : -trade.stake;
  
  state.dailyPnl += profit;
  
  // Manually update balance for UI instantly. The Deriv API stream will eventually overwrite this to keep it perfectly synced.
  if (isWin && state.balance != null) {
      state.balance += (trade.expectedProfit + trade.stake);
  }
  
  const resultStr = isWin ? 'WIN' : 'LOSS';
  addLog(`${marketName} REAL ${resultStr} (Exit Digit: ${exitDigit}) | PnL: $${profit.toFixed(2)} | Daily PnL: $${state.dailyPnl.toFixed(2)}`);

  state.realTradeInProgress = false;
  state.activeRealTrade = null;
  state.marketStates[trade.market].cooldownTicksLeft = COOLDOWN_TICKS;
  
  checkDailyLimits();
  saveState();
  broadcastSSE({ state: sanitizeState() });
}

// ---------- Tick processing ----------
function processTick(sym, price) {
  const market = MARKETS[sym];
  analyzers[sym].feed(price, market.dp);
  const analysis = analyzers[sym].getAnalysis();
  
  if (analysis) {
    state.marketSignals[sym] = {
      overConf: analysis.overConf.toFixed(1),
      trend: analysis.slope > 0.0001 ? 'up' : (analysis.slope < -0.0001 ? 'down' : 'flat')
    };
  }

  if (!state.active || state.locked || !state.warmupComplete) return;

  // Active Trade Tick Tracking (Digit Verification Method)
  if (state.realTradeInProgress && state.activeRealTrade && state.activeRealTrade.isBought) {
    if (sym === state.activeRealTrade.market) {
        state.activeRealTrade.tickCount++;
        if (state.activeRealTrade.tickCount === 2) {
            // This is the exit tick for a 2-tick trade
            const exitDigit = parseInt(parseFloat(price).toFixed(market.dp).slice(-1));
            settleRealTrade(exitDigit);
        }
    }
    return; // Block new entries while a trade is in progress
  }

  const ms = state.marketStates[sym];
  if (ms.cooldownTicksLeft > 0) { ms.cooldownTicksLeft--; return; }
  if (state.realTradeInProgress) return;
  
  // 1. Confidence between 70 to 100
  if (!analysis || analysis.overConf < 70 || analysis.overConf > 100) return;
  if (TREND_FILTER && analysis.slope < -0.0001) return;

  // 2. Entry condition: Last two consecutive digits are between 6 and 9
  const shortTicks = analyzers[sym].shortTicks;
  if (shortTicks.length < 2) return;
  const lastTwo = shortTicks.slice(-2);
  if (lastTwo[0] < 6 || lastTwo[0] > 9 || lastTwo[1] < 6 || lastTwo[1] > 9) return;

  // 3. Execution: 1% stake
  state.realTradeInProgress = true;
  let stake = Number((state.balance * 0.01).toFixed(2));
  
  addLog(`${market.name} REAL entry trigger – stake $${stake.toFixed(2)}`);
  
  // Initialize trade tracking object
  state.activeRealTrade = { 
    market: sym, 
    stake: stake, 
    expectedProfit: 0, 
    isBought: false, 
    tickCount: 0 
  };
  
  send({
    proposal: 1, 
    amount: stake, 
    basis: 'stake', 
    currency: state.currency || 'USD',
    duration: 2,           
    duration_unit: 't', 
    symbol: sym,
    contract_type: 'DIGITUNDER',
    barrier: 6,            
    req_id: ++reqId
  });
  
  broadcastSSE({ state: sanitizeState() });
}

// ---------- Deriv WebSocket ----------
let derivWs = null;
let reqId = 0;

function send(msg) {
  if (derivWs && derivWs.readyState === WebSocket.OPEN) derivWs.send(JSON.stringify(msg));
}

function connectDeriv() {
  if (!state.active) return;
  if (derivWs) derivWs.close();
  const appId = process.env.DERIV_APP_ID;
  derivWs = new WebSocket(`wss://ws.binaryws.com/websockets/v3?app_id=${appId}`);
  derivWs.on('open', () => {
    addLog('Connected. Authorizing...');
    send({ authorize: process.env.DERIV_API_TOKEN });
  });
  derivWs.on('message', data => { try { handleMessage(JSON.parse(data)); } catch(e) {} });
  derivWs.on('close', () => {
    addLog('WebSocket closed.');
    if (state.active) setTimeout(connectDeriv, 5000);
  });
  derivWs.on('error', err => addLog(`WebSocket error: ${err.message}`));
}

function handleMessage(msg) {
  if (msg.error) {
    addLog(`Deriv error: ${msg.error.message}`);
    if (state.realTradeInProgress) {
      state.realTradeInProgress = false; 
      state.activeRealTrade = null;
      addLog('Trade aborted due to API error – bot will retry.');
    }
    return;
  }

  if (msg.msg_type === 'authorize') {
    addLog('Authorized. Requesting history for warm‑up...');
    send({ balance: 1, subscribe: 1, req_id: ++reqId });
    state.warmupMarketsRemaining = Object.keys(MARKETS).length;
    state.warmupComplete = false;
    state.liveSubscribed = false;
    for (const sym of Object.keys(MARKETS)) {
      send({ ticks_history: sym, count: WARMUP_TICKS, end: 'latest', req_id: ++reqId });
    }
  }
  else if (msg.msg_type === 'balance') {
    state.balance = parseFloat(msg.balance.balance);
    state.currency = msg.balance.currency;
    broadcastSSE({ state: sanitizeState() });
  }
  else if (msg.msg_type === 'history') {
    const sym = msg.echo_req.ticks_history;
    if (analyzers[sym] && msg.history && msg.history.prices) {
      addLog(`Warming up ${MARKETS[sym].name} (${msg.history.prices.length} ticks)...`);
      for (const p of msg.history.prices) analyzers[sym].feed(p, MARKETS[sym].dp);
      state.warmupMarketsRemaining--;
      if (state.warmupMarketsRemaining <= 0 && !state.liveSubscribed) {
        state.warmupComplete = true;
        state.liveSubscribed = true;
        addLog('✅ All markets warmed up – subscribing to live ticks.');
        for (const s of Object.keys(MARKETS)) {
          send({ ticks: s, req_id: ++reqId });
        }
      }
      broadcastSSE({ state: sanitizeState() });
    }
  }
  else if (msg.msg_type === 'tick') {
    const sym = msg.tick.symbol;
    if (!MARKETS[sym]) return;
    processTick(sym, parseFloat(msg.tick.quote));
    broadcastSSE({ state: sanitizeState() });
  }
  else if (msg.msg_type === 'proposal') {
    // Capture the exact payout from Deriv before executing the buy
    if (state.activeRealTrade && !state.activeRealTrade.isBought) {
        state.activeRealTrade.expectedProfit = msg.proposal.payout - msg.proposal.ask_price;
        send({ buy: msg.proposal.id, price: msg.proposal.ask_price, req_id: ++reqId });
    }
  }
  else if (msg.msg_type === 'buy') {
    addLog(`Contract bought – tracking 2 ticks for settlement...`);
    if (state.activeRealTrade) {
        state.activeRealTrade.isBought = true;
        state.activeRealTrade.tickCount = 0;
    }
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
    state.warmupComplete = false; state.liveSubscribed = false;
    for (const sym of Object.keys(MARKETS)) {
      state.marketStates[sym] = { cooldownTicksLeft: 0 };
    }
    state.realTradeInProgress = false; state.activeRealTrade = null;
    addLog('Bot started – feeding historical ticks for warm‑up...');
    connectDeriv();
    saveState();
  } else if (action === 'stop') {
    state.active = false;
    if (derivWs) derivWs.close();
    state.realTradeInProgress = false; state.activeRealTrade = null;
    addLog('Trading stopped.');
    saveState();
  }
  broadcastSSE({ state: sanitizeState() }); res.json({ success: true });
});

loadState();
server.listen(PORT, () => console.log(`Multi-market Bot (Under 6) on port ${PORT}`));
