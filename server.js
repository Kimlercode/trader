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
  R_10:  { name: 'Volatility 10 Index',  dp: 3, zoneLimit: 499 },
  R_25:  { name: 'Volatility 25 Index',  dp: 3, zoneLimit: 499 },
  R_50:  { name: 'Volatility 50 Index',  dp: 4, zoneLimit: 499 },
  R_75:  { name: 'Volatility 75 Index',  dp: 4, zoneLimit: 499 },
  R_100: { name: 'Volatility 100 Index', dp: 2, zoneLimit: 49  },
};

const FIXED_BARRIER = 4;
const MIN_CONFIDENCE = 100;
const HOUSE_EDGE = 0.98;
const TREND_FILTER = true;

const BASE_STAKE = 0.35;
const MARTINGALE = 1.85;
const VIRTUAL_LOSSES_NEEDED = 3;          // as you requested
const COOLDOWN_TICKS = 5;
const DAILY_PROFIT_CAP = 3.00;
const DAILY_STOP_LOSS = 5.00;
const SETTLE_TICKS = 15;                  // ticks to wait for balance update

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

  marketStates: {},
  realTradeInProgress: false,
  activeRealTrade: null,            // { market, stake, balanceBefore }
  settleTicksRemaining: 0,

  marketSignals: {},
  logs: [],
  sessionAlreadyUsedToday: false
};

for (const sym of Object.keys(MARKETS)) {
  state.marketStates[sym] = {
    mode: 'virtual',
    virtualLosses: 0,
    stake: BASE_STAKE,
    cooldownTicksLeft: 0,
    waitingForOutcome: false,
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
        if (saved.marketStates) {
          for (const sym of Object.keys(MARKETS)) {
            if (saved.marketStates[sym]) state.marketStates[sym] = saved.marketStates[sym];
          }
        }
      }
    }
  } catch(e) {}
}

// ---------- Helpers ----------
function checkDailyLimits() {
  if (!state.dailyStartBalance) return false;
  if (state.dailyPnl >= DAILY_PROFIT_CAP) {
    state.locked = true;
    state.lockReason = `Daily profit target $${DAILY_PROFIT_CAP} reached.`;
    addLog(state.lockReason);
    return true;
  }
  if (state.dailyPnl <= -DAILY_STOP_LOSS) {
    state.locked = true;
    state.lockReason = `Daily stop-loss $${DAILY_STOP_LOSS} hit.`;
    addLog(state.lockReason);
    return true;
  }
  return false;
}

function inZone(price, dp, limit) {
  const s = parseFloat(price).toFixed(dp);
  const dec = s.split('.')[1] || '';
  if (dp === 2) {
    const two = parseInt(dec.slice(-2));
    return !isNaN(two) && two <= limit;
  } else {
    const three = parseInt(dec.slice(-3));
    return !isNaN(three) && three <= limit;
  }
}

// ---------- Virtual outcome resolution ----------
function resolveVirtualOutcome(sym, currentPrice) {
  const ms = state.marketStates[sym];
  if (!ms.waitingForOutcome) return;
  ms.waitingForOutcome = false;

  const lastDigit = parseInt(parseFloat(currentPrice).toFixed(MARKETS[sym].dp).slice(-1));
  const win = lastDigit > FIXED_BARRIER;

  if (win) {
    ms.virtualLosses = 0;
    addLog(`${MARKETS[sym].name} VIRTUAL WIN – losses reset`);
  } else {
    ms.virtualLosses++;
    addLog(`${MARKETS[sym].name} VIRTUAL LOSS (${ms.virtualLosses}/${VIRTUAL_LOSSES_NEEDED})`);
    if (ms.virtualLosses >= VIRTUAL_LOSSES_NEEDED) {
      ms.mode = 'real';
      // martingale stake preserved
      addLog(`${MARKETS[sym].name} → REAL mode (stake $${ms.stake.toFixed(2)})`);
    }
  }
  ms.cooldownTicksLeft = COOLDOWN_TICKS;
}

// ---------- Real trade settlement (tick‑based balance change) ----------
function settleRealTrade() {
  if (!state.activeRealTrade || state.balance == null) return;
  const trade = state.activeRealTrade;
  const profit = state.balance - trade.balanceBefore;
  state.dailyPnl += profit;
  const result = profit > 0 ? 'WIN' : (profit < 0 ? 'LOSS' : 'DRAW');
  addLog(`${MARKETS[trade.market].name} REAL ${result}: ${profit.toFixed(2)} | Daily P&L: ${state.dailyPnl.toFixed(2)}`);

  const ms = state.marketStates[trade.market];
  if (profit > 0) {
    ms.stake = BASE_STAKE;
    ms.mode = 'virtual';
    ms.virtualLosses = 0;
  } else if (profit < 0) {
    ms.stake = Math.min(ms.stake * MARTINGALE, state.balance);
    // stay real until a win (original behaviour)
  } else {
    // draw – go back to virtual, stake unchanged
    ms.mode = 'virtual';
    ms.virtualLosses = 0;
  }

  state.realTradeInProgress = false;
  state.activeRealTrade = null;
  state.settleTicksRemaining = 0;
  ms.cooldownTicksLeft = COOLDOWN_TICKS;

  checkDailyLimits();
  saveState();
  broadcastSSE({ state: sanitizeState() });
}

// ---------- Tick processing ----------
function processTick(sym, price) {
  const market = MARKETS[sym];
  const analyzer = analyzers[sym];
  analyzer.feed(price, market.dp);

  const analysis = analyzer.getAnalysis();
  if (analysis) {
    state.marketSignals[sym] = {
      overConf: analysis.overConf.toFixed(1),
      trend: analysis.slope > 0.0001 ? 'up' : (analysis.slope < -0.0001 ? 'down' : 'flat')
    };
  }

  if (!state.active || state.locked) return;

  // 1. If we are counting ticks for settlement, handle that first
  if (state.settleTicksRemaining > 0) {
    state.settleTicksRemaining--;
    if (state.settleTicksRemaining === 0) {
      settleRealTrade();
    }
    broadcastSSE({ state: sanitizeState() });
    return;
  }

  const ms = state.marketStates[sym];

  // 2. Resolve pending virtual outcome
  if (ms.waitingForOutcome) {
    resolveVirtualOutcome(sym, price);
    broadcastSSE({ state: sanitizeState() });
    return;
  }

  // 3. Cooldown
  if (ms.cooldownTicksLeft > 0) {
    ms.cooldownTicksLeft--;
    return;
  }

  // 4. Global lock – only one real trade at a time
  if (state.realTradeInProgress) return;

  // 5. Signal checks
  if (!analysis || analysis.overConf < MIN_CONFIDENCE) return;
  if (!inZone(price, market.dp, market.zoneLimit)) return;
  if (TREND_FILTER && analysis.slope < -0.0001) return;

  if (ms.mode === 'virtual') {
    ms.waitingForOutcome = true;
    addLog(`${market.name} VIRTUAL entry – awaiting next tick`);
  } else {
    // Real trade
    state.realTradeInProgress = true;
    const rawStake = Math.min(ms.stake, state.balance);
    const stake = Math.round(rawStake * 100) / 100;
    const contractType = 'DIGITOVER';

    addLog(`${market.name} REAL entry – stake ${stake.toFixed(2)}`);

    state.activeRealTrade = {
      market: sym,
      stake,
      balanceBefore: state.balance,   // captured before proposal
    };

    send({
      proposal: 1,
      amount: stake,
      basis: 'stake',
      currency: state.currency || 'USD',
      duration: 1,
      duration_unit: 't',
      symbol: sym,
      contract_type: contractType,
      barrier: FIXED_BARRIER,
      req_id: ++reqId
    });
  }

  broadcastSSE({ state: sanitizeState() });
}

// ---------- Deriv WebSocket (legacy token auth) ----------
let derivWs = null;
let reqId = 0;

function send(msg) {
  if (derivWs && derivWs.readyState === WebSocket.OPEN) derivWs.send(JSON.stringify(msg));
}

function connectDeriv() {
  if (derivWs) derivWs.close();
  const appId = process.env.DERIV_APP_ID;
  derivWs = new WebSocket(`wss://ws.binaryws.com/websockets/v3?app_id=${appId}`);
  derivWs.on('open', () => {
    addLog('Connected. Authorizing...');
    send({ authorize: process.env.DERIV_API_TOKEN });
  });
  derivWs.on('message', data => {
    try { handleMessage(JSON.parse(data)); } catch(e) {}
  });
  derivWs.on('close', () => setTimeout(connectDeriv, 5000));
  derivWs.on('error', err => addLog(`WebSocket error: ${err.message}`));
}

function handleMessage(msg) {
  if (msg.error) {
    addLog(`Deriv error: ${msg.error.message}`);
    if (state.realTradeInProgress) {
      state.realTradeInProgress = false;
      state.activeRealTrade = null;
      state.settleTicksRemaining = 0;
      addLog('Trade aborted due to API error – bot will retry on next signal.');
    }
    return;
  }

  if (msg.msg_type === 'authorize') {
    addLog('Authorized. Subscribing to balance & all ticks.');
    send({ balance: 1, subscribe: 1, req_id: ++reqId });
    for (const sym of Object.keys(MARKETS)) {
      send({ ticks_history: sym, count: 1000, end: 'latest', req_id: ++reqId });
    }
  }
  else if (msg.msg_type === 'balance') {
    state.balance = parseFloat(msg.balance.balance);
    state.currency = msg.balance.currency;
    broadcastSSE({ state: sanitizeState() });
  }
  else if (msg.msg_type === 'history') {
    const sym = msg.echo_req.ticks_history;
    const analyzer = analyzers[sym];
    if (analyzer && msg.history && msg.history.prices) {
      for (const p of msg.history.prices) analyzer.feed(p, MARKETS[sym].dp);
      send({ ticks: sym, req_id: ++reqId });
    }
  }
  else if (msg.msg_type === 'tick') {
    const sym = msg.tick.symbol;
    if (!MARKETS[sym]) return;
    processTick(sym, parseFloat(msg.tick.quote));
    broadcastSSE({ state: sanitizeState() });
  }
  else if (msg.msg_type === 'proposal') {
    send({ buy: msg.proposal.id, price: msg.proposal.ask_price, req_id: ++reqId });
  }
  else if (msg.msg_type === 'buy') {
    addLog(`Contract bought – ID ${msg.buy.contract_id}`);
    if (state.activeRealTrade) {
      // Start counting 15 ticks for settlement
      state.settleTicksRemaining = SETTLE_TICKS;
    }
  }
  // Ignore proposal_open_contract – we use balance change after tick wait
}

// ---------- API ----------
app.get('/api/logs', (req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', Connection: 'keep-alive' });
  res.write('\n');
  sseClients.add(res);
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
    for (const sym of Object.keys(MARKETS)) {
      state.marketStates[sym] = {
        mode: 'virtual', virtualLosses: 0, stake: BASE_STAKE,
        cooldownTicksLeft: 0, waitingForOutcome: false
      };
    }
    state.realTradeInProgress = false; state.activeRealTrade = null; state.settleTicksRemaining = 0;
    addLog('Multi-market z‑score bot started (legacy auth, VL=3, 15‑tick balance settlement).');
    saveState();
  } else if (action === 'stop') {
    state.active = false;
    state.realTradeInProgress = false; state.activeRealTrade = null; state.settleTicksRemaining = 0;
    addLog('Trading stopped.');
    saveState();
  }
  broadcastSSE({ state: sanitizeState() });
  res.json({ success: true });
});

loadState();
connectDeriv();
server.listen(PORT, () => console.log(`Multi-market VL bot on port ${PORT}`));
