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

// ---------- Under-6 Institutional Configuration ----------
const MARKET = { sym: 'R_75', name: 'Volatility 75 Index', dp: 4 };
const FIXED_BARRIER = 6;         // Fixed to 6 (Wins on 0, 1, 2, 3, 4, 5)

// The Edge Core: Trade only when losing digits (6-9) heavily saturate the feed
const EXHAUSTION_Z_SCORE = 2.00; // Requires > 2.0 Standard Deviations of imbalance
const MIN_STREAK = 4;            // Must see at least 4 consecutive losing digits (6-9)

const BASE_STAKE = 0.35;         // Strict flat stake allocation
const COOLDOWN_TICKS = 25;       
const SETTLE_TICKS = 15;
const WARMUP_TICKS = 300;

const TP_PERCENT = 10;
const SL_PERCENT = 20;

let globalTickCounter = 0;

// ---------- Statistical Exhaustion Analyzer ----------
class StatisticalExhaustionEngine {
  constructor() {
    this.ticks = [];
    this.consecutiveLossDigits = 0;
  }

  feed(price) {
    const digit = parseInt(parseFloat(price).toFixed(MARKET.dp).slice(-1));
    this.ticks.push(digit);
    if (this.ticks.length > 400) this.ticks.shift();

    // Track streaks of losing digits (6, 7, 8, 9)
    if (digit >= 6) {
      this.consecutiveLossDigits++;
    } else {
      this.consecutiveLossDigits = 0;
    }
  }

  getMetrics() {
    if (this.ticks.length < WARMUP_TICKS) return null;

    const shortWindow = this.ticks.slice(-30);
    const longWindow = this.ticks.slice(-300);

    // Dynamic historical baseline tracking for digits 6-9 (~40% expected)
    const longLossDigits = longWindow.filter(d => d >= 6).length;
    const pBaseline = longLossDigits / longWindow.length; 

    const shortLossDigits = shortWindow.filter(d => d >= 6).length;
    const n = shortWindow.length;

    const expectedLossDigits = n * pBaseline;
    const standardDeviation = Math.sqrt(n * pBaseline * (1 - pBaseline));
    
    const zScore = (shortLossDigits - expectedLossDigits) / (standardDeviation || 1);

    return {
      zScore: zScore, 
      currentStreak: this.consecutiveLossDigits,
      shortDensity: (shortLossDigits / n) * 100
    };
  }
}

const engine = new StatisticalExhaustionEngine();

// ---------- State ----------
const state = {
  active: false,
  tradingMode: 'demo', 
  balance: null,
  currency: 'USD',
  dailyStartBalance: null,
  dailyPnl: 0,
  locked: false,
  lockReason: '',
  warmupComplete: false,
  warmupTicksFed: 0,
  liveSubscribed: false,
  accountType: 'Connecting...',

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
      tradingMode: state.tradingMode,
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
      
      if (saved.tradingMode) state.tradingMode = saved.tradingMode;
      
      if (saved.date === today && saved.sessionActive) {
        state.sessionAlreadyUsedToday = true;
        state.locked = true;
        state.lockReason = 'Daily session safety limits locked.';
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

function getTP() { return state.dailyStartBalance ? (state.dailyStartBalance * TP_PERCENT / 100) : 0; }
function getSL() { return state.dailyStartBalance ? (state.dailyStartBalance * SL_PERCENT / 100) : 0; }

function checkDailyLimits() {
  if (!state.dailyStartBalance) return false;
  if (state.dailyPnl >= getTP()) {
    state.locked = true;
    state.lockReason = `Target Target-Profit +$${getTP().toFixed(2)} secured.`;
    addLog(state.lockReason);
    return true;
  }
  if (state.dailyPnl <= -getSL()) {
    state.locked = true;
    state.lockReason = `Emergency Stop-Loss -$${getSL().toFixed(2)} executed.`;
    addLog(state.lockReason);
    return true;
  }
  return false;
}

function settleRealTrade() {
  if (!state.activeRealTrade || state.balance == null) return;
  const profit = state.balance - state.activeRealTrade.balanceBefore;
  state.dailyPnl += profit;
  const result = profit > 0 ? 'WIN' : (profit < 0 ? 'LOSS' : 'DRAW');
  
  addLog(`[${state.tradingMode.toUpperCase()}] ${result}: ${profit > 0 ? '+' : ''}${profit.toFixed(2)} | Session P&L: ${state.dailyPnl.toFixed(2)}`);

  state.currentStake = BASE_STAKE; // Always Flat Stake
  state.tradeInProgress = false;
  state.activeRealTrade = null;
  state.settleTicksRemaining = 0;
  state.cooldownTicksLeft = COOLDOWN_TICKS;

  checkDailyLimits();
  saveState();
  broadcastSSE({ state: sanitizeState() });
}

function processTick(price) {
  engine.feed(price);
  const metrics = engine.getMetrics();
  
  globalTickCounter++;
  
  // Dashboard metrics visual heartbeat
  if (globalTickCounter % 15 === 0 && metrics) {
    addLog(`📊 Matrix Status: Losing Streak (6-9): [${metrics.currentStreak}] | Risk Density: [${metrics.shortDensity.toFixed(0)}%] | Exhaustion Z-Score: [${metrics.zScore.toFixed(2)} / ${EXHAUSTION_Z_SCORE}]`);
  }

  if (!state.active || state.locked || !state.warmupComplete) return;

  if (state.settleTicksRemaining > 0) {
    state.settleTicksRemaining--;
    if (state.settleTicksRemaining === 0) settleRealTrade();
    broadcastSSE({ state: sanitizeState() });
    return;
  }

  if (state.cooldownTicksLeft > 0) { state.cooldownTicksLeft--; return; }
  if (state.tradeInProgress) return;
  if (!metrics) return;

  // UNDER 6 HIGH-CONVICTION EXECUTION TARGET
  if (metrics.zScore >= EXHAUSTION_Z_SCORE && metrics.currentStreak >= MIN_STREAK) {
    state.tradeInProgress = true;
    
    const stake = Math.round(Math.min(BASE_STAKE, state.balance) * 100) / 100;
    addLog(`🎯 EXHAUSTION DETECTED! Losing numbers cluster over-extended (Streak: ${metrics.currentStreak}, Z-Score: ${metrics.zScore.toFixed(2)}). Order: DIGITUNDER 6`);

    state.activeRealTrade = { stake, balanceBefore: state.balance };
    
    send({
      proposal: 1, amount: stake, basis: 'stake', currency: state.currency || 'USD',
      duration: 1, duration_unit: 't', symbol: MARKET.sym,
      contract_type: 'DIGITUNDER', barrier: FIXED_BARRIER, req_id: ++reqId
    });

    broadcastSSE({ state: sanitizeState() });
  }
}

// ---------- WebSocket & REST Core Setup ----------
let derivWs = null;
let reqId = 0;

function send(msg) {
  if (derivWs && derivWs.readyState === WebSocket.OPEN) derivWs.send(JSON.stringify(msg));
}

async function connectDeriv() {
  if (!state.active) return;
  
  const appId = (process.env.DERIV_APP_ID || '').trim();
  const token = (process.env.DERIV_PAT || '').trim();

  if (!appId || !token) {
    addLog('Configuration error: Check Render environment strings.');
    return;
  }

  try {
    const accountsResponse = await fetch('https://api.derivws.com/trading/v1/options/accounts', {
      method: 'GET',
      headers: { 'Authorization': `Bearer ${token}`, 'Deriv-App-ID': appId, 'Content-Type': 'application/json' }
    });

    if (!accountsResponse.ok) throw new Error('API Profile access denied.');

    const accountsData = await accountsResponse.json();
    const accountList = Array.isArray(accountsData.data) ? accountsData.data : [accountsData.data];
    const account = accountList.find(acc => acc.account_type === (state.tradingMode || 'demo'));
    
    if (!account) throw new Error(`Target account configuration mismatch.`);

    const accountId = account.account_id;
    state.accountType = account.account_type === 'demo' ? '🧪 DEMO PIPELINE' : '⚠️ LIVE PRODUCTION PORTFOLIO';
    state.balance = parseFloat(account.balance);
    state.currency = account.currency || 'USD';
    
    if (state.dailyStartBalance === null) state.dailyStartBalance = state.balance;

    addLog(`✅ Secure Routing Active: ${accountId} (${state.accountType})`);

    const otpResponse = await fetch(`https://api.derivws.com/trading/v1/options/accounts/${accountId}/otp`, {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${token}`, 'Deriv-App-ID': appId, 'Content-Type': 'application/json' }
    });

    if (!otpResponse.ok) throw new Error('Security core refused session allocation.');

    const otpData = await otpResponse.json();
    derivWs = new WebSocket(otpData.data.url);

    derivWs.on('open', () => {
      addLog(`Syncing historical feed arrays...`);
      send({ balance: 1, subscribe: 1, req_id: ++reqId });
      send({ ticks_history: MARKET.sym, count: WARMUP_TICKS, end: 'latest', req_id: ++reqId });
    });

    derivWs.on('message', data => { try { handleMessage(JSON.parse(data)); } catch(e) {} });
    derivWs.on('close', () => { if (state.active) setTimeout(connectDeriv, 5000); });
  } catch (e) {
    addLog(`Pipeline Error: ${e.message}. Retrying...`);
    if (state.active) setTimeout(connectDeriv, 10000);
  }
}

function handleMessage(msg) {
  if (msg.error) {
    addLog(`Deriv API Flag: ${msg.error.message}`);
    state.tradeInProgress = false; state.activeRealTrade = null; state.settleTicksRemaining = 0;
    return;
  }
  if (msg.msg_type === 'balance') {
    state.balance = parseFloat(msg.balance.balance);
    broadcastSSE({ state: sanitizeState() });
  } else if (msg.msg_type === 'history') {
    state.warmupTicksFed += msg.history.prices.length;
    for (const p of msg.history.prices) engine.feed(p);
    if (state.warmupTicksFed >= WARMUP_TICKS && !state.liveSubscribed) {
      state.warmupComplete = true; state.liveSubscribed = true;
      addLog('✅ Baseline calibrated. High-probability Under-6 strategy armed.');
      send({ ticks: MARKET.sym, req_id: ++reqId });
    }
    broadcastSSE({ state: sanitizeState() });
  } else if (msg.msg_type === 'tick') {
    if (msg.tick.symbol !== MARKET.sym) return;
    processTick(parseFloat(msg.tick.quote));
    broadcastSSE({ state: sanitizeState() });
  } else if (msg.msg_type === 'proposal') {
    send({ buy: msg.proposal.id, price: msg.proposal.ask_price, req_id: ++reqId });
  } else if (msg.msg_type === 'buy') {
    if (state.activeRealTrade) state.settleTicksRemaining = SETTLE_TICKS;
  }
}

app.get('/api/logs', (req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', Connection: 'keep-alive' });
  sseClients.add(res); res.write(`data: ${JSON.stringify({ state: sanitizeState() })}\n\n`);
  req.on('close', () => sseClients.delete(res));
});

app.get('/api/state', (req, res) => res.json({ ...state, logs: undefined }));

app.post('/api/control', (req, res) => {
  const { action, mode } = req.body;
  if (action === 'set_mode') {
    if (state.active) return res.status(400).json({ error: 'System processing core active.' });
    state.tradingMode = mode; state.dailyStartBalance = null; state.dailyPnl = 0;
    saveState(); broadcastSSE({ state: sanitizeState() }); return res.json({ success: true });
  }
  if (action === 'start') {
    if (state.sessionAlreadyUsedToday) return res.status(403).json({ error: 'Session locked.' });
    state.active = true; state.locked = false; state.dailyStartBalance = null; state.dailyPnl = 0;
    state.warmupComplete = false; state.warmupTicksFed = 0; state.liveSubscribed = false;
    state.tradeInProgress = false; state.activeRealTrade = null; state.settleTicksRemaining = 0;
    addLog(`🚀 Core Initiated. Target Vector: [UNDER 6] Mode: [${state.tradingMode.toUpperCase()}].`);
    connectDeriv(); saveState();
  } else if (action === 'stop') {
    state.active = false; if (derivWs) derivWs.close();
    state.tradeInProgress = false; addLog('Engine returned to standby.'); saveState();
  }
  broadcastSSE({ state: sanitizeState() }); res.json({ success: true });
});

loadState();
server.listen(PORT, () => console.log(`Under-6 Execution Matrix Online on Port ${PORT}`));
