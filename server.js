require('dotenv').config();
const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');
const fs = require('fs');

// --- SINGLE SOURCE OF TRUTH ---
const { supabase, saveTradeToCloud } = require('./database.js');

const app = express();
const server = http.createServer(app);

// --- VARIABLE DEFINITIONS ---
const PORT = process.env.PORT || 3000;
const STATE_FILE = '/var/data/deriv_multimarket_state.json';

// =====================================================================
//  🎯  EASY TWEAK CONFIGURATION – Change numbers here only
// =====================================================================
const CONFIG = {
    // ---------- Gap Range ----------
    MIN_GAP_OVER: 11,
    MAX_GAP_OVER: 14,
    MIN_GAP_UNDER: -14,
    MAX_GAP_UNDER: -11,

    // ---------- Digit Pattern Parameters ----------
    OVER_4TH_PREV: [7, 8, 9],
    OVER_LAST3_RANGE: [0, 3],
    UNDER_4TH_PREV: [0, 1, 2, 3],
    UNDER_LAST3_RANGE: [7, 9],

    // ---------- Timing & Cooldowns ----------
    MIN_TRIGGER_INTERVAL: 20000,
    MAX_CONSECUTIVE_LOSSES: 2,
    LOSS_COOLDOWN_MS: 120000,

    // ---------- Risk Management (DAILY = 24h rolling) ----------
    RISK_PERCENT: 1,
    TP_PERCENT: 5,
    SL_PERCENT: 10,
    MIN_STAKE: 0.35,

    // ---------- Trade Execution ----------
    COOLDOWN_TICKS: 1,
    SETTLE_TICKS: 5,
    SETTLEMENT_TIMEOUT_MS: 10000,

    // ---------- P&L Sync ----------
    PNL_SYNC_INTERVAL_MS: 300000
};
// =====================================================================

// ---------- SCHEDULED RESTART (03:00 East African Time) ----------
function scheduleRestart() {
  const now = Date.now();
  const nextMidnightUTC = new Date(now);
  nextMidnightUTC.setUTCHours(0, 0, 0, 0);
  if (nextMidnightUTC.getTime() < now) {
    nextMidnightUTC.setUTCDate(nextMidnightUTC.getUTCDate() + 1);
  }
  const delay = nextMidnightUTC.getTime() - now;
  console.log(`⏰ Next restart scheduled at ${nextMidnightUTC.toISOString()} (03:00 EAT)`);
  setTimeout(() => {
    console.log('🔄 Scheduled restart at 03:00 EAT. Resetting and restarting...');
    // Keep active as is, but reset daily lock – will be re‑evaluated from DB after restart
    state.sessionPnl = 0;
    state.locked = false;
    state.lockReason = '';
    saveState();
    process.exit(0);
  }, delay);
}
scheduleRestart();

// --- DATABASE HEALTH CHECK ---
async function checkDatabaseConnection() {
  try {
    const { count, error } = await supabase
      .from('trading_ledger')
      .select('id', { count: 'exact', head: true });
    if (error) throw error;
    console.log(`✅ Supabase Database Connected (Total Records: ${count})`);
    return true;
  } catch (err) {
    console.error(`❌ Database Connection Failed: ${err.message}`);
    return false;
  }
}

app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json());

// ---------- ANALYTICS API ----------
app.get('/api/ledger/analytics', async (req, res) => {
  const { start, end, mode } = req.query;

  if (mode === 'session') {
    // Session = in‑memory P&L since last start/restart
    return res.json({
      totalProfit: state.sessionPnl || 0,
      strikeRate: '0',
      totalTrades: 0,
      rawData: []
    });
  }

  let startDate = start, endDate = end;
  const now = new Date();
  if (mode === 'hour') {
    const oneHourAgo = new Date(now.getTime() - 60 * 60 * 1000);
    startDate = oneHourAgo.toISOString();
    endDate = now.toISOString();
  } else if (mode === '24h') {
    const oneDayAgo = new Date(now.getTime() - 24 * 60 * 60 * 1000);
    startDate = oneDayAgo.toISOString();
    endDate = now.toISOString();
  } else if (mode === 'month') {
    const oneMonthAgo = new Date(now.getFullYear(), now.getMonth() - 1, now.getDate());
    startDate = oneMonthAgo.toISOString();
    endDate = now.toISOString();
  } else if (mode === '6months') {
    const sixMonthsAgo = new Date(now.getFullYear(), now.getMonth() - 6, now.getDate());
    startDate = sixMonthsAgo.toISOString();
    endDate = now.toISOString();
  } else if (mode === '1year') {
    const oneYearAgo = new Date(now.getFullYear() - 1, now.getMonth(), now.getDate());
    startDate = oneYearAgo.toISOString();
    endDate = now.toISOString();
  }

  if (!startDate || !endDate) {
    return res.status(400).json({ error: 'Invalid date range. Please provide start and end dates.' });
  }

  try {
    const { data, error } = await supabase
      .from('trading_ledger')
      .select('*')
      .gte('created_at', startDate)
      .lte('created_at', endDate);

    if (error) throw error;

    const totalProfit = data.reduce((acc, curr) => acc + (curr.profit_loss || 0), 0);
    const totalTrades = data.length;
    const wins = data.filter(t => t.is_win).length;
    const strikeRate = totalTrades > 0 ? ((wins / totalTrades) * 100).toFixed(1) : 0;

    let grossProfit = 0, grossLoss = 0;
    data.forEach(t => {
      const pnl = t.profit_loss || 0;
      if (pnl > 0) grossProfit += pnl;
      else if (pnl < 0) grossLoss += Math.abs(pnl);
    });
    const profitFactor = grossLoss > 0 ? (grossProfit / grossLoss) : (grossProfit > 0 ? Infinity : 0);

    const sorted = data.slice().sort((a, b) => new Date(a.created_at) - new Date(b.created_at));
    let peak = 0, maxDrawdown = 0, cum = 0;
    sorted.forEach(t => {
      cum += (t.profit_loss || 0);
      if (cum > peak) peak = cum;
      const drawdown = peak - cum;
      if (drawdown > maxDrawdown) maxDrawdown = drawdown;
    });
    const drawdownPercent = (peak > 0) ? (maxDrawdown / peak) * 100 : 0;

    res.json({
      totalProfit: totalProfit.toFixed(2),
      strikeRate,
      totalTrades,
      profitFactor: profitFactor.toFixed(2),
      drawdown: drawdownPercent.toFixed(2),
      rawData: data
    });
  } catch (err) {
    console.error('❌ Analytics Error:', err.message);
    res.status(500).json({ error: 'Failed to fetch historical data' });
  }
});

// --- REQUIRED: Live Logging System ---
const sseClients = new Set();
let logId = 1;

function addLog(msg) {
  const entry = { id: logId++, time: new Date().toISOString(), message: msg };
  state.logs.unshift(entry);
  if (state.logs.length > 250) state.logs.pop();
  broadcastSSE({ logs: [entry], state: sanitizeState() });
}

function broadcastSSE(payload) {
  sseClients.forEach(c => c.write(`data: ${JSON.stringify(payload)}\n\n`));
}

// ---------- SSE ENDPOINT ----------
app.get('/api/logs', (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  const client = res;
  sseClients.add(client);
  client.write(`data: ${JSON.stringify({ state: sanitizeState(), logs: state.logs.slice(0, 50) })}\n\n`);

  req.on('close', () => {
    sseClients.delete(client);
    client.end();
  });
});

// ---------- CONTROL ENDPOINT ----------
app.post('/api/control', (req, res) => {
  const { action, mode } = req.body;

  if (action === 'start') {
    if (state.locked) {
      return res.status(400).json({ 
        error: `System is paused: ${state.lockReason}` 
      });
    }
    state.active = true;
    addLog('🔓 Automation matrix ARMED by user.');
    return res.json({ success: true });
  }

  if (action === 'stop') {
    state.active = false;
    addLog('🔒 Automation matrix DISARMED by user.');
    return res.json({ success: true });
  }

  if (action === 'set_mode') {
    if (!mode || !['demo', 'real'].includes(mode)) {
      return res.status(400).json({ error: 'Invalid mode. Use "demo" or "real".' });
    }
    state.tradingMode = mode;
    state.active = false;
    addLog(`🔄 Switching to ${mode.toUpperCase()} account. Reconnecting...`);
    disconnectDeriv();
    setTimeout(connectDeriv, 1000);
    return res.json({ success: true });
  }

  res.status(400).json({ error: 'Unknown action.' });
});

// ---------- Markets Configuration ----------
const MARKETS = {
  'R_10':  { id: 'R_10',  name: 'Volatility 10 Index',  dp: 3 },
  'R_25':  { id: 'R_25',  name: 'Volatility 25 Index',  dp: 3 },
  'R_50':  { id: 'R_50',  name: 'Volatility 50 Index',  dp: 4 },
  'R_75':  { id: 'R_75',  name: 'Volatility 75 Index',  dp: 4 },
  'R_100': { id: 'R_100', name: 'Volatility 100 Index', dp: 2 }
};
const BUFFER_CAPACITY = 1000;

// ---------- Pipeline Class (computes cumulative gap) ----------
class MultiMarketPipeline {
  constructor() {
    this.buffers = {};
    for (const symbol in MARKETS) {
      this.buffers[symbol] = [];
    }
  }

  extractDigit(price, dp) {
    return parseInt(parseFloat(price).toFixed(dp).slice(-1));
  }

  seed(symbol, prices) {
    const config = MARKETS[symbol];
    this.buffers[symbol] = prices.map(p => this.extractDigit(p, config.dp));
    if (this.buffers[symbol].length > BUFFER_CAPACITY) {
      this.buffers[symbol] = this.buffers[symbol].slice(-BUFFER_CAPACITY);
    }
  }

  feed(symbol, price) {
    const config = MARKETS[symbol];
    const digit = this.extractDigit(price, config.dp);
    this.buffers[symbol].push(digit);
    if (this.buffers[symbol].length > BUFFER_CAPACITY) this.buffers[symbol].shift();
    return this.analyze(symbol);
  }

  analyze(symbol) {
    const ticks = this.buffers[symbol];
    if (ticks.length < BUFFER_CAPACITY) return null;

    const freq = Array(10).fill(0);
    ticks.forEach(d => freq[d]++);

    const pcts = freq.map(count => (count / BUFFER_CAPACITY) * 100);
    const over0 = (ticks.filter(d => d > 0).length / BUFFER_CAPACITY) * 100;
    const under9 = (ticks.filter(d => d < 9).length / BUFFER_CAPACITY) * 100;
    const over1 = (ticks.filter(d => d > 1).length / BUFFER_CAPACITY) * 100;
    const under8 = (ticks.filter(d => d < 8).length / BUFFER_CAPACITY) * 100;
    const over2 = (ticks.filter(d => d > 2).length / BUFFER_CAPACITY) * 100;
    const under7 = (ticks.filter(d => d < 7).length / BUFFER_CAPACITY) * 100;
    const over3 = (ticks.filter(d => d > 3).length / BUFFER_CAPACITY) * 100;
    const under6 = (ticks.filter(d => d < 6).length / BUFFER_CAPACITY) * 100;
    const over4 = (ticks.filter(d => d > 4).length / BUFFER_CAPACITY) * 100;
    const under5 = (ticks.filter(d => d < 5).length / BUFFER_CAPACITY) * 100;

    const totalGap = (over0 - under9) + (over1 - under8) + (over2 - under7) + (over3 - under6) + (over4 - under5);

    return {
      symbol,
      pcts,
      totalGap,
      greenCircle: 0,
      densityOver3: Math.round((ticks.filter(d => d > 3).length / BUFFER_CAPACITY) * 100),
      last3: ticks.slice(-3)
    };
  }
}

const engine = new MultiMarketPipeline();

const state = {
  active: false,                // User arming state – persists across restarts
  tradingMode: 'demo',
  balance: null,
  currency: 'USD',
  
  // --- P&L Values ---
  sessionPnl: 0,                // In‑memory P&L since last start (resets on redeploy)
  dailyPnl: 0,                  // 24h rolling P&L from database (persists)
  dailyStartBalance: null,      // balance - dailyPnl (computed from DB)
  
  // --- Lock State ---
  locked: false,                // Paused due to daily limit – does NOT change active
  lockReason: '',
  
  // --- Trade State ---
  tradeInProgress: false,
  activeRealTrade: null,
  settleTicksRemaining: 0,
  currentStake: 0.35,
  cooldownTicksLeft: 0,
  marketMetrics: {},
  logs: [],
  lastTriggerTime: 0,
  lossCooldownUntil: 0,
  pendingSettlement: false
};

function sanitizeState() {
  const { logs, ...rest } = state;
  return rest;
}

// -------- DAILY P&L SYNC FROM DATABASE (24h rolling) --------
async function syncDailyPnlFromDB() {
  try {
    const now = new Date();
    const twentyFourHoursAgo = new Date(now.getTime() - 24 * 60 * 60 * 1000);
    const { data, error } = await supabase
      .from('trading_ledger')
      .select('profit_loss')
      .gte('created_at', twentyFourHoursAgo.toISOString());

    if (error) throw error;

    const total = data.reduce((sum, row) => sum + (row.profit_loss || 0), 0);
    state.dailyPnl = total;

    // Compute the starting balance for this 24h period
    if (state.balance !== null) {
      state.dailyStartBalance = state.balance - state.dailyPnl;
    }

    // Check daily limits – this may set 'locked' but does NOT touch 'active'
    checkDailyLimits();
    broadcastSSE({ state: sanitizeState() });
    return total;
  } catch (err) {
    console.error('❌ Failed to sync daily P&L:', err.message);
    return 0;
  }
}

// -------- DAILY LIMITS CHECK (uses 24h P&L) – does NOT change 'active' --------
function checkDailyLimits() {
  if (state.dailyStartBalance === null || state.dailyStartBalance === 0) return false;
  const tpLimit = state.dailyStartBalance * (CONFIG.TP_PERCENT / 100);
  const slLimit = state.dailyStartBalance * (CONFIG.SL_PERCENT / 100);

  if (state.dailyPnl >= tpLimit) {
    state.locked = true;
    state.lockReason = `🎯 Daily Target Reached: +$${state.dailyPnl.toFixed(2)} (${CONFIG.TP_PERCENT}% of start). Trading paused. Will resume at midnight.`;
    addLog(state.lockReason);
    return true;
  }
  if (state.dailyPnl <= -slLimit) {
    state.locked = true;
    state.lockReason = `🛑 Daily Loss Limit Breached: -$${Math.abs(state.dailyPnl).toFixed(2)} (${CONFIG.SL_PERCENT}% of start). Trading paused. Will resume at midnight.`;
    addLog(state.lockReason);
    return true;
  }
  // If we were locked and now P&L is back within limits (shouldn't happen, but just in case)
  if (state.locked && state.dailyPnl < tpLimit && state.dailyPnl > -slLimit) {
    state.locked = false;
    state.lockReason = '';
    addLog('✅ Daily limits cleared. Trading resumed.');
  }
  return false;
}

// -------- STATE PERSISTENCE (keeps 'active' across restarts) --------
function saveState() {
  try {
    const dir = path.dirname(STATE_FILE);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(STATE_FILE, JSON.stringify({
      date: new Date().toLocaleDateString("en-US", { timeZone: "Africa/Nairobi" }),
      tradingMode: state.tradingMode,
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
      const today = new Date().toLocaleDateString("en-US", { timeZone: "Africa/Nairobi" });
      if (saved.date === today) {
        state.tradingMode = saved.tradingMode || 'demo';
        state.locked = saved.locked || false;
        state.lockReason = saved.lockReason || '';
        state.active = saved.sessionActive || false;
      } else {
        // New day: reset lock but keep active as is
        state.locked = false;
        state.lockReason = '';
        state.active = saved.sessionActive || false;
      }
    }
  } catch(e) {}
}

// -------- SETTLEMENT (uses balance stream) --------
function settleRealTrade() {
  if (!state.activeRealTrade || !state.activeRealTrade.contractId || state.balance == null) {
    if (state.activeRealTrade) {
      addLog("⚠️ Trade closed or never executed. Resetting state.");
      state.tradeInProgress = false;
      state.activeRealTrade = null;
    }
    state.pendingSettlement = false;
    return;
  }

  const profit = state.balance - state.activeRealTrade.balanceBefore;
  
  // Update session P&L (in‑memory, resets on restart)
  state.sessionPnl += profit;
  
  // Update daily P&L (will be overwritten by DB sync, but good for UI)
  state.dailyPnl += profit;

  const isWin = profit >= 0;
  const grossPayout = isWin ? (state.activeRealTrade.stake + profit) : 0;

  // Update consecutive losses
  if (isWin) {
    consecutiveLosses = 0;
  } else {
    consecutiveLosses++;
    if (consecutiveLosses >= CONFIG.MAX_CONSECUTIVE_LOSSES) {
      state.lossCooldownUntil = Date.now() + CONFIG.LOSS_COOLDOWN_MS;
      addLog(`⏳ ${CONFIG.MAX_CONSECUTIVE_LOSSES} consecutive losses. Cooling down for ${CONFIG.LOSS_COOLDOWN_MS/60000} minutes.`);
    }
  }

  // Save trade to DB – this will update the 24h P&L
  saveTradeToCloud({
    contract_id: state.activeRealTrade.contractId,
    asset: MARKETS[state.activeRealTrade.symbol]?.name || state.activeRealTrade.symbol,
    contractType: state.activeRealTrade.contractType,
    stake: state.activeRealTrade.stake,
    payout: grossPayout,
    isWin: isWin,
    barrier: state.activeRealTrade.barrier,
    exitTick: state.activeRealTrade.exitTick
  });

  addLog(`[Settlement] ${state.activeRealTrade.symbol} | Result: ${isWin ? '🟢 WIN (+$' : '🔴 LOSS (-$'}${Math.abs(profit).toFixed(2)}) | Session: $${state.sessionPnl.toFixed(2)} | 24h: $${state.dailyPnl.toFixed(2)}`);

  state.tradeInProgress = false;
  state.activeRealTrade = null;
  state.settleTicksRemaining = 0;
  state.cooldownTicksLeft = CONFIG.COOLDOWN_TICKS;
  state.pendingSettlement = false;

  const rawStake = Math.max(CONFIG.MIN_STAKE, state.balance * (CONFIG.RISK_PERCENT / 100));
  state.currentStake = Math.round(Math.min(rawStake, state.balance) * 100) / 100;

  // Sync daily P&L from DB to get the exact value (and recompute start balance)
  syncDailyPnlFromDB().then(() => {
    saveState();
    broadcastSSE({ state: sanitizeState() });
  });
}

let consecutiveLosses = 0;

// =====================================================================
// ENTRY LOGIC – uses gap range + 4‑digit pattern
// =====================================================================
function processLiveFeed(symbol, price) {
  // 1. If pending settlement, wait for balance update
  if (state.pendingSettlement) {
    broadcastSSE({ state: sanitizeState() });
    return;
  }

  // 2. Count down settlement ticks
  if (state.settleTicksRemaining > 0) {
    state.settleTicksRemaining--;
    if (state.settleTicksRemaining === 0) {
      state.pendingSettlement = true;
      addLog(`⏳ ${CONFIG.SETTLE_TICKS} ticks elapsed. Waiting for balance update to settle ${state.activeRealTrade?.symbol}...`);
      setTimeout(() => {
        if (state.pendingSettlement) {
          addLog(`⚠️ Balance update timeout. Forcing settlement now.`);
          state.pendingSettlement = false;
          settleRealTrade();
        }
      }, CONFIG.SETTLEMENT_TIMEOUT_MS);
    }
    broadcastSSE({ state: sanitizeState() });
    return;
  }

  // 3. Feed engine and update metrics
  const analysis = engine.feed(symbol, price);
  if (!analysis) return;

  state.marketMetrics[symbol] = analysis;
  if (state.cooldownTicksLeft > 0) state.cooldownTicksLeft--;

  // 4. Check if we can trade: must be active, not locked, no trade in progress, cooldown done
  if (!state.active || state.locked || state.tradeInProgress || state.cooldownTicksLeft > 0) {
    broadcastSSE({ state: sanitizeState() });
    return;
  }

  // 5. Loss cooldown
  const now = Date.now();
  if (now < state.lossCooldownUntil) {
    broadcastSSE({ state: sanitizeState() });
    return;
  }

  // 6. Minimum interval between trades (20s)
  if (now - state.lastTriggerTime < CONFIG.MIN_TRIGGER_INTERVAL) {
    broadcastSSE({ state: sanitizeState() });
    return;
  }

  // 7. Evaluate each market using new pattern
  let bestCandidate = null;
  let bestScore = -Infinity;

  for (const sym in MARKETS) {
    const buffer = engine.buffers[sym];
    if (buffer.length < 4) continue;

    const gap = state.marketMetrics[sym]?.totalGap;
    if (gap === undefined) continue;

    const last4 = buffer.slice(-4);
    const fourthPrev = last4[0];
    const lastThree = last4.slice(1);

    // OVER condition: gap in [MIN_GAP_OVER, MAX_GAP_OVER]
    if (gap >= CONFIG.MIN_GAP_OVER && gap <= CONFIG.MAX_GAP_OVER) {
      if (CONFIG.OVER_4TH_PREV.includes(fourthPrev)) {
        const allInRange = lastThree.every(d => d >= CONFIG.OVER_LAST3_RANGE[0] && d <= CONFIG.OVER_LAST3_RANGE[1]);
        const allDistinct = (new Set(lastThree)).size === 3;
        if (allInRange && allDistinct) {
          const score = gap;
          if (score > bestScore) {
            bestScore = score;
            bestCandidate = { symbol: sym, direction: 'OVER', barrier: 3, gap, last4 };
          }
        }
      }
    }

    // UNDER condition: gap in [MIN_GAP_UNDER, MAX_GAP_UNDER]
    if (gap >= CONFIG.MIN_GAP_UNDER && gap <= CONFIG.MAX_GAP_UNDER) {
      if (CONFIG.UNDER_4TH_PREV.includes(fourthPrev)) {
        const allInRange = lastThree.every(d => d >= CONFIG.UNDER_LAST3_RANGE[0] && d <= CONFIG.UNDER_LAST3_RANGE[1]);
        const allDistinct = (new Set(lastThree)).size === 3;
        if (allInRange && allDistinct) {
          const score = -gap;
          if (score > bestScore) {
            bestScore = score;
            bestCandidate = { symbol: sym, direction: 'UNDER', barrier: 6, gap, last4 };
          }
        }
      }
    }
  }

  // 8. Execute if candidate found
  if (bestCandidate) {
    const { symbol, direction, barrier, gap, last4 } = bestCandidate;

    state.pendingSettlement = false;
    state.tradeInProgress = true;
    const rawStake = Math.max(CONFIG.MIN_STAKE, state.balance * (CONFIG.RISK_PERCENT / 100));
    state.currentStake = Math.round(Math.min(rawStake, state.balance) * 100) / 100;

    const contractType = direction === 'OVER' ? 'DIGITOVER' : 'DIGITUNDER';
    addLog(`🔥 ${direction} Signal: ${symbol} | Gap: ${gap.toFixed(1)} | Pattern: ${last4.join('-')}`);

    state.activeRealTrade = {
      symbol,
      stake: state.currentStake,
      balanceBefore: state.balance,
      contractType,
      barrier,
      direction
    };

    state.lastTriggerTime = now;

    addLog(`📤 Requesting proposal for ${symbol} ${direction} with barrier ${barrier}...`);
    send({
      proposal: 1,
      amount: state.currentStake,
      basis: 'stake',
      contract_type: contractType,
      currency: state.currency || 'USD',
      duration: 1,
      duration_unit: 't',
      underlying_symbol: symbol,
      barrier: barrier,
      req_id: ++reqId
    });
  }

  broadcastSSE({ state: sanitizeState() });
}

// ------------------ WEBSOCKET CONNECTION ------------------
let derivWs = null;
let reqId = 0;
let keepAliveLoop = null;
let watchdogTimer = null;

function send(msg) { if (derivWs && derivWs.readyState === WebSocket.OPEN) derivWs.send(JSON.stringify(msg)); }

function disconnectDeriv() {
  clearInterval(keepAliveLoop);
  clearTimeout(watchdogTimer);
  if (derivWs) { derivWs.removeAllListeners(); try { derivWs.terminate(); } catch(e) {} derivWs = null; }
}

async function connectDeriv() {
  disconnectDeriv();
  const appId = (process.env.DERIV_APP_ID || '').trim();
  const token = (process.env.DERIV_PAT || '').trim();
  if (!appId || !token) { addLog('System Configuration Halt: Credentials missing.'); return; }

  try {
    const accRes = await fetch('https://api.derivws.com/trading/v1/options/accounts', {
      method: 'GET', headers: { 'Authorization': `Bearer ${token}`, 'Deriv-App-ID': appId, 'Content-Type': 'application/json' }
    });
    if (!accRes.ok) throw new Error('Authentication Denied.');

    const data = await accRes.json();
    const accList = Array.isArray(data.data) ? data.data : [data.data];
    const targetAccount = accList.find(a => a.account_type === state.tradingMode);

    if (!targetAccount) throw new Error(`Target profile missing: ${state.tradingMode}`);

    state.balance = parseFloat(targetAccount.balance);
    state.currency = targetAccount.currency || 'USD';

    // Reset session P&L on fresh connection
    state.sessionPnl = 0;

    // Sync daily P&L from DB to compute dailyStartBalance and check limits
    await syncDailyPnlFromDB();

    broadcastSSE({ state: sanitizeState() });

    const otpRes = await fetch(`https://api.derivws.com/trading/v1/options/accounts/${targetAccount.account_id}/otp`, {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${token}`, 'Deriv-App-ID': appId, 'Content-Type': 'application/json' },
      body: JSON.stringify({})
    });
    if (!otpRes.ok) throw new Error('Security allocation failure.');

    const otpData = await otpRes.json();
    derivWs = new WebSocket(otpData.data.url);

    derivWs.on('open', () => {
      addLog(`🌐 Connected. Balance: $${state.balance.toFixed(2)} | Session: $${state.sessionPnl.toFixed(2)} | 24h: $${state.dailyPnl.toFixed(2)}`);
      send({ balance: 1, subscribe: 1, req_id: ++reqId });
      for (const key in MARKETS) send({ ticks_history: key, count: BUFFER_CAPACITY, end: 'latest', req_id: ++reqId });

      keepAliveLoop = setInterval(() => {
        send({ ping: 1 });
        watchdogTimer = setTimeout(() => { if (derivWs) derivWs.terminate(); }, 3000);
      }, 15000);
    });

    derivWs.on('message', raw => {
      try {
        const msg = JSON.parse(raw);
        if (msg.msg_type === 'ping') { clearTimeout(watchdogTimer); return; }
        handleMessage(msg);
      } catch(e) {}
    });

    derivWs.on('close', () => { disconnectDeriv(); setTimeout(connectDeriv, 2000); });
    derivWs.on('error', () => { if (derivWs) derivWs.terminate(); });
  } catch(e) {
    addLog(`Network Exception: ${e.message}.`);
    setTimeout(connectDeriv, 5000);
  }
}

function handleMessage(msg) {
  if (msg.error) {
    addLog(`API Error: ${msg.error.message}`);
    state.tradeInProgress = false;
    state.activeRealTrade = null;
    state.settleTicksRemaining = 0;
    state.pendingSettlement = false;
    return;
  }

  if (msg.msg_type === 'proposal') {
    if (msg.error) {
      addLog(`❌ Proposal Error: ${msg.error.message}`);
      state.tradeInProgress = false;
      state.activeRealTrade = null;
      state.pendingSettlement = false;
    } else {
      send({
        buy: msg.proposal.id,
        price: msg.proposal.ask_price,
        req_id: ++reqId
      });
      addLog(`✅ Proposal confirmed: ${msg.proposal.ask_price}. Executing buy...`);
    }
    return;
  }

  if (msg.msg_type === 'balance') {
    state.balance = parseFloat(msg.balance.balance);
    if (state.pendingSettlement && state.activeRealTrade) {
      state.pendingSettlement = false;
      settleRealTrade();
    }
    if (state.dailyPnl !== undefined) {
      state.dailyStartBalance = state.balance - state.dailyPnl;
    }
    broadcastSSE({ state: sanitizeState() });
  }
  else if (msg.msg_type === 'history') {
    const symbol = msg.echo_req.ticks_history;
    engine.seed(symbol, msg.history.prices);
    addLog(`✅ History synchronized for ${symbol}`);
    send({ ticks: symbol, req_id: ++reqId });
  }
  else if (msg.msg_type === 'tick') {
    processLiveFeed(msg.tick.symbol, parseFloat(msg.tick.quote));
  }
  else if (msg.msg_type === 'buy') {
    if (state.activeRealTrade) {
      state.activeRealTrade.contractId = msg.buy.contract_id;
      state.settleTicksRemaining = CONFIG.SETTLE_TICKS;
      addLog(`💰 Trade Executed: Contract ID ${msg.buy.contract_id}`);
    }
  }
}

// ------------------ MANUAL TRADING PAYLOAD ------------------ //
app.post('/api/manual-trade', (req, res) => {
  const { symbol, contractType } = req.body;

  if (state.locked || state.tradeInProgress) {
    return res.status(400).json({ 
      error: state.locked ? state.lockReason : 'Trade in progress.' 
    });
  }
  if (!MARKETS[symbol]) {
    return res.status(400).json({ error: 'Invalid symbol.' });
  }

  const rawStake = Math.max(CONFIG.MIN_STAKE, state.balance * (CONFIG.RISK_PERCENT / 100));
  state.currentStake = Math.round(Math.min(rawStake, state.balance) * 100) / 100;

  state.pendingSettlement = false;
  state.tradeInProgress = true;
  
  let barrier, contractTypeApi;
  if (contractType === 'OVER') {
    barrier = 3;
    contractTypeApi = 'DIGITOVER';
  } else if (contractType === 'UNDER') {
    barrier = 6;
    contractTypeApi = 'DIGITUNDER';
  } else {
    return res.status(400).json({ error: 'Invalid contract type. Use "OVER" or "UNDER".' });
  }

  state.activeRealTrade = {
    symbol,
    stake: state.currentStake,
    balanceBefore: state.balance,
    contractType: contractTypeApi,
    barrier,
    direction: contractType
  };

  send({
    proposal: 1,
    amount: state.currentStake,
    basis: 'stake',
    contract_type: contractTypeApi,
    currency: state.currency || 'USD',
    duration: 1,
    duration_unit: 't',
    underlying_symbol: symbol,
    barrier: barrier,
    req_id: ++reqId
  });

  addLog(`📤 Manual ${contractType} request for ${symbol} with barrier ${barrier}...`);
  res.json({ success: true, message: 'Proposal requested' });
});

// ------------------ PERIODIC P&L SYNC ------------------
setInterval(() => {
  if (state.balance !== null) {
    syncDailyPnlFromDB().catch(err => console.error('Periodic sync error:', err));
  }
}, CONFIG.PNL_SYNC_INTERVAL_MS);

// ------------------ STARTUP ------------------
loadState();
checkDatabaseConnection().then(() => {
  connectDeriv();
  server.listen(PORT, () => console.log(`🚀 System Armed on port ${PORT}`));
});
