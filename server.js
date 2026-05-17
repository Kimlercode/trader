const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');

const app = express();
const server = http.createServer(app);
const PORT = process.env.PORT || 3000;

app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json());

// ---------- SSE ----------
const sseClients = new Set();
let logId = 1;

function addLog(msg) {
  const entry = { id: logId++, time: new Date().toISOString(), message: msg };
  state.logs.unshift(entry);
  if (state.logs.length > 500) state.logs.pop();
  broadcastSSE({ logs: [entry], state: sanitizeState() });
}

function broadcastSSE(payload) {
  sseClients.forEach(c => c.write(`data: ${JSON.stringify(payload)}\n\n`));
}

function sanitizeState() {
  const { logs, ...rest } = state;
  return rest;
}

// ---------- State ----------
const state = {
  active: false,
  balance: null,
  currency: 'USD',
  sessionPnL: 0,
  locked: false,
  lockReason: '',
  currentStake: 0.35,
  r100: { digits: [], lastPrice: null },
  r25: { digits: [], lastPrice: null, lastDigit: null },
  tradeInProgress: false,
  activeTrade: null,
  logs: [],
};

// ---------- Risk / Martingale ----------
const SESSION_TP = 10.0;
const SESSION_SL = -10.0;
const BASE_STAKE = 0.35;
const MARTINGALE = 1.85;
const BARRIER = 5;
const HOUSE_EDGE = 0.98;

function checkLimits() {
  if (state.sessionPnL >= SESSION_TP) {
    state.locked = true;
    state.lockReason = `Take-profit $${SESSION_TP} reached.`;
    addLog(state.lockReason);
    return true;
  }
  if (state.sessionPnL <= SESSION_SL) {
    state.locked = true;
    state.lockReason = `Stop-loss $${SESSION_SL} hit.`;
    addLog(state.lockReason);
    return true;
  }
  return false;
}

function processR100Tick(price) {
  state.r100.lastPrice = price;
  const digit = parseInt(parseFloat(price).toFixed(2).slice(-1));
  state.r100.digits.push(digit);
  if (state.r100.digits.length > 10) state.r100.digits.shift();
  addLog(`[R_100] price=${price} digit=${digit}`);
}

function processR25Tick(price) {
  state.r25.lastPrice = price;
  const digit = parseInt(parseFloat(price).toFixed(3).slice(-1));
  state.r25.lastDigit = digit;
  state.r25.digits.push(digit);
  if (state.r25.digits.length > 10) state.r25.digits.shift();
  addLog(`[R_25] price=${price} digit=${digit}`);

  if (!state.active || state.locked || state.tradeInProgress) return;

  const d = state.r100.digits;
  if (d.length < 4) return;
  const last4 = d.slice(-4);
  const diffs = [last4[1]-last4[0], last4[2]-last4[1], last4[3]-last4[2]];
  const absDiffs = diffs.map(Math.abs);
  if (absDiffs[0] <= absDiffs[1] || absDiffs[1] <= absDiffs[2]) return;
  if (last4[3] < 7) return;
  if (digit < 5) return;

  addLog(`[LLCM SIGNAL] R_100 deceleration + extreme high, R_25 digit=${digit} → DIGITUNDER barrier ${BARRIER}`);

  state.tradeInProgress = true;
  const stakeToUse = Math.min(state.currentStake, state.balance);
  state.activeTrade = {
    market: 'R_25',
    direction: 'under',
    barrier: BARRIER,
    stake: stakeToUse,
    balanceBefore: state.balance,
  };

  send({
    proposal: 1,
    amount: stakeToUse,
    basis: 'stake',
    currency: state.currency || 'USD',
    duration: 1,
    duration_unit: 't',
    symbol: 'R_25',
    contract_type: 'DIGITUNDER',
    barrier: BARRIER,
    req_id: ++reqId
  });
}

function settleTrade(contract) {
  if (!state.activeTrade) return;
  const profit = typeof contract.profit === 'number' ? contract.profit : parseFloat(contract.profit) || 0;
  state.sessionPnL += profit;
  addLog(`[TRADE RESULT] ${profit >= 0 ? 'WIN' : 'LOSS'} | Profit: ${profit.toFixed(2)} | Session P&L: ${state.sessionPnL.toFixed(2)}`);

  // Martingale
  if (profit >= 0) {
    state.currentStake = BASE_STAKE;
  } else {
    state.currentStake = Math.min(state.currentStake * MARTINGALE, state.balance);
  }

  state.tradeInProgress = false;
  state.activeTrade = null;

  if (checkLimits()) {
    state.active = false;
  }

  broadcastSSE({ state: sanitizeState() });
}

// ---------- Deriv WebSocket ----------
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
    addLog('Connected to Deriv. Authorizing...');
    send({ authorize: process.env.DERIV_API_TOKEN });
  });
  derivWs.on('message', data => {
    try { handleMessage(JSON.parse(data)); } catch(e) { console.error('Invalid msg'); }
  });
  derivWs.on('close', () => setTimeout(connectDeriv, 5000));
  derivWs.on('error', err => addLog(`WebSocket error: ${err.message}`));
}

function handleMessage(msg) {
  if (msg.error) return addLog(`Deriv error: ${msg.error.message}`);

  if (msg.msg_type === 'authorize') {
    addLog('Authorized. Subscribing to balance & ticks.');
    send({ balance: 1, subscribe: 1, req_id: ++reqId });
    send({ ticks: 'R_100', req_id: ++reqId });
    send({ ticks: 'R_25', req_id: ++reqId });
    send({ ticks_history: 'R_100', count: 100, end: 'latest', req_id: ++reqId });
    send({ ticks_history: 'R_25', count: 100, end: 'latest', req_id: ++reqId });
  }
  else if (msg.msg_type === 'balance') {
    state.balance = parseFloat(msg.balance.balance);
    state.currency = msg.balance.currency;
    broadcastSSE({ state: sanitizeState() });
  }
  else if (msg.msg_type === 'history') {
    const sym = msg.echo_req.ticks_history;
    const prices = msg.history.prices;
    if (prices) {
      for (const p of prices) {
        if (sym === 'R_100') processR100Tick(parseFloat(p));
        else if (sym === 'R_25') processR25Tick(parseFloat(p));
      }
    }
  }
  else if (msg.msg_type === 'tick') {
    const sym = msg.tick.symbol;
    const price = parseFloat(msg.tick.quote);
    if (sym === 'R_100') processR100Tick(price);
    else if (sym === 'R_25') processR25Tick(price);
    broadcastSSE({ state: sanitizeState() });
  }
  else if (msg.msg_type === 'proposal') {
    send({ buy: msg.proposal.id, price: msg.proposal.ask_price, req_id: ++reqId });
  }
  else if (msg.msg_type === 'buy') {
    addLog(`Contract bought – ID ${msg.buy.contract_id}`);
    send({ proposal_open_contract: 1, contract_id: msg.buy.contract_id, subscribe: 1, req_id: ++reqId });
  }
  else if (msg.msg_type === 'proposal_open_contract') {
    if (state.activeTrade) {
      settleTrade(msg.proposal_open_contract);
      broadcastSSE({ state: sanitizeState() });
    }
  }
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
    state.active = true; state.locked = false; state.sessionPnL = 0; state.lockReason = '';
    state.currentStake = BASE_STAKE;
    state.r100.digits = []; state.r25.digits = [];
    state.tradeInProgress = false; state.activeTrade = null;
    addLog('LLCM trading started (Martingale 1.85).');
  } else if (action === 'stop') {
    state.active = false;
    state.tradeInProgress = false; state.activeTrade = null;
    addLog('Trading stopped manually.');
  }
  broadcastSSE({ state: sanitizeState() });
  res.json({ success: true });
});

connectDeriv();
server.listen(PORT, () => console.log(`LLCM + Martingale server on port ${PORT}`));
