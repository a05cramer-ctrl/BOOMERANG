// BOOMERANG GET /api/feed: bundled by build.mjs from src/api/feed.js, src/data/*, shared/parse.mjs. Edit those, then rebuild.

// src/data/base58.js
var B58 = /^[1-9A-HJ-NP-Za-km-z]+$/;
function isAddress(s) {
  return typeof s === "string" && s.length >= 32 && s.length <= 44 && B58.test(s);
}

// src/data/rpc.js
var RpcError = class extends Error {
  constructor(message, status) {
    super(message);
    this.name = "RpcError";
    this.status = status;
  }
};
var wait = (ms) => new Promise((r) => setTimeout(r, ms));
async function runPool(items, size, fn) {
  let next = 0;
  const workers = [];
  for (let w = 0; w < Math.min(size, items.length); w++) {
    workers.push((async () => {
      while (next < items.length) {
        const item = items[next++];
        await fn(item);
      }
    })());
  }
  await Promise.all(workers);
}
function createRpc(o) {
  const {
    url,
    fetchImpl,
    batch = false,
    maxBatch = 20,
    concurrency = 3,
    retries = 2,
    backoffMs = 350,
    timeoutMs = 8e3,
    label = "rpc",
    sleep = wait
  } = o;
  const doFetch = fetchImpl || ((...a) => globalThis.fetch(...a));
  let seq = 0;
  async function post(body) {
    const ctl = typeof AbortController === "function" ? new AbortController() : null;
    const timer = ctl ? setTimeout(() => ctl.abort(), timeoutMs) : null;
    try {
      let res;
      try {
        res = await doFetch(url, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify(body),
          signal: ctl ? ctl.signal : void 0
        });
      } catch {
        throw new RpcError(label + " unreachable");
      }
      if (!res || !res.ok) throw new RpcError(label + " http " + (res ? res.status : "?"), res ? res.status : 0);
      try {
        return await res.json();
      } catch {
        throw new RpcError(label + " sent invalid JSON");
      }
    } finally {
      if (timer) clearTimeout(timer);
    }
  }
  async function withRetry(fn) {
    let last;
    for (let i = 0; i <= retries; i++) {
      if (i) await sleep(backoffMs * 2 ** (i - 1));
      try {
        return await fn();
      } catch (e) {
        last = e;
        if (e && e.status && e.status >= 400 && e.status < 500 && e.status !== 429) break;
      }
    }
    throw last;
  }
  async function call(method, params) {
    return withRetry(async () => {
      const j = await post({ jsonrpc: "2.0", id: ++seq, method, params });
      if (j && j.error) throw new RpcError(label + " " + method + " error " + (j.error.code ?? ""));
      return j ? j.result : null;
    });
  }
  async function many(calls) {
    const out = new Array(calls.length).fill(void 0);
    if (!calls.length) return out;
    let okRequests = 0;
    let failedRequests = 0;
    if (batch) {
      const starts = [];
      for (let i = 0; i < calls.length; i += maxBatch) starts.push(i);
      await runPool(starts, concurrency, async (start) => {
        const part = calls.slice(start, start + maxBatch);
        const ids = part.map(() => ++seq);
        const body = part.map((c, k) => ({ jsonrpc: "2.0", id: ids[k], method: c.method, params: c.params }));
        try {
          const arr = await withRetry(async () => {
            const j = await post(body);
            if (!Array.isArray(j)) throw new RpcError(label + " batch rejected");
            return j;
          });
          const byId = new Map(arr.map((r) => [r && r.id, r]));
          ids.forEach((id, k) => {
            const r = byId.get(id);
            out[start + k] = r && !r.error ? r.result : void 0;
          });
          okRequests++;
        } catch {
          failedRequests++;
        }
      });
    } else {
      await runPool(calls.map((_, i) => i), concurrency, async (i) => {
        try {
          out[i] = await call(calls[i].method, calls[i].params);
          okRequests++;
        } catch {
          failedRequests++;
        }
      });
    }
    if (!okRequests && failedRequests) throw new RpcError(label + " unavailable");
    return out;
  }
  return { call, many };
}

// src/data/stonk.js
var STONK_ORIGIN = "https://www.stonkfun.xyz";
var num = (v) => {
  if (v === null || v === void 0 || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
};
var str = (v) => typeof v === "string" && v ? v : null;
function normalizeStonk(j) {
  const d = j && typeof j === "object" ? j.data : null;
  const t = d && d.token;
  const l = d && d.launch;
  const creator = l && isAddress(l.creator) ? l.creator : null;
  if (!t || !isAddress(t.mint)) return { found: false, token: null, creator };
  const m = t.market || {};
  const q = t.quote || {};
  return {
    found: true,
    creator,
    token: {
      mint: t.mint,
      pool: isAddress(t.pool) ? t.pool : l && isAddress(l.pool) ? l.pool : null,
      name: str(t.name),
      symbol: str(t.symbol),
      quoteMint: isAddress(q.mint) ? q.mint : null,
      quoteSymbol: str(q.symbol),
      launchpad: str(t.launchpad),
      mode: str(t.mode),
      status: str(t.status),
      graduationProgress: num(t.graduationProgress),
      createdAt: t.createdAt ?? null,
      priceUsd: num(m.priceUsd),
      marketCapUsd: num(m.marketCapUsd),
      fdvUsd: num(m.fdvUsd),
      volume24hUsd: num(m.volume24hUsd),
      liquidityUsd: num(m.liquidityUsd)
    }
  };
}
async function fetchStonk(mint, { fetchImpl, timeoutMs = 6e3 } = {}) {
  if (!isAddress(mint)) throw new Error("bad mint");
  const doFetch = fetchImpl || ((...a) => globalThis.fetch(...a));
  const ctl = typeof AbortController === "function" ? new AbortController() : null;
  const timer = ctl ? setTimeout(() => ctl.abort(), timeoutMs) : null;
  try {
    let r;
    try {
      r = await doFetch(`${STONK_ORIGIN}/api/public/v1/tokens/${mint}`, {
        headers: { accept: "application/json" },
        signal: ctl ? ctl.signal : void 0
      });
    } catch {
      throw new Error("stonkfun unreachable");
    }
    if (r.status === 404) return { found: false, token: null, creator: null };
    if (!r.ok) throw new Error("stonkfun http " + r.status);
    let j;
    try {
      j = await r.json();
    } catch {
      throw new Error("stonkfun sent invalid JSON");
    }
    return normalizeStonk(j);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

// ../shared/parse.mjs
var LAUNCHLAB_AUTH = "WLHv2UAZm6z4KyaaELi5pjdbJh6RESMva1Rnn8pJVVh";
var CPMM_AUTH = "GpMZbSM2GgvTKHJirzeGfMFoaZ8UR2X7F4v8vHTvxFbL";
var STONKFUN_FEE_WALLET = "56XVRVAsgWv6ADaxzoNnbL38LMoWKM5WiSAhrAWUbd2p";
var STONKFUN_FORWARDER = "5CEbueQnq1Ym2uSSx2xXds3jQAqT1BDnkA59RZobSPAG";
var NVDAX_MINT = "Xsc9qvGR1efVDFGLrVsmkzv3qi45LTBjeUKSPmx9qEh";
var MEMO_PROGRAM = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr";
var MEMO_TAG = "BOOMERANG v1";
var POOL_AUTHORITIES = [LAUNCHLAB_AUTH, CPMM_AUTH];
var FEE_WALLETS = [STONKFUN_FEE_WALLET];
var big = (v) => {
  if (typeof v === "bigint") return v;
  if (v === void 0 || v === null || v === "") return 0n;
  return BigInt(String(v));
};
var absBig = (v) => v < 0n ? -v : v;
function txSignature(tx) {
  return tx && tx.transaction && tx.transaction.signatures && tx.transaction.signatures[0] || null;
}
function txSigners(tx) {
  const keys = tx && tx.transaction && tx.transaction.message && tx.transaction.message.accountKeys || [];
  return keys.filter((k) => k && k.signer === true).map((k) => typeof k === "string" ? k : k.pubkey);
}
function tokenDeltas(tx) {
  const out = /* @__PURE__ */ new Map();
  const meta = tx && tx.meta || {};
  const pre = meta.preTokenBalances || [];
  const post = meta.postTokenBalances || [];
  const add = (owner, mint, d) => {
    if (!owner || !mint || d === 0n) return;
    if (!out.has(owner)) out.set(owner, /* @__PURE__ */ new Map());
    const m = out.get(owner);
    m.set(mint, (m.get(mint) || 0n) + d);
  };
  const preByIdx = new Map(pre.map((b) => [b.accountIndex, b]));
  const postByIdx = new Map(post.map((b) => [b.accountIndex, b]));
  for (const p of post) {
    const q = preByIdx.get(p.accountIndex);
    add(p.owner, p.mint, big(p.uiTokenAmount && p.uiTokenAmount.amount) - big(q && q.uiTokenAmount && q.uiTokenAmount.amount));
  }
  for (const q of pre) {
    if (!postByIdx.has(q.accountIndex)) add(q.owner, q.mint, -big(q.uiTokenAmount && q.uiTokenAmount.amount));
  }
  return out;
}
function deltaOf(deltas, owner, mint) {
  const m = deltas.get(owner);
  return m && m.get(mint) || 0n;
}
function postBalance(tx, owner, mint) {
  let s = 0n;
  for (const p of tx && tx.meta && tx.meta.postTokenBalances || []) {
    if (p.owner === owner && p.mint === mint) s += big(p.uiTokenAmount && p.uiTokenAmount.amount);
  }
  return s;
}
function parseTrade(tx, opts) {
  const { baseMint, quoteMint } = opts || {};
  const poolAuthorities = opts && opts.poolAuthorities || POOL_AUTHORITIES;
  const feeWallets = opts && opts.feeWallets || FEE_WALLETS;
  if (!tx || !tx.meta || tx.meta.err || !baseMint || !quoteMint) return null;
  const deltas = tokenDeltas(tx);
  let poolOwner = null;
  let poolBase = 0n;
  for (const a of poolAuthorities) {
    const d = deltaOf(deltas, a, baseMint);
    if (d !== 0n && absBig(d) > absBig(poolBase)) {
      poolOwner = a;
      poolBase = d;
    }
  }
  if (!poolOwner) return null;
  let trader = null;
  let traderBase = 0n;
  for (const [owner, m] of deltas) {
    if (poolAuthorities.includes(owner) || feeWallets.includes(owner)) continue;
    const d = m.get(baseMint) || 0n;
    if (d === 0n || d > 0n === poolBase > 0n) continue;
    if (absBig(d) > absBig(traderBase)) {
      trader = owner;
      traderBase = d;
    }
  }
  if (!trader) return null;
  const side = traderBase > 0n ? "buy" : "sell";
  const poolQuote = deltaOf(deltas, poolOwner, quoteMint);
  let feeRaw = 0n;
  for (const w of feeWallets) {
    const d = deltaOf(deltas, w, quoteMint);
    if (d > 0n) feeRaw += d;
  }
  let grossQuoteRaw;
  if (side === "buy") {
    if (poolQuote <= 0n) return null;
    grossQuoteRaw = poolQuote + feeRaw;
  } else {
    if (poolQuote >= 0n) return null;
    grossQuoteRaw = -poolQuote;
  }
  return {
    sig: txSignature(tx),
    slot: tx.slot ?? null,
    time: tx.blockTime ?? null,
    side,
    trader,
    baseRaw: absBig(traderBase),
    grossQuoteRaw,
    feeRaw: feeRaw > 0n ? feeRaw : null,
    traderBasePostRaw: postBalance(tx, trader, baseMint)
  };
}
function creatorShareRaw(trade, opts) {
  const creatorShareBps = BigInt((opts && opts.creatorShareBps) ?? 5e3);
  const fallbackFeeBps = BigInt((opts && opts.fallbackFeeBps) ?? 100);
  const fee = trade.feeRaw != null ? big(trade.feeRaw) : big(trade.grossQuoteRaw) * fallbackFeeBps / 10000n;
  return fee * creatorShareBps / 10000n;
}
function memoText(tx) {
  const ixs = [];
  const msg = tx && tx.transaction && tx.transaction.message;
  if (msg && Array.isArray(msg.instructions)) ixs.push(...msg.instructions);
  for (const inner of tx && tx.meta && tx.meta.innerInstructions || []) ixs.push(...inner.instructions || []);
  for (const ix of ixs) {
    if (!ix) continue;
    if (ix.programId === MEMO_PROGRAM || ix.program === "spl-memo") {
      if (typeof ix.parsed === "string") return ix.parsed;
    }
  }
  for (const l of tx && tx.meta && tx.meta.logMessages || []) {
    const m = /Memo \(len \d+\): "(.*)"$/.exec(l);
    if (m) return m[1];
  }
  return null;
}
function parsePayout(tx, opts) {
  const { quoteMint, engine } = opts || {};
  if (!tx || !tx.meta || tx.meta.err || !quoteMint || !engine) return null;
  const memo = memoText(tx);
  if (!memo || !memo.startsWith(MEMO_TAG)) return null;
  const deltas = tokenDeltas(tx);
  if (deltaOf(deltas, engine, quoteMint) >= 0n) return null;
  const items = [];
  let totalRaw = 0n;
  for (const [owner, m] of deltas) {
    if (owner === engine) continue;
    const d = m.get(quoteMint) || 0n;
    if (d > 0n) {
      items.push({ owner, amountRaw: d });
      totalRaw += d;
    }
  }
  if (!items.length) return null;
  return { sig: txSignature(tx), slot: tx.slot ?? null, time: tx.blockTime ?? null, items, totalRaw };
}
function parseForward(tx, opts) {
  const { quoteMint, engine } = opts || {};
  const forwarders = opts && opts.forwarders || [STONKFUN_FORWARDER];
  if (!tx || !tx.meta || tx.meta.err || !quoteMint || !engine) return null;
  const signers = txSigners(tx);
  if (!signers.some((s) => forwarders.includes(s))) return null;
  const d = deltaOf(tokenDeltas(tx), engine, quoteMint);
  if (d <= 0n) return null;
  return { sig: txSignature(tx), slot: tx.slot ?? null, time: tx.blockTime ?? null, amountRaw: d };
}

// src/data/chain.js
var SERVER_LIMITS = {
  tradeSigs: 40,
  // pool (or mint) signatures read per refresh
  maxTrades: 25,
  engineSigs: 200,
  // engine NVDAx-account signatures listed per refresh
  engineTxCap: 100,
  // engine transactions parsed per refresh (cached by signature)
  maxPayouts: 40,
  maxForwards: 40,
  nullRetries: 1,
  budgetMs: 7e3
};
var TX_OPTS = { encoding: "jsonParsed", maxSupportedTransactionVersion: 0, commitment: "confirmed" };
var wait2 = (ms) => new Promise((r) => setTimeout(r, ms));
var newestFirst = (a, b) => (b.time ?? 0) - (a.time ?? 0) || (b.slot ?? 0) - (a.slot ?? 0);
function createTxCache(max = 2500) {
  const m = /* @__PURE__ */ new Map();
  return {
    has: (s) => m.has(s),
    get: (s) => m.get(s),
    set(s, v) {
      if (!m.has(s) && m.size >= max) m.delete(m.keys().next().value);
      m.set(s, v);
    },
    get size() {
      return m.size;
    }
  };
}
var isMemoIx = (ix) => ix && (ix.programId === MEMO_PROGRAM || ix.program === "spl-memo");
function trimTx(tx) {
  if (!tx || !tx.meta) return null;
  const msg = tx.transaction && tx.transaction.message || {};
  const signers = (msg.accountKeys || []).filter((k) => k && typeof k === "object" && k.signer === true).map((k) => ({ pubkey: k.pubkey, signer: true }));
  const memoIxs = (msg.instructions || []).filter(isMemoIx).map((ix) => ({ programId: ix.programId, program: ix.program, parsed: ix.parsed }));
  const innerMemo = [];
  for (const inner of tx.meta.innerInstructions || []) {
    for (const ix of inner.instructions || []) if (isMemoIx(ix)) innerMemo.push({ programId: ix.programId, program: ix.program, parsed: ix.parsed });
  }
  const bal = (list) => (list || []).map((b) => ({
    accountIndex: b.accountIndex,
    mint: b.mint,
    owner: b.owner,
    uiTokenAmount: { amount: b.uiTokenAmount ? String(b.uiTokenAmount.amount) : "0" }
  }));
  return {
    slot: tx.slot ?? null,
    blockTime: tx.blockTime ?? null,
    transaction: {
      signatures: [tx.transaction && tx.transaction.signatures && tx.transaction.signatures[0] || null],
      message: { accountKeys: signers, instructions: memoIxs }
    },
    meta: {
      err: tx.meta.err ?? null,
      preTokenBalances: bal(tx.meta.preTokenBalances),
      postTokenBalances: bal(tx.meta.postTokenBalances),
      innerInstructions: innerMemo.length ? [{ index: 0, instructions: innerMemo }] : [],
      logMessages: (tx.meta.logMessages || []).filter((l) => typeof l === "string" && l.includes("Memo (len"))
    }
  };
}
async function fetchTxs(rpc, sigs, cache, opts = {}) {
  const { nullRetries = 1, backoffMs = 400, sleep = wait2, deadline = Infinity, now = Date.now } = opts;
  let pending = sigs.filter((s) => !cache.has(s));
  for (let attempt = 0; pending.length && attempt <= nullRetries; attempt++) {
    if (attempt) {
      if (now() + backoffMs * attempt > deadline) break;
      await sleep(backoffMs * attempt);
    }
    if (now() > deadline) break;
    const res = await rpc.many(pending.map((s) => ({ method: "getTransaction", params: [s, TX_OPTS] })));
    const next = [];
    pending.forEach((s, i) => {
      const tx = res[i];
      if (tx && tx.meta) cache.set(s, trimTx(tx));
      else next.push(s);
    });
    pending = next;
  }
  return sigs.map((s) => cache.get(s) || null);
}
async function listSigs(rpc, address, limit) {
  const r = await rpc.call("getSignaturesForAddress", [address, { limit, commitment: "confirmed" }]);
  return (Array.isArray(r) ? r : []).filter((s) => s && typeof s.signature === "string");
}
async function collectTrades({ rpc, cache, source, mint, quoteMint, limits, deadline, sleep }) {
  const sigs = (await listSigs(rpc, source, limits.tradeSigs)).filter((s) => !s.err);
  const txs = await fetchTxs(rpc, sigs.map((s) => s.signature), cache, { nullRetries: limits.nullRetries, deadline, sleep });
  const trades = [];
  for (const tx of txs) {
    if (!tx) continue;
    const t = parseTrade(tx, { baseMint: mint, quoteMint });
    if (t) trades.push({ ...t, creatorShareRaw: creatorShareRaw(t) });
  }
  trades.sort(newestFirst);
  return { trades: trades.slice(0, limits.maxTrades), scanned: sigs.length, missing: txs.filter((t) => !t).length };
}
async function collectEngine({ rpc, cache, engine, quoteMint, limits, nowMs, deadline, sleep }) {
  const res = await rpc.call("getTokenAccountsByOwner", [engine, { mint: quoteMint }, { encoding: "jsonParsed", commitment: "confirmed" }]);
  const accounts = (res && Array.isArray(res.value) ? res.value : []).filter((a) => a && typeof a.pubkey === "string");
  let floatRaw = 0n;
  for (const a of accounts) {
    const amt = a.account && a.account.data && a.account.data.parsed && a.account.data.parsed.info && a.account.data.parsed.info.tokenAmount && a.account.data.parsed.info.tokenAmount.amount;
    if (typeof amt === "string" && /^\d+$/.test(amt)) floatRaw += BigInt(amt);
  }
  const base = { floatRaw, payouts: [], forwards: [], forwarded24hRaw: 0n, thrown24hRaw: 0n };
  if (!accounts.length) return { ...base, window: { scanned: 0, oldestTime: null, covers24h: true, accounts: 0 } };
  const lists = await Promise.all(accounts.slice(0, 3).map((a) => listSigs(rpc, a.pubkey, limits.engineSigs)));
  const bySig = /* @__PURE__ */ new Map();
  let fullHistory = true;
  let oldestListed = Infinity;
  for (const l of lists) {
    if (l.length >= limits.engineSigs) fullHistory = false;
    for (const s of l) {
      if (s.blockTime != null) oldestListed = Math.min(oldestListed, s.blockTime);
      if (!bySig.has(s.signature)) bySig.set(s.signature, s);
    }
  }
  const ok = [...bySig.values()].filter((s) => !s.err).sort((a, b) => (b.blockTime ?? 0) - (a.blockTime ?? 0) || (b.slot ?? 0) - (a.slot ?? 0));
  const take = ok.slice(0, limits.engineTxCap);
  const txs = await fetchTxs(rpc, take.map((s) => s.signature), cache, { nullRetries: limits.nullRetries, deadline, sleep });
  const dayAgo = Math.floor(nowMs / 1e3) - 86400;
  const payouts = [];
  const forwards = [];
  let fwd24 = 0n;
  let thrown24 = 0n;
  let missing24 = 0;
  let fetched = 0;
  let oldestFetched = null;
  txs.forEach((tx, i) => {
    const s = take[i];
    if (!tx) {
      if ((s.blockTime ?? nowMs / 1e3) >= dayAgo) missing24++;
      return;
    }
    fetched++;
    const t = tx.blockTime ?? s.blockTime ?? null;
    if (t != null) oldestFetched = oldestFetched == null ? t : Math.min(oldestFetched, t);
    const p = parsePayout(tx, { quoteMint, engine });
    if (p) {
      payouts.push(p);
      if (t != null && t >= dayAgo) thrown24 += p.totalRaw;
      return;
    }
    const f = parseForward(tx, { quoteMint, engine });
    if (f) {
      forwards.push(f);
      if (t != null && t >= dayAgo) fwd24 += f.amountRaw;
    }
  });
  const last = take.length ? take[take.length - 1] : null;
  const reachedPast24h = !!last && last.blockTime != null && last.blockTime < dayAgo;
  const listCovers = fullHistory || oldestListed < dayAgo;
  const covers24h = missing24 === 0 && (reachedPast24h || take.length === ok.length && listCovers);
  payouts.sort(newestFirst);
  forwards.sort(newestFirst);
  return {
    floatRaw,
    payouts: payouts.slice(0, limits.maxPayouts),
    forwards: forwards.slice(0, limits.maxForwards),
    forwarded24hRaw: fwd24,
    thrown24hRaw: thrown24,
    window: { scanned: fetched, oldestTime: oldestFetched, covers24h, accounts: accounts.length }
  };
}
async function buildFeed({ rpc, cache, stonk, mint, engine, limits, nowMs = Date.now(), sleep }) {
  const token = stonk && stonk.token ? stonk.token : null;
  const engineAddr = engine || stonk && stonk.creator || null;
  const quoteMint = token && token.quoteMint || NVDAX_MINT;
  const usePool = !!(token && token.pool && token.status !== "graduated");
  const source = usePool ? token.pool : mint;
  const deadline = Date.now() + limits.budgetMs;
  const enginePromise = engineAddr ? collectEngine({ rpc, cache, engine: engineAddr, quoteMint, limits, nowMs, deadline, sleep }).then(
    (v) => ({ ok: true, v }),
    () => ({ ok: false })
  ) : Promise.resolve(null);
  const [tr, en] = await Promise.all([
    collectTrades({ rpc, cache, source, mint, quoteMint, limits, deadline, sleep }),
    enginePromise
  ]);
  const e = en && en.ok ? en.v : null;
  return {
    token,
    engine: engineAddr,
    quoteMint,
    trades: tr.trades,
    payouts: e ? e.payouts : [],
    forwards: e ? e.forwards : [],
    engineFloatRaw: e ? e.floatRaw : null,
    forwarded24hRaw: e ? e.forwarded24hRaw : null,
    thrown24hRaw: e ? e.thrown24hRaw : null,
    engineWindow: e ? e.window : null,
    engineStatus: !engineAddr ? "unknown" : e ? "ok" : "error",
    stonkStatus: !stonk ? "error" : stonk.found ? "ok" : "indexing",
    tradeWindow: { source: usePool ? "pool" : "mint", scanned: tr.scanned },
    generatedAt: nowMs
  };
}

// src/data/feed.js
function serializeFeed(feed) {
  return JSON.stringify(feed, (k, v) => typeof v === "bigint" ? v.toString() : v);
}

// src/api/feed.js
var txCache = createTxCache(4e3);
var stonkCache = /* @__PURE__ */ new Map();
var feedCache = /* @__PURE__ */ new Map();
var inflight = /* @__PURE__ */ new Map();
var STONK_TTL_MS = 1e4;
var FEED_TTL_MS = 2e3;
function send(res, status, body, cache) {
  res.statusCode = status;
  res.setHeader("content-type", "application/json; charset=utf-8");
  res.setHeader("cache-control", cache || "no-store");
  res.end(typeof body === "string" ? body : JSON.stringify(body));
}
function readQuery(req) {
  try {
    const u = new URL(req.url || "/", "http://localhost");
    return { mint: u.searchParams.get("mint") || "", engine: u.searchParams.get("engine") || "" };
  } catch {
    return { mint: "", engine: "" };
  }
}
async function stonkFor(mint) {
  const hit = stonkCache.get(mint);
  if (hit && Date.now() - hit.at < STONK_TTL_MS) return hit.value;
  let value = null;
  try {
    value = await fetchStonk(mint);
  } catch {
    value = hit ? hit.value : null;
  }
  stonkCache.set(mint, { at: Date.now(), value });
  if (stonkCache.size > 50) stonkCache.delete(stonkCache.keys().next().value);
  return value;
}
async function compute(mint, engine, key) {
  const rpc = createRpc({
    url: `https://mainnet.helius-rpc.com/?api-key=${process.env.HELIUS_API_KEY}`,
    batch: true,
    maxBatch: 20,
    concurrency: 3,
    retries: 2,
    timeoutMs: 8e3,
    label: "helius"
  });
  const stonk = await stonkFor(mint);
  const feed = await buildFeed({ rpc, cache: txCache, stonk, mint, engine: engine || null, limits: SERVER_LIMITS, nowMs: Date.now() });
  const body = serializeFeed(feed);
  feedCache.set(key, { at: Date.now(), body });
  if (feedCache.size > 50) feedCache.delete(feedCache.keys().next().value);
  return body;
}
async function handler(req, res) {
  if (req.method && req.method !== "GET" && req.method !== "HEAD") {
    res.setHeader("allow", "GET, HEAD");
    return send(res, 405, { error: "method not allowed" });
  }
  const { mint, engine } = readQuery(req);
  if (!isAddress(mint)) return send(res, 400, { error: "mint must be a base58 address" });
  if (engine && !isAddress(engine)) return send(res, 400, { error: "engine must be a base58 address" });
  if (!process.env.HELIUS_API_KEY) return send(res, 503, { error: "HELIUS_API_KEY is not set on this deployment" });
  const key = mint + "|" + (engine || "");
  const fresh = feedCache.get(key);
  if (fresh && Date.now() - fresh.at < FEED_TTL_MS) {
    return send(res, 200, fresh.body, "s-maxage=4, stale-while-revalidate=30");
  }
  try {
    let p = inflight.get(key);
    if (!p) {
      p = compute(mint, engine, key).finally(() => inflight.delete(key));
      inflight.set(key, p);
    }
    const body = await p;
    return send(res, 200, body, "s-maxage=4, stale-while-revalidate=30");
  } catch {
    return send(res, 502, { error: "chain data unavailable, retry shortly" });
  }
}
export {
  handler as default
};
