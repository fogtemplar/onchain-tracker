// ════════════════════════════════════════════════════════════
// 🐋 Onchain Whale Worker (Real-time)
// Alchemy WebSocket × 5 chains → Telegram instant alerts
// ════════════════════════════════════════════════════════════

import WebSocket, { WebSocketServer } from 'ws';
import http from 'http';
import Database from 'better-sqlite3';
import fs from 'fs';
import path from 'path';

// ── Config (env or hardcoded fallback) ──
const ALCHEMY_KEY = process.env.ALCHEMY_KEY || 'TDTn41PcLHD3pGfG1ilgz';
const BOT_TOKEN   = process.env.BOT_TOKEN   || '7890873311:AAGMVgMBcFsWg9mcWE5Vi9ernwNPwoq0GVk';
const CHAT_ID     = process.env.CHAT_ID     || '-1003743061931';
const MIN_USD     = parseInt(process.env.MIN_USD || '100000');
const PORT        = parseInt(process.env.PORT || '3000');

// ── Chain config ──
// BSC: Alchemy가 logs 구독 거부 → PublicNode 무료 WebSocket 사용
const CHAINS = {
  bsc:  { wss: 'wss://bsc-rpc.publicnode.com',                          http: `https://bnb-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}`,     name: 'BSC',  exp: 'https://bscscan.com' },
  eth:  { wss: `wss://eth-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}`,     http: `https://eth-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}`,     name: 'ETH',  exp: 'https://etherscan.io' },
  arb:  { wss: `wss://arb-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}`,     http: `https://arb-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}`,     name: 'ARB',  exp: 'https://arbiscan.io' },
  base: { wss: `wss://base-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}`,    http: `https://base-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}`,    name: 'BASE', exp: 'https://basescan.org' },
  poly: { wss: `wss://polygon-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}`, http: `https://polygon-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}`, name: 'POLY', exp: 'https://polygonscan.com' },
};

const TRANSFER_TOPIC = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef';

// ── SQLite (Railway Volume /data 또는 로컬 ./data) ──
const DATA_DIR = process.env.DATA_DIR || (fs.existsSync('/data') ? '/data' : './data');
try { fs.mkdirSync(DATA_DIR, { recursive: true }); } catch (e) {}
const DB_PATH = path.join(DATA_DIR, 'whale.db');
const db = new Database(DB_PATH);
db.pragma('journal_mode = WAL');
db.pragma('synchronous = NORMAL');
db.exec(`
  CREATE TABLE IF NOT EXISTS txs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts INTEGER NOT NULL,
    chain TEXT NOT NULL,
    sym TEXT NOT NULL,
    ca TEXT NOT NULL,
    amt REAL NOT NULL,
    price REAL NOT NULL,
    usd REAL NOT NULL,
    supply_pct REAL DEFAULT 0,
    ex_from TEXT,
    ex_to TEXT,
    addr_from TEXT NOT NULL,
    addr_to TEXT NOT NULL,
    hash TEXT NOT NULL,
    tx_type TEXT,
    tx_tag TEXT,
    UNIQUE(chain, hash, addr_from, addr_to)
  );
  CREATE INDEX IF NOT EXISTS idx_ts ON txs(ts DESC);
  CREATE INDEX IF NOT EXISTS idx_chain_ts ON txs(chain, ts DESC);
  CREATE INDEX IF NOT EXISTS idx_sym_ts ON txs(sym, ts DESC);
  CREATE INDEX IF NOT EXISTS idx_usd ON txs(usd DESC);
  CREATE INDEX IF NOT EXISTS idx_tx_type ON txs(tx_type);
`);
// 기존 DB에 컬럼 없으면 추가 (migration)
try { db.exec('ALTER TABLE txs ADD COLUMN tx_type TEXT'); } catch (e) {}
try { db.exec('ALTER TABLE txs ADD COLUMN tx_tag TEXT'); } catch (e) {}
const dbCount = db.prepare('SELECT COUNT(*) AS c FROM txs').get().c;
console.log(`[DB] SQLite at ${DB_PATH} — ${dbCount} rows`);

// Prepared statements
const stmtInsert = db.prepare(`
  INSERT OR IGNORE INTO txs (ts, chain, sym, ca, amt, price, usd, supply_pct, ex_from, ex_to, addr_from, addr_to, hash, tx_type, tx_tag)
  VALUES (@ts, @chain, @sym, @ca, @amt, @price, @usd, @supply_pct, @ex_from, @ex_to, @addr_from, @addr_to, @hash, @tx_type, @tx_tag)
`);
const stmtRecent = db.prepare(`SELECT * FROM txs ORDER BY ts DESC LIMIT ?`);
const stmtFilter = db.prepare(`
  SELECT * FROM txs
  WHERE ts >= @from_ts
    AND (@chain = '' OR chain = @chain)
    AND (@sym = '' OR sym = @sym)
    AND usd >= @min_usd
  ORDER BY ts DESC
  LIMIT @lim
`);
const stmtStats = db.prepare(`
  SELECT
    chain,
    COUNT(*) AS cnt,
    SUM(usd) AS total_usd,
    MAX(usd) AS max_usd
  FROM txs
  WHERE ts >= ?
  GROUP BY chain
`);
const stmtTopSyms = db.prepare(`
  SELECT sym, COUNT(*) AS cnt, SUM(usd) AS total_usd
  FROM txs
  WHERE ts >= ?
  GROUP BY sym
  ORDER BY total_usd DESC
  LIMIT ?
`);
const stmtPurge = db.prepare(`DELETE FROM txs WHERE ts < ?`);

// 30일 이상 데이터 자동 삭제 (1시간마다)
setInterval(() => {
  try {
    const cutoff = Date.now() - 30 * 86400 * 1000;
    const r = stmtPurge.run(cutoff);
    if (r.changes > 0) console.log(`[DB] purged ${r.changes} rows older than 30d`);
  } catch (e) { console.warn('purge err:', e.message); }
}, 3600 * 1000);

// ── EXCLUDE list (메이저/스테이블) ──
const EXCLUDE = new Set([
  'BTC','WBTC','ETH','WETH','XRP','BNB','SOL','TRX','DOGE','ADA','AVAX',
  'TON','HBAR','SUI','BCH','LTC','XLM','DOT','POL','MATIC','NEAR','ATOM',
  'APT','ALGO','VET','FIL','ICP','XDC','KAS','FLR','ETC','CRO','MNT',
  'ARB','OP','INJ','SEI','STX','TIA','THETA','IOTA','NEO','STRK','ZK','LINK',
  'USDT','USDC','DAI','BUSD','TUSD','FDUSD','PYUSD','USDE','LUSD','FRAX',
  'GUSD','USDP','USDD','CRVUSD','GHO','MIM','EURS','USDS','USD1','USDG',
  'USDF','USDTB','USTB','USDY','USYC','USD0','RLUSD','BFUSD','YLDS',
  'EURC','USX','USDAI','USDA','AUSD','REUSD','NUSD','FDIT','SATUSD',
  'BUIDL','OUSG','JTRSY','EUTBL','BCAP','HASH','XAUT','PAXG',
  'LEO','OKB','BGB','HTX','KCS','GT','WBT','NEXO','HT',
  'UNI','AAVE','MKR','COMP','CRV','SUSHI','LDO','SNX','CAKE','RPL','PENDLE',
  'MORPHO','SKY','ONDO','ENA','JUP','ENS','GRT','PYTH','RAY','HNT',
  'STETH','RETH','WSTETH','CBETH','SFRXETH','EZETH','WEETH','RSETH',
  'TAO','RENDER','FET','HYPE','PI','QNT','XMR','ZEC','DASH','WLD',
  'PUMP','BONK','SHIB','PEPE','TRUMP','CC','RAIN','NIGHT','STABLE',
  'MEMECORE','ASTER','FLOKI','WIF','PENGU',
  'BSC-USD','BSC-USDT'
]);

// ── 거래소 hot wallet ──
const EXCHANGES = {
  bsc: [
    { name:'Binance', addr:'0x8894e0a0c962cb723c1976a4421c95949be2d4e3' },
    { name:'Binance', addr:'0xf977814e90da44bfa03b6295a0616a897441acec' },
    { name:'OKX',     addr:'0x79f7d32fc680f6d20b12e5f3e3bd5fbf2d73e22d' },
    { name:'OKX',     addr:'0xbd612a3f30dca67bf60a39fd0d35e39b7ab80774' },
    { name:'Bybit',   addr:'0xe2fc31f816a9b3a5d7f77ffa59e40e25e6e0d50' },
    { name:'Bybit',   addr:'0x4f3a120e72c76c22ae802d129f599bfdd677dc6' },
    { name:'Gate',    addr:'0x68b22215ff74e3606bd5e6c1de8c2d68180c85f7' },
    { name:'KuCoin',  addr:'0x46705dfff24256421a05d056c29e81bdc09723b8' },
    { name:'HTX',     addr:'0xf7858da8a6617f7c6d0ff2bcafdb6d2eedf64840' },
    { name:'Crypto.com', addr:'0x44971abf0251958492fee97da3e5c5ada88b9185' },
    { name:'MEXC',    addr:'0x4f2d4cc2eab56e8973a8b9ade5d8e3c341e12625' },
    { name:'MEXC',    addr:'0x4982085c9e2f89f2ecb8131eca71afad896e89cb' },
    { name:'MEXC',    addr:'0xd1748257f7c6e39a2ff7dfbdf48a1c9fcfba3048' },
    { name:'Bitget',  addr:'0x0639556f03714a74a5feeaf5736a4a64ff70d206' },
  ],
  eth: [
    { name:'Binance', addr:'0x28c6c06298d514db089934071355e5743bf21d60' },
    { name:'Binance', addr:'0xbe0eb53f46cd790cd13851d5eff43d12404d33e8' },
    { name:'Binance', addr:'0xf977814e90da44bfa03b6295a0616a897441acec' },
    { name:'Binance', addr:'0x5a52e96bacdabb82fd05763e25335261b270efcb' },
    { name:'Binance', addr:'0x3c783c21a0383057d128bae431894a5c19f9cf06' },
    { name:'OKX',     addr:'0x6cc5f688a315f3dc28a7781717a9a798a59fda7b' },
    { name:'OKX',     addr:'0x8103683202aa8da10536036edef04cdd865c225e' },
    { name:'OKX',     addr:'0xa7efae728d2936e78bda97dc267687568dd593f3' },
    { name:'Bybit',   addr:'0xf89d7b9c864f589bbf53a82105107622b35eaa40' },
    { name:'Coinbase',addr:'0x71660c4005ba85c37ccec55d0c4493e66fe775d3' },
    { name:'Coinbase',addr:'0x503828976d22510aad0201ac7ec88293211d23da' },
    { name:'Coinbase',addr:'0xa9d1e08c7793af67e9d92fe308d5697fb81d3e43' },
    { name:'Kraken',  addr:'0x2910543af39aba0cd09dbb2d50200b3e800a63d2' },
    { name:'Kraken',  addr:'0xae2d4617c862309a3d75a0ffb358c7a5009c673f' },
    { name:'Gate',    addr:'0x0d0707963952f2fba59dd06f2b425ace40b492fe' },
    { name:'Gate',    addr:'0x7793cd85c11a924478d358d49b05b37b91b9e181' },
    { name:'KuCoin',  addr:'0x2b5634c42055806a59e9107ed44d43c426e99d2a' },
    { name:'KuCoin',  addr:'0x689c56aef474df92d44a1b70850f808488f9769c' },
    { name:'HTX',     addr:'0xab5c66752a9e8167967685f1450532fb96d5d24f' },
    { name:'HTX',     addr:'0x6748f50f686bfbca6fe8ad62b22228b87f31ff2b' },
    { name:'MEXC',    addr:'0x75e89d5979e4f6fba9f97c104c2f0afb3f1dcb88' },
    { name:'Bitget',  addr:'0x1ab4973a48dc892cd9971ece8e01dcc7688f8f23' },
    { name:'Crypto.com', addr:'0x6262998ced04146fa42253a5c0af90ca02dfd2a3' },
    { name:'Bitfinex',addr:'0x77134cbc06cb00b66f4c7e623d5fdbf6777635ec' },
  ],
  arb: [
    { name:'Binance', addr:'0xb38e8c17e38363af6ebdcb3dae12e0243582891d' },
    { name:'Bybit',   addr:'0x1db92e2eebc8e0c075a02bea49a2935bcd2dfcf4' },
    { name:'OKX',     addr:'0x461249076d88d0bb5b2f7bb2cd0ffdadf18bc1e3' },
    { name:'MEXC',    addr:'0x9117ef8d3a7a8cd2f80f33e7b38e0d82a4945dfe' },
  ],
  base: [
    { name:'Binance', addr:'0x3304e22ddaa22bcdc5fca2269b418046ae7b566a' },
    { name:'Coinbase',addr:'0x3304e22ddaa22bcdc5fca2269b418046ae7b566a' },
    { name:'Bybit',   addr:'0x3d9819210a31b4961b30ef54be2aed79b9c9cd3b' },
    { name:'OKX',     addr:'0x461249076d88d0bb5b2f7bb2cd0ffdadf18bc1e3' },
  ],
  poly: [
    { name:'Binance', addr:'0xab5c66752a9e8167967685f1450532fb96d5d24f' },
    { name:'OKX',     addr:'0x2716b1b3dea3a8d16ef5ca5e5e617a76daa23be2' },
    { name:'MEXC',    addr:'0x4982085c9e2f89f2ecb8131eca71afad896e89cb' },
  ],
};

// 모든 거래소 주소 셋 (CEX→CEX 제외용)
const EX_SET = new Set();
const EX_LABEL = new Map(); // addr → name
for(const ch of Object.keys(EXCHANGES)) {
  for(const e of EXCHANGES[ch]) {
    EX_SET.add(e.addr.toLowerCase());
    EX_LABEL.set(e.addr.toLowerCase(), e.name);
  }
}

// ── LP Position Manager (제외 대상) ──
const LP_MANAGERS = new Set([
  '0xc36442b4a4522e871399cd717abdd847ab11fe88', // Uniswap V3 NFT PM
  '0xbd216513d74c8cf14cf4747e6aaa6420ff64ee9e', // Uniswap V4 PM
  '0x46a15b0b27311cedf172ab29e4f4766fbe7f4364', // PancakeSwap V3 PM
  '0x556b9306565093c855aea9ae92a594704c2cd59e', // PancakeSwap MasterChef V3
  '0x2214a42d8e2a1d20635c2cb0664422c528b6a432', // SushiSwap V3 PM
]);
const DEX_BLACKLIST = LP_MANAGERS;

// ── DEX 라우터 (스왑 분류용 — 제외 안 함) ──
const DEX_ROUTERS = new Map([
  // Uniswap
  ['0x7a250d5630b4cf539739df2c5dacb4c659f2488d', 'Uniswap V2'],
  ['0xe592427a0aece92de3edee1f18e0157c05861564', 'Uniswap V3'],
  ['0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45', 'Uniswap V3'],
  ['0xef1c6e67703c7bd7107eed8303fbe6ec2554bf6b', 'Uniswap Universal'],
  ['0x66a9893cc07d91d95644aedd05d03f95e1dba8af', 'Uniswap V4'],
  // PancakeSwap
  ['0x10ed43c718714eb63d5aa57b78b54704e256024e', 'PancakeSwap V2'],
  ['0x13f4ea83d0bd40e75c8222255bc855a974568dd4', 'PancakeSwap V3'],
  ['0x1a0a18ac4becddbd6389559687d1a73d8927e416', 'PancakeSwap Smart'],
  // SushiSwap
  ['0xd9e1ce17f2641f24ae83637ab66a2cca9c378b9f', 'SushiSwap'],
  ['0x1b02da8cb0d097eb8d57a175b88c7d8b47997506', 'SushiSwap'],
  // 1inch
  ['0x1111111254fb6c44bac0bed2854e76f90643097d', '1inch v4'],
  ['0x1111111254eeb25477b68fb85ed929f73a960582', '1inch v5'],
  ['0x111111125421ca6dc452d289314280a0f8842a65', '1inch v6'],
  // 0x
  ['0xdef1c0ded9bec7f1a1670819833240f027b25eff', '0x'],
  ['0xdef1abe32c034e558cdd535791643c58a13acc10', '0x v2'],
  // Paraswap
  ['0x216b4b4ba9f3e719726886d34a177484278bfcae', 'Paraswap'],
  ['0xdef171fe48cf0115b1d80b88dc8eab59176fee57', 'Paraswap'],
  ['0x6a000f20005980200259b80c5102003040001068', 'Paraswap'],
  // KyberSwap
  ['0x6131b5fae19ea4f9d964eac0408e4408b66337b5', 'KyberSwap'],
  // Curve
  ['0x99a58482bd75cbab83b27ec03ca68ff489b5788f', 'Curve'],
  ['0xfa9a30350048b2bf66865ee20363067c66f67e58', 'Curve'],
  // Balancer Vault
  ['0xba12222222228d8ba445958a75a0704d566bf2c8', 'Balancer'],
  // ODOS
  ['0xcf5540fffcdc3d510b18bfca6d2b9987b0772559', 'ODOS'],
  ['0xa669e7a0d4b3e4fa48af2de86bd4cd7126be4e13', 'ODOS'],
  // DODO
  ['0xa356867fdcea8e71aeaf87805808803806231fdc', 'DODO'],
  // CowSwap
  ['0x9008d19f58aabd9ed0d60971565aa8510560ab41', 'CowSwap'],
]);

// ── Bridge contracts ──
const BRIDGE_CONTRACTS = new Map([
  ['0x4200000000000000000000000000000000000010', 'Optimism Bridge'],
  ['0x99c9fc46f92e8a1c0dec1b1747d010903e884be1', 'Optimism Bridge L1'],
  ['0xa3a7b6f88361f48403514059f1f16c8e78d60eec', 'Arbitrum Bridge L1'],
  ['0xcee284f754e854890e311e3280b767f80797180d', 'Across'],
  ['0x8731d54e9d02c286767d56ac03e8037c07e01e98', 'Stargate'],
  ['0x150f94b44927f078737562f0fcf3c95c01cc2376', 'Stargate ETH'],
  ['0x40c57923924b5c5c5455c48d93317139addac8fb', 'Wormhole'],
  ['0xb8901acb165ed027e32754e0ffe830802919727f', 'Wormhole'],
  ['0x6b7a87899490ece95443e979ca9485cbe7e71522', 'Hop Protocol'],
  ['0x3e4a3a4796d16c0cd582c382691998f7c06420b6', 'Synapse'],
]);

// 트랜잭션 유형 분류
function classifyTxType(from, to, fromEx, toEx) {
  // 거래소
  if (fromEx && !toEx) return { type: 'cex_out', label: '🏦↗ 거래소출금', tag: fromEx };
  if (!fromEx && toEx) return { type: 'cex_in',  label: '🏦↙ 거래소입금', tag: toEx };
  if (fromEx && toEx)  return { type: 'cex_int', label: '🏦↔ 거래소내부', tag: fromEx+'→'+toEx };
  // DEX swap
  const fromDex = DEX_ROUTERS.get(from);
  const toDex   = DEX_ROUTERS.get(to);
  if (fromDex || toDex) return { type: 'dex_swap', label: '🔁 DEX스왑', tag: fromDex || toDex };
  // Bridge
  const fromBridge = BRIDGE_CONTRACTS.get(from);
  const toBridge   = BRIDGE_CONTRACTS.get(to);
  if (fromBridge || toBridge) return { type: 'bridge', label: '🌉 브리지', tag: fromBridge || toBridge };
  // LP (걸러져서 여기 안 옴)
  if (LP_MANAGERS.has(from) || LP_MANAGERS.has(to)) return { type: 'lp', label: '💧 LP', tag: 'LP' };
  // EOA-EOA
  return { type: 'p2p', label: '👤 개인이체', tag: '' };
}

// ── State ──
const state = {
  ws: {},                    // chain → WebSocket (cex 전용)
  wsFull: {},                // chain → WebSocket (full 모드)
  reconnect: {},
  priceCache: new Map(),
  metaCache: new Map(),
  binanceWL: null,
  binanceWLts: 0,
  seenHashes: new Set(),
  stats: { detected: 0, sent: 0, errors: 0, startedAt: Date.now() },
  recentTxs: [],
  clients: new Set(),
  caList: { eth: [], bsc: [], arb: [], base: [], poly: [] },  // 풀모드 CA 구독 대상
  caListTs: 0,
  lpAddresses: new Set(),    // 동적으로 감지된 LP/pair (handleLog에서 제외)
};

// SCAN_MODE: 'cex' | 'full'  (env로 토글)
const SCAN_MODE = (process.env.SCAN_MODE || 'full').toLowerCase();
console.log(`[CONFIG] SCAN_MODE=${SCAN_MODE}`);

// ── Util ──
function log(...args) { console.log(`[${new Date().toISOString()}]`, ...args); }
function fN(n) {
  if (n >= 1e9) return (n / 1e9).toFixed(2) + 'B';
  if (n >= 1e6) return (n / 1e6).toFixed(2) + 'M';
  if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K';
  return n.toFixed(0);
}
function addrToTopic(addr) {
  return '0x' + '0'.repeat(24) + addr.toLowerCase().replace(/^0x/, '');
}
function abiDecodeString(hex) {
  if (!hex || hex === '0x') return '';
  try {
    const h = hex.startsWith('0x') ? hex.slice(2) : hex;
    if (h.length >= 128) {
      const off = parseInt(h.slice(0, 64), 16);
      if (off === 32) {
        const len = parseInt(h.slice(64, 128), 16);
        if (len > 0 && len <= 64) {
          let str = '';
          const chunk = h.slice(128, 128 + len * 2);
          for (let i = 0; i < chunk.length; i += 2) str += String.fromCharCode(parseInt(chunk.slice(i, i + 2), 16));
          return str.replace(/[^\x20-\x7E]/g, '').trim();
        }
      }
    }
    let b = '';
    for (let j = 0; j < Math.min(64, h.length); j += 2) b += String.fromCharCode(parseInt(h.slice(j, j + 2), 16));
    return b.replace(/[\x00-\x1F\x7F-\xFF]/g, '').trim();
  } catch (e) { return ''; }
}

// ── Alchemy HTTP RPC ──
async function rpcCall(chain, method, params) {
  const res = await fetch(CHAINS[chain].http, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ jsonrpc: '2.0', method, params, id: 1 }),
  });
  const d = await res.json();
  if (d.error) throw new Error(d.error.message);
  return d.result;
}

async function getTokenMeta(chain, ca) {
  const key = `${chain}:${ca.toLowerCase()}`;
  if (state.metaCache.has(key)) return state.metaCache.get(key);
  try {
    const [symHex, decHex, supHex] = await Promise.all([
      rpcCall(chain, 'eth_call', [{ to: ca, data: '0x95d89b41' }, 'latest']),
      rpcCall(chain, 'eth_call', [{ to: ca, data: '0x313ce567' }, 'latest']),
      rpcCall(chain, 'eth_call', [{ to: ca, data: '0x18160ddd' }, 'latest']),
    ]);
    const symbol = (abiDecodeString(symHex) || '?').toUpperCase();
    const decimals = parseInt(decHex || '0x12', 16) || 18;
    let totalSupply = 0;
    try { totalSupply = Number(BigInt(supHex || '0x0')) / Math.pow(10, decimals); } catch (e) {}
    const meta = { symbol, decimals, totalSupply };
    state.metaCache.set(key, meta);
    return meta;
  } catch (e) {
    return { symbol: '?', decimals: 18, totalSupply: 0 };
  }
}

// ── Price ──
async function getPrice(symbol, ca) {
  const key = symbol.toUpperCase();
  const cached = state.priceCache.get(key);
  if (cached && Date.now() - cached.ts < 5 * 60 * 1000) return cached.price;
  // Binance
  try {
    const r = await fetch(`https://api.binance.com/api/v3/ticker/price?symbol=${key}USDT`);
    if (r.ok) {
      const d = await r.json();
      const p = parseFloat(d.price);
      if (p > 0) {
        state.priceCache.set(key, { price: p, ts: Date.now() });
        return p;
      }
    }
  } catch (e) {}
  // DexScreener
  try {
    const r = await fetch(`https://api.dexscreener.com/latest/dex/tokens/${ca}`);
    if (r.ok) {
      const d = await r.json();
      const valid = (d.pairs || []).filter(p => p.priceUsd && parseFloat(p.priceUsd) > 0);
      if (valid.length) {
        valid.sort((a, b) => (b.liquidity?.usd || 0) - (a.liquidity?.usd || 0));
        const p = parseFloat(valid[0].priceUsd);
        state.priceCache.set(key, { price: p, ts: Date.now() });
        return p;
      }
    }
  } catch (e) {}
  return 0;
}

// ── CA 리스트 빌드 (CoinGecko top 1000 ∩ Binance whitelist) ──
async function buildCAList() {
  const BNB = await loadBinanceWhitelist();
  if (!BNB || BNB.size === 0) {
    log('CA build skipped — Binance WL empty');
    return state.caList;
  }
  log(`CA list 빌드 시작 (BNB: ${BNB.size}심볼, CoinGecko 페치 중...)`);

  // CoinGecko coins/list?include_platform=true (1회, ~1MB)
  let coinsList = [];
  try {
    const r = await fetch('https://api.coingecko.com/api/v3/coins/list?include_platform=true', {
      signal: AbortSignal.timeout(30000)
    });
    log(`coins/list HTTP ${r.status}`);
    if (r.ok) coinsList = await r.json();
  } catch (e) { log('coins/list err:', e.message); }
  if (!Array.isArray(coinsList) || coinsList.length === 0) {
    log(`coins/list 실패 (got ${typeof coinsList}, ${coinsList?.length || 0})`);
    return state.caList;
  }
  log(`coins/list 받음: ${coinsList.length}개`);

  // markets top 1000 (시총 순)
  let topCoins = [];
  try {
    for (let page = 1; page <= 4; page++) {
      const r = await fetch(`https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=250&page=${page}`, {
        signal: AbortSignal.timeout(15000)
      });
      log(`markets page ${page} HTTP ${r.status}`);
      if (r.ok) {
        const d = await r.json();
        if (Array.isArray(d)) topCoins = topCoins.concat(d);
      }
      await new Promise(rs => setTimeout(rs, 500));
    }
  } catch (e) { log('markets err:', e.message); }
  log(`markets 받음: ${topCoins.length}개 코인`);

  // id → platforms 인덱스
  const idToPlatforms = {};
  coinsList.forEach(c => { idToPlatforms[c.id] = c.platforms || {}; });

  const platformKey = {
    eth: 'ethereum', bsc: 'binance-smart-chain',
    arb: 'arbitrum-one', base: 'base', poly: 'polygon-pos',
  };
  const newCAList = { eth: [], bsc: [], arb: [], base: [], poly: [] };
  const seen = new Set();

  topCoins.forEach(c => {
    const sym = (c.symbol || '').toUpperCase();
    if (!BNB.has(sym)) return;
    if (EXCLUDE.has(sym)) return; // 메이저/스테이블 제외
    const platforms = idToPlatforms[c.id];
    if (!platforms) return;
    for (const ch of Object.keys(platformKey)) {
      const ca = platforms[platformKey[ch]];
      if (ca && ca.startsWith('0x') && ca.length === 42) {
        const key = ch + ':' + ca.toLowerCase();
        if (!seen.has(key)) {
          newCAList[ch].push(ca.toLowerCase());
          seen.add(key);
        }
      }
    }
  });

  state.caList = newCAList;
  state.caListTs = Date.now();
  const total = Object.values(newCAList).reduce((s, a) => s + a.length, 0);
  log(`CA list 빌드 완료: ${total}개 (eth:${newCAList.eth.length} bsc:${newCAList.bsc.length} arb:${newCAList.arb.length} base:${newCAList.base.length} poly:${newCAList.poly.length})`);
  return newCAList;
}

// ── Binance whitelist ──
async function loadBinanceWhitelist() {
  if (state.binanceWL && Date.now() - state.binanceWLts < 24 * 3600 * 1000) return state.binanceWL;
  const bases = new Set();
  try {
    const sr = await fetch('https://api.binance.com/api/v3/exchangeInfo');
    const sd = await sr.json();
    (sd.symbols || []).filter(s => s.status === 'TRADING' && ['USDT', 'USDC', 'BTC', 'FDUSD'].includes(s.quoteAsset))
      .forEach(s => bases.add(s.baseAsset.toUpperCase()));
  } catch (e) { log('Binance Spot fetch failed:', e.message); }
  try {
    const fr = await fetch('https://fapi.binance.com/fapi/v1/exchangeInfo');
    const fd = await fr.json();
    (fd.symbols || []).filter(s => s.status === 'TRADING' && s.contractType === 'PERPETUAL')
      .forEach(s => bases.add(s.baseAsset.toUpperCase()));
  } catch (e) { log('Binance Futures fetch failed:', e.message); }
  // CoinGecko fallback
  if (bases.size < 100) {
    try {
      for (let page = 1; page <= 4; page++) {
        const r = await fetch(`https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=250&page=${page}`);
        if (r.ok) {
          const d = await r.json();
          if (Array.isArray(d)) d.forEach(c => bases.add((c.symbol || '').toUpperCase()));
        }
        await new Promise(rs => setTimeout(rs, 200));
      }
    } catch (e) { log('CoinGecko fallback failed:', e.message); }
  }
  state.binanceWL = bases;
  state.binanceWLts = Date.now();
  log(`Binance whitelist loaded: ${bases.size} symbols`);
  return bases;
}

// ── Telegram ──
async function sendTelegram(text) {
  try {
    const r = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        chat_id: CHAT_ID,
        text,
        parse_mode: 'HTML',
        disable_web_page_preview: true,
      }),
    });
    if (r.ok) state.stats.sent++;
  } catch (e) { log('Telegram err:', e.message); state.stats.errors++; }
}

function formatMessage(r) {
  const tier = r.usd >= 1e6 ? '🔴 1M+' : r.usd >= 5e5 ? '🟠 500K+' : '🟡 100K+';
  const supplyStr = r.supplyPct > 0 ? ` (총 공급량의 ${r.supplyPct.toFixed(2)}%)` : '';
  return `${tier} <b>${r.sym}</b> 전송 감지\n\n` +
    `💰 가치: <b>$${fN(r.usd)}</b>\n` +
    `📊 가격: $${r.price < 1 ? r.price.toFixed(6) : r.price.toFixed(4)}\n` +
    `📦 수량: ${fN(r.amt)} ${r.sym}${supplyStr}\n` +
    `🔗 네트워크: ${CHAINS[r.chain].name}\n` +
    `🕐 ${new Date(r.ts).toLocaleString('ko-KR', { timeZone: 'Asia/Seoul' })}\n\n` +
    `🏦 From: <code>${r.from}</code>${r.fromEx ? ' (' + r.fromEx + ')' : ''}\n` +
    `👤 To: <code>${r.to}</code>${r.toEx ? ' (' + r.toEx + ')' : ''}\n\n` +
    `<a href="${CHAINS[r.chain].exp}/tx/${r.hash}">TX 보기</a> · ` +
    `<a href="${CHAINS[r.chain].exp}/token/${r.ca}">토큰</a>`;
}

// ── Log handler ──
async function handleLog(chain, log_, source) {
  try {
    const fromTopic = log_.topics[1] || '';
    const from = '0x' + fromTopic.slice(-40);
    const toTopic = log_.topics[2] || '';
    const to = '0x' + toTopic.slice(-40);

    // CEX 모드: from이 거래소여야만
    // FULL 모드: from/to 둘 다 검사
    const fromEx = EX_LABEL.get(from);
    const toEx = EX_LABEL.get(to);

    if (source === 'cex') {
      if (!fromEx) return;
      if (toEx) return; // CEX→CEX 제외
    } else if (source === 'full') {
      // CEX→CEX 제외
      if (fromEx && toEx) return;
    }

    // mint/burn 제외
    if (from === '0x0000000000000000000000000000000000000000') return;
    if (to === '0x0000000000000000000000000000000000000000') return;
    if (to === '0x000000000000000000000000000000000000dead') return;

    // DEX/Bridge/Aggregator 제외 (양방향)
    if (DEX_BLACKLIST.has(from) || DEX_BLACKLIST.has(to)) return;
    // 동적 LP pair 제외
    if (state.lpAddresses.has(from) || state.lpAddresses.has(to)) return;

    const ca = (log_.address || '').toLowerCase();
    const hash = log_.transactionHash;
    const dedup = `${chain}-${hash}-${log_.logIndex}`;
    if (state.seenHashes.has(dedup)) return;
    state.seenHashes.add(dedup);
    if (state.seenHashes.size > 50000) {
      const arr = Array.from(state.seenHashes);
      state.seenHashes = new Set(arr.slice(-30000));
    }

    // 메타데이터
    const meta = await getTokenMeta(chain, ca);
    if (!meta.symbol || meta.symbol === '?') return;
    if (EXCLUDE.has(meta.symbol)) return;

    // Binance whitelist
    const BNB = await loadBinanceWhitelist();
    if (BNB && BNB.size && !BNB.has(meta.symbol)) return;

    // 수량
    let raw = 0n;
    try { raw = BigInt(log_.data); } catch (e) {}
    const amt = Number(raw) / Math.pow(10, meta.decimals);
    if (amt <= 0) return;

    // 가격
    const price = await getPrice(meta.symbol, ca);
    if (!price) return;
    const usd = amt * price;
    if (usd < MIN_USD) return;

    // 발행량 %
    const supplyPct = meta.totalSupply > 0 ? (amt / meta.totalSupply * 100) : 0;

    // 트랜잭션 유형 분류
    const txClass = classifyTxType(from, to, fromEx, toEx);

    state.stats.detected++;
    const r = {
      chain, sym: meta.symbol, ca, amt, price, usd,
      from, to, fromEx, toEx, hash, supplyPct,
      tx_type: txClass.type,
      tx_label: txClass.label,
      tx_tag: txClass.tag,
      ts: Date.now(),
    };
    log_msg(`[${chain.toUpperCase()}] ${meta.symbol} $${fN(usd)} ${fromEx}→${to.slice(0, 8)}…`);

    // SQLite 영구 저장
    try {
      stmtInsert.run({
        ts: r.ts, chain: r.chain, sym: r.sym, ca: r.ca,
        amt: r.amt, price: r.price, usd: r.usd, supply_pct: r.supplyPct || 0,
        ex_from: r.fromEx || null, ex_to: r.toEx || null,
        addr_from: r.from, addr_to: r.to, hash: r.hash,
        tx_type: r.tx_type || null, tx_tag: r.tx_tag || null,
      });
    } catch (e) { console.warn('db insert err:', e.message); }

    // 메모리 큐 (최근 200건)
    state.recentTxs.unshift(r);
    if (state.recentTxs.length > 200) state.recentTxs = state.recentTxs.slice(0, 200);

    // 연결된 브라우저 클라이언트들에게 broadcast
    const payload = JSON.stringify({ type: 'tx', data: r });
    for (const client of state.clients) {
      if (client.readyState === WebSocket.OPEN) {
        try { client.send(payload); } catch (e) {}
      }
    }

    await sendTelegram(formatMessage(r));
  } catch (e) {
    state.stats.errors++;
    log('handleLog err:', e.message);
  }
}

function log_msg(...args) { log(...args); }

// ── Full mode WebSocket: subscribe to token CAs (Transfer events) ──
function connectChainFull(chain) {
  const cas = state.caList[chain] || [];
  if (cas.length === 0) {
    log(`[${chain}-full] CA list empty, skipping`);
    return;
  }
  // Alchemy: address 필터 최대 ~1000개 / 일부 RPC는 더 작음
  // 1000개씩 청크로 나눠서 여러 구독
  const CHUNK = chain === 'bsc' ? 100 : 1000;
  const chunks = [];
  for (let i = 0; i < cas.length; i += CHUNK) chunks.push(cas.slice(i, i + CHUNK));

  const cfg = CHAINS[chain];
  log(`[${chain}-full] WebSocket 연결 (${cas.length}개 CA, ${chunks.length}개 sub)...`);
  const ws = new WebSocket(cfg.wss);
  state.wsFull[chain] = ws;
  const subIds = new Set();

  ws.on('open', () => {
    log(`[${chain}-full] ✓ 연결됨`);
    // BSC는 PublicNode 사용 → 청크 크기 줄임 + 순차 전송
    if (chain === 'bsc') {
      (async () => {
        for (let i = 0; i < chunks.length; i++) {
          ws.send(JSON.stringify({
            jsonrpc: '2.0',
            method: 'eth_subscribe',
            params: ['logs', { address: chunks[i], topics: [TRANSFER_TOPIC] }],
            id: 2000 + i,
          }));
          await new Promise(r => setTimeout(r, 200));
        }
      })();
    } else {
      chunks.forEach((chunk, i) => {
        ws.send(JSON.stringify({
          jsonrpc: '2.0',
          method: 'eth_subscribe',
          params: ['logs', { address: chunk, topics: [TRANSFER_TOPIC] }],
          id: 2000 + i,
        }));
      });
    }
  });

  ws.on('message', async (data) => {
    try {
      const msg = JSON.parse(data.toString());
      if (msg.id != null && msg.result) {
        subIds.add(msg.result);
        if (subIds.size === chunks.length) {
          log(`[${chain}-full] ✓ 구독 시작 (${cas.length}개 CA, ${chunks.length}개 sub)`);
        }
        return;
      }
      if (msg.id != null && msg.error) {
        log(`[${chain}-full] ❌ 구독 실패 (id=${msg.id}):`, msg.error.message);
        return;
      }
      if (msg.method === 'eth_subscription' && msg.params?.result) {
        await handleLog(chain, msg.params.result, 'full');
      }
    } catch (e) { log(`[${chain}-full] msg parse err:`, e.message); }
  });

  ws.on('error', (err) => log(`[${chain}-full] ws error:`, err.message));

  ws.on('close', () => {
    log(`[${chain}-full] 🔴 연결 끊김 — 5초 후 재연결...`);
    delete state.wsFull[chain];
    if (state.caList[chain]?.length > 0) {
      setTimeout(() => connectChainFull(chain), 5000);
    }
  });

  const pingTimer = setInterval(() => {
    if (ws.readyState === WebSocket.OPEN) {
      try { ws.ping(); } catch (e) {}
    } else {
      clearInterval(pingTimer);
    }
  }, 30000);
}

// ── WebSocket connection ──
function connectChain(chain) {
  const cfg = CHAINS[chain];
  log(`[${chain}] WebSocket 연결 중... ${cfg.wss.replace(ALCHEMY_KEY, '***')}`);
  const ws = new WebSocket(cfg.wss);
  state.ws[chain] = ws;

  // BSC는 거래소 OR 필터를 일정 개수 이상이면 거부 → topic0만 구독, 클라이언트에서 from 매칭
  // 다른 체인은 한 번에 array로 가능
  const isBsc = chain === 'bsc';
  const subIds = new Set();

  ws.on('open', () => {
    log(`[${chain}] ✓ 연결됨`);
    const exchangeAddrs = EXCHANGES[chain] || [];

    if (isBsc) {
      // BSC: PublicNode 순차 구독 (rate limit 방지)
      (async () => {
        for (let idx = 0; idx < exchangeAddrs.length; idx++) {
          const ex = exchangeAddrs[idx];
          ws.send(JSON.stringify({
            jsonrpc: '2.0',
            method: 'eth_subscribe',
            params: ['logs', { topics: [TRANSFER_TOPIC, addrToTopic(ex.addr)] }],
            id: 1000 + idx,
          }));
          await new Promise(r => setTimeout(r, 200));
        }
      })();
    } else {
      // 일반: 한 번에 array
      const addrTopics = exchangeAddrs.map(e => addrToTopic(e.addr));
      ws.send(JSON.stringify({
        jsonrpc: '2.0',
        method: 'eth_subscribe',
        params: ['logs', { topics: [TRANSFER_TOPIC, addrTopics] }],
        id: 1,
      }));
    }
  });

  ws.on('message', async (data) => {
    try {
      const msg = JSON.parse(data.toString());
      // 구독 응답
      if (msg.id != null && msg.result) {
        subIds.add(msg.result);
        if (isBsc) {
          // 각 응답 즉시 로깅
          log(`[${chain}] sub ok id=${msg.id} (${subIds.size}/${(EXCHANGES[chain] || []).length})`);
        } else {
          log(`[${chain}] ✓ 구독 시작 (${(EXCHANGES[chain] || []).length}개 거래소, 1개 sub)`);
        }
        return;
      }
      if (msg.id != null && msg.error) {
        log(`[${chain}] ❌ 구독 실패 (id=${msg.id}):`, msg.error.message);
        return;
      }
      if (msg.method === 'eth_subscription' && msg.params?.result) {
        await handleLog(chain, msg.params.result, 'cex');
      }
    } catch (e) { log(`[${chain}] msg parse err:`, e.message); }
  });

  ws.on('error', (err) => log(`[${chain}] ws error:`, err.message));

  ws.on('close', () => {
    log(`[${chain}] 🔴 연결 끊김 — 5초 후 재연결...`);
    delete state.ws[chain];
    state.reconnect[chain] = setTimeout(() => connectChain(chain), 5000);
  });

  // Keepalive ping (30초)
  const pingTimer = setInterval(() => {
    if (ws.readyState === WebSocket.OPEN) {
      try { ws.ping(); } catch (e) {}
    } else {
      clearInterval(pingTimer);
    }
  }, 30000);
}

// ── HTTP server (헬스체크 + 최근 트랜잭션 + WebSocket 업그레이드) ──
function setCors(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', '*');
}

const httpServer = http.createServer((req, res) => {
  setCors(res);
  if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }

  if (req.url === '/health' || req.url === '/') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    const uptime = Math.floor((Date.now() - state.stats.startedAt) / 1000);
    let dbTotal = 0;
    try { dbTotal = db.prepare('SELECT COUNT(*) AS c FROM txs').get().c; } catch (e) {}
    const totalCA = Object.values(state.caList).reduce((s, a) => s + a.length, 0);
    res.end(JSON.stringify({
      ok: true,
      uptime_sec: uptime,
      mode: SCAN_MODE,
      detected: state.stats.detected,
      sent: state.stats.sent,
      errors: state.stats.errors,
      clients: state.clients.size,
      recent_txs: state.recentTxs.length,
      db_total: dbTotal,
      db_path: DB_PATH,
      chains: {
        cex: Object.keys(state.ws).map(c => ({ chain: c, connected: state.ws[c]?.readyState === WebSocket.OPEN })),
        full: Object.keys(state.wsFull).map(c => ({ chain: c, connected: state.wsFull[c]?.readyState === WebSocket.OPEN, ca_count: state.caList[c]?.length || 0 })),
      },
      ca_total: totalCA,
      ca_per_chain: Object.fromEntries(Object.entries(state.caList).map(([k, v]) => [k, v.length])),
      cache: { prices: state.priceCache.size, meta: state.metaCache.size, seen: state.seenHashes.size },
      binance_wl: state.binanceWL?.size || 0,
      dex_blacklist: DEX_BLACKLIST.size,
    }, null, 2));
    return;
  }

  if (req.url === '/transactions' || req.url?.startsWith('/transactions?')) {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true, count: state.recentTxs.length, data: state.recentTxs }));
    return;
  }

  // /history?hours=24&chain=bsc&sym=PEPE&min_usd=100000&limit=500
  if (req.url?.startsWith('/history')) {
    try {
      const u = new URL(req.url, 'http://x');
      const hours = parseInt(u.searchParams.get('hours') || '24');
      const chain = u.searchParams.get('chain') || '';
      const sym = (u.searchParams.get('sym') || '').toUpperCase();
      const minUsd = parseFloat(u.searchParams.get('min_usd') || '0');
      const limit = Math.min(parseInt(u.searchParams.get('limit') || '500'), 5000);
      const fromTs = hours > 0 ? Date.now() - hours * 3600 * 1000 : 0;
      const rows = stmtFilter.all({ from_ts: fromTs, chain, sym, min_usd: minUsd, lim: limit });
      // DB row → JS object 형식으로 변환 (대시보드 호환)
      const data = rows.map(row => ({
        ts: row.ts, chain: row.chain, sym: row.sym, ca: row.ca,
        amt: row.amt, price: row.price, usd: row.usd, supplyPct: row.supply_pct,
        from: row.addr_from, to: row.addr_to,
        fromEx: row.ex_from, toEx: row.ex_to, hash: row.hash,
        tx_type: row.tx_type, tx_tag: row.tx_tag,
      }));
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: true, count: data.length, data }));
    } catch (e) {
      res.writeHead(500);
      res.end(JSON.stringify({ ok: false, error: e.message }));
    }
    return;
  }

  // /stats?days=7
  if (req.url?.startsWith('/stats')) {
    try {
      const u = new URL(req.url, 'http://x');
      const days = parseInt(u.searchParams.get('days') || '7');
      const fromTs = Date.now() - days * 86400 * 1000;
      const byChain = stmtStats.all(fromTs);
      const topSyms = stmtTopSyms.all(fromTs, 20);
      const total = db.prepare('SELECT COUNT(*) AS c, SUM(usd) AS sum FROM txs WHERE ts >= ?').get(fromTs);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: true, days, total: total.c, total_usd: total.sum || 0, by_chain: byChain, top_syms: topSyms }));
    } catch (e) {
      res.writeHead(500);
      res.end(JSON.stringify({ ok: false, error: e.message }));
    }
    return;
  }

  // /analytics/freq?days=1|7|30  → 토큰별 빈도 + USD + 평균 + 스파크라인
  if (req.url?.startsWith('/analytics/freq')) {
    try {
      const u = new URL(req.url, 'http://x');
      const days = parseInt(u.searchParams.get('days') || '7');
      const fromTs = Date.now() - days * 86400 * 1000;
      // 토큰별 집계
      const rows = db.prepare(`
        SELECT
          sym,
          chain,
          COUNT(*) AS cnt,
          SUM(usd) AS total_usd,
          AVG(usd) AS avg_usd,
          MAX(usd) AS max_usd,
          MIN(ts) AS first_ts,
          MAX(ts) AS last_ts
        FROM txs
        WHERE ts >= ?
        GROUP BY sym, chain
        ORDER BY cnt DESC
        LIMIT 200
      `).all(fromTs);

      // 각 토큰의 일별 분포 (스파크라인용)
      const sparkStmt = db.prepare(`
        SELECT
          CAST((ts - ?) / 86400000 AS INTEGER) AS day_idx,
          COUNT(*) AS cnt
        FROM txs
        WHERE ts >= ? AND sym = ? AND chain = ?
        GROUP BY day_idx
      `);
      rows.forEach(r => {
        const buckets = new Array(days).fill(0);
        const dist = sparkStmt.all(fromTs, fromTs, r.sym, r.chain);
        dist.forEach(d => {
          if (d.day_idx >= 0 && d.day_idx < days) buckets[d.day_idx] = d.cnt;
        });
        r.spark = buckets;
      });

      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: true, days, count: rows.length, data: rows }));
    } catch (e) {
      res.writeHead(500);
      res.end(JSON.stringify({ ok: false, error: e.message }));
    }
    return;
  }

  // /analytics/anomalies → 5가지 이상 신호 종합
  if (req.url?.startsWith('/analytics/anomalies')) {
    try {
      const now = Date.now();
      const anomalies = [];

      // 1. 폭증 (24h 횟수 > 7일 평균 × 3, 최소 5건)
      const spikes = db.prepare(`
        WITH h24 AS (
          SELECT sym, chain, COUNT(*) AS cnt, SUM(usd) AS total_usd
          FROM txs
          WHERE ts >= ?
          GROUP BY sym, chain
        ),
        avg7 AS (
          SELECT sym, chain, COUNT(*)/7.0 AS avg_daily
          FROM txs
          WHERE ts >= ? AND ts < ?
          GROUP BY sym, chain
        )
        SELECT h24.sym, h24.chain, h24.cnt, h24.total_usd,
               COALESCE(avg7.avg_daily, 0.5) AS avg_daily
        FROM h24
        LEFT JOIN avg7 ON h24.sym = avg7.sym AND h24.chain = avg7.chain
        WHERE h24.cnt >= 5 AND h24.cnt > COALESCE(avg7.avg_daily, 0.5) * 3
        ORDER BY (h24.cnt * 1.0 / COALESCE(avg7.avg_daily, 0.5)) DESC
        LIMIT 20
      `).all(now - 86400000, now - 7 * 86400000, now - 86400000);
      spikes.forEach(s => anomalies.push({
        type: 'spike',
        severity: s.cnt > s.avg_daily * 5 ? 'high' : 'med',
        title: `${s.sym} 폭증 (${s.chain.toUpperCase()})`,
        desc: `24h ${s.cnt}건 (7일 평균 ${s.avg_daily.toFixed(1)}건의 ${(s.cnt / s.avg_daily).toFixed(1)}x ↑)`,
        sym: s.sym, chain: s.chain, value: s.cnt, total_usd: s.total_usd,
      }));

      // 2. 메가 트랜잭션 (24h, $5M+)
      const megas = db.prepare(`
        SELECT * FROM txs
        WHERE ts >= ? AND usd >= 5000000
        ORDER BY usd DESC
        LIMIT 20
      `).all(now - 86400000);
      megas.forEach(m => anomalies.push({
        type: 'mega',
        severity: m.usd >= 1e7 ? 'high' : 'med',
        title: `🐋 메가 트랜잭션 $${(m.usd/1e6).toFixed(2)}M ${m.sym}`,
        desc: `${m.ex_from || '?'} → ${m.addr_to.slice(0,10)}…`,
        sym: m.sym, chain: m.chain, ts: m.ts, hash: m.hash, total_usd: m.usd,
      }));

      // 3. 클러스터 (1시간 내 같은 거래소→같은 토큰 5건+)
      const clusters = db.prepare(`
        SELECT ex_from, sym, chain, COUNT(*) AS cnt, SUM(usd) AS total_usd, MIN(ts) AS first, MAX(ts) AS last
        FROM txs
        WHERE ts >= ? AND ex_from IS NOT NULL
        GROUP BY ex_from, sym, chain
        HAVING cnt >= 5
        ORDER BY cnt DESC
        LIMIT 20
      `).all(now - 3600000);
      clusters.forEach(c => anomalies.push({
        type: 'cluster',
        severity: c.cnt >= 10 ? 'high' : 'med',
        title: `🔁 클러스터: ${c.ex_from} → ${c.sym}`,
        desc: `1시간 내 ${c.cnt}건, $${(c.total_usd/1e6).toFixed(2)}M`,
        sym: c.sym, chain: c.chain, value: c.cnt, total_usd: c.total_usd,
      }));

      // 4. 신규 토큰 (24h 내 첫 등장)
      const newcomers = db.prepare(`
        SELECT sym, chain, COUNT(*) AS cnt, SUM(usd) AS total_usd, MIN(ts) AS first_seen
        FROM txs
        WHERE ts >= ?
          AND (sym, chain) NOT IN (
            SELECT DISTINCT sym, chain FROM txs WHERE ts < ?
          )
        GROUP BY sym, chain
        ORDER BY total_usd DESC
        LIMIT 20
      `).all(now - 86400000, now - 86400000);
      newcomers.forEach(n => anomalies.push({
        type: 'newcomer',
        severity: n.total_usd >= 1e6 ? 'high' : 'med',
        title: `🎯 신규: ${n.sym} (${n.chain.toUpperCase()})`,
        desc: `24h 내 첫 등장, ${n.cnt}건 $${(n.total_usd/1e6).toFixed(2)}M`,
        sym: n.sym, chain: n.chain, value: n.cnt, total_usd: n.total_usd, ts: n.first_seen,
      }));

      // 5. 단일 수신자 누적 (24h, 3개+ 토큰, $500K+)
      const accumulators = db.prepare(`
        SELECT addr_to, COUNT(DISTINCT sym) AS uniq_syms, COUNT(*) AS cnt, SUM(usd) AS total_usd
        FROM txs
        WHERE ts >= ?
        GROUP BY addr_to
        HAVING uniq_syms >= 3 AND total_usd >= 500000
        ORDER BY total_usd DESC
        LIMIT 20
      `).all(now - 86400000);
      accumulators.forEach(a => anomalies.push({
        type: 'accumulator',
        severity: a.total_usd >= 2e6 ? 'high' : 'med',
        title: `📈 누적 수신자 ${a.addr_to.slice(0,10)}…`,
        desc: `24h ${a.uniq_syms}개 토큰 ${a.cnt}건 $${(a.total_usd/1e6).toFixed(2)}M`,
        addr: a.addr_to, value: a.uniq_syms, total_usd: a.total_usd,
      }));

      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: true, count: anomalies.length, data: anomalies }));
    } catch (e) {
      res.writeHead(500);
      res.end(JSON.stringify({ ok: false, error: e.message }));
    }
    return;
  }

  res.writeHead(404);
  res.end('Not found');
});

// WebSocket server (브라우저 클라이언트용)
const wss = new WebSocketServer({ server: httpServer, path: '/ws' });
wss.on('connection', (clientWs, req) => {
  state.clients.add(clientWs);
  log(`📡 client connected (total: ${state.clients.size})`);

  // 초기 데이터: DB에서 최근 500건 + 통계
  try {
    const rows = stmtRecent.all(500);
    const data = rows.map(row => ({
      ts: row.ts, chain: row.chain, sym: row.sym, ca: row.ca,
      amt: row.amt, price: row.price, usd: row.usd, supplyPct: row.supply_pct,
      from: row.addr_from, to: row.addr_to,
      fromEx: row.ex_from, toEx: row.ex_to, hash: row.hash,
      tx_type: row.tx_type, tx_tag: row.tx_tag,
    }));
    const total = db.prepare('SELECT COUNT(*) AS c FROM txs').get().c;
    clientWs.send(JSON.stringify({
      type: 'init',
      data,
      stats: {
        detected: state.stats.detected,
        uptime: Math.floor((Date.now() - state.stats.startedAt)/1000),
        db_total: total,
      }
    }));
  } catch (e) { console.warn('init send err:', e.message); }

  // ping 30초마다
  const pingTimer = setInterval(() => {
    if (clientWs.readyState === WebSocket.OPEN) {
      try { clientWs.ping(); } catch (e) {}
    } else {
      clearInterval(pingTimer);
    }
  }, 30000);

  clientWs.on('close', () => {
    state.clients.delete(clientWs);
    clearInterval(pingTimer);
    log(`📡 client disconnected (total: ${state.clients.size})`);
  });
  clientWs.on('error', (e) => log('client ws error:', e.message));
});

httpServer.listen(PORT, () => log(`HTTP+WS server on :${PORT}`));

// ── Boot ──
(async () => {
  log('🐋 Onchain Whale Worker starting...');
  log(`MIN_USD=${MIN_USD}, chains=${Object.keys(CHAINS).join(',')}, mode=${SCAN_MODE}`);
  await loadBinanceWhitelist();

  if (SCAN_MODE === 'full' || SCAN_MODE === 'both') {
    // CA 리스트 빌드 (CoinGecko top 1000 ∩ Binance)
    await buildCAList();
    // 매일 1회 갱신
    setInterval(() => { buildCAList().catch(e => log('CA rebuild err:', e.message)); }, 86400 * 1000);
  }

  if (SCAN_MODE === 'cex' || SCAN_MODE === 'both') {
    for (const chain of Object.keys(CHAINS)) {
      connectChain(chain);
    }
  }

  if (SCAN_MODE === 'full' || SCAN_MODE === 'both') {
    for (const chain of Object.keys(CHAINS)) {
      connectChainFull(chain);
    }
  }

  const totalCA = Object.values(state.caList).reduce((s, a) => s + a.length, 0);
  await sendTelegram(
    `🚀 <b>Whale Worker 시작</b>\n` +
    `모드: ${SCAN_MODE.toUpperCase()}\n` +
    `5체인 WebSocket 연결\n` +
    `임계값: $${fN(MIN_USD)}+\n` +
    (SCAN_MODE !== 'cex' ? `구독 토큰: ${totalCA}개 CA\n` : '') +
    `DEX/Bridge ${DEX_BLACKLIST.size}개 블랙리스트`
  );
})();

// Graceful shutdown
process.on('SIGTERM', () => {
  log('SIGTERM 받음 — 종료');
  Object.values(state.ws).forEach(ws => { try { ws.close(); } catch (e) {} });
  Object.values(state.wsFull).forEach(ws => { try { ws.close(); } catch (e) {} });
  try { db.close(); } catch (e) {}
  process.exit(0);
});
