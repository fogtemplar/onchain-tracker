// ════════════════════════════════════════════════════════════
// 🐋 Onchain Whale Worker (Real-time)
// Alchemy WebSocket × 5 chains → Telegram instant alerts
// ════════════════════════════════════════════════════════════

import WebSocket, { WebSocketServer } from 'ws';
import http from 'http';
import Database from 'better-sqlite3';
import fs from 'fs';
import path from 'path';
import pkg from 'js-sha3';
const { keccak256 } = pkg;

// ── Config (env or hardcoded fallback) ──
const ALCHEMY_KEY = process.env.ALCHEMY_KEY || 'I3Is5NQvnbgijvrbhdFp9';
const BOT_TOKEN   = process.env.BOT_TOKEN   || '7890873311:AAGMVgMBcFsWg9mcWE5Vi9ernwNPwoq0GVk';
const CHAT_ID     = process.env.CHAT_ID     || '-1003743061931';
const MIN_USD     = parseInt(process.env.MIN_USD || '100000');
const PORT        = parseInt(process.env.PORT || '3000');
const CMC_KEY     = process.env.CMC_KEY     || '7c23a703-55ff-4b19-babd-a4cf83aae98c';
const CG_API_KEY  = process.env.CG_API_KEY  || 'b745429f379948b8b715f6beded5c2ea';
const ETH_KEY     = process.env.ETHERSCAN_KEY || 'MFC6RKPAYYCWID4YEH9ZM7FJWTZPY9HM4Z';
const ARKHAM_KEY  = process.env.ARKHAM_KEY || '';

// ── Chain config ──
// WebSocket + HTTP: PublicNode 무료 → Alchemy CU 0
const CHAINS = {
  bsc:  { wss: 'wss://bsc-rpc.publicnode.com',            http: 'https://bsc-rpc.publicnode.com',            name: 'BSC',  exp: 'https://bscscan.com' },
  eth:  { wss: 'wss://ethereum-rpc.publicnode.com',        http: 'https://ethereum-rpc.publicnode.com',        name: 'ETH',  exp: 'https://etherscan.io' },
  arb:  { wss: 'wss://arbitrum-one-rpc.publicnode.com',    http: 'https://arbitrum-one-rpc.publicnode.com',    name: 'ARB',  exp: 'https://arbiscan.io' },
  base: { wss: 'wss://base-rpc.publicnode.com',            http: 'https://base-rpc.publicnode.com',            name: 'BASE', exp: 'https://basescan.org' },
  poly: { wss: 'wss://polygon-bor-rpc.publicnode.com',     http: 'https://polygon-bor-rpc.publicnode.com',     name: 'POLY', exp: 'https://polygonscan.com' },
};

const TRANSFER_TOPIC = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef';

// Etherscan V2 chainid 매핑
const CHAIN_ID = { eth: 1, bsc: 56, arb: 42161, base: 8453, poly: 137 };

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
try { db.exec('ALTER TABLE txs ADD COLUMN from_age TEXT'); } catch (e) {}
try { db.exec('ALTER TABLE txs ADD COLUMN to_age TEXT'); } catch (e) {}
try { db.exec('ALTER TABLE txs ADD COLUMN from_ens TEXT'); } catch (e) {}
try { db.exec('ALTER TABLE txs ADD COLUMN to_ens TEXT'); } catch (e) {}
try { db.exec('ALTER TABLE txs ADD COLUMN to_flags TEXT'); } catch (e) {}
try { db.exec('ALTER TABLE txs ADD COLUMN from_arkham TEXT'); } catch (e) {}
try { db.exec('ALTER TABLE txs ADD COLUMN to_arkham TEXT'); } catch (e) {}
try { db.exec('ALTER TABLE txs ADD COLUMN signals TEXT'); } catch (e) {}

// 사용자 라벨 테이블 (다중 사용자 공유)
db.exec(`
  CREATE TABLE IF NOT EXISTS wallet_labels (
    addr TEXT PRIMARY KEY,
    label TEXT NOT NULL,
    color TEXT,
    note TEXT,
    created_by TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
  )
`);
const stmtLabelGet = db.prepare('SELECT addr, label, color, note, created_by, updated_at FROM wallet_labels WHERE addr = ?');
const stmtLabelGetAll = db.prepare('SELECT addr, label, color, note, created_by, updated_at FROM wallet_labels');
const stmtLabelSet = db.prepare(`
  INSERT INTO wallet_labels (addr, label, color, note, created_by, created_at, updated_at)
  VALUES (?, ?, ?, ?, ?, ?, ?)
  ON CONFLICT(addr) DO UPDATE SET label=excluded.label, color=excluded.color, note=excluded.note, updated_at=excluded.updated_at
`);
const stmtLabelDel = db.prepare('DELETE FROM wallet_labels WHERE addr = ?');

const dbCount = db.prepare('SELECT COUNT(*) AS c FROM txs').get().c;
const labelCount = db.prepare('SELECT COUNT(*) AS c FROM wallet_labels').get().c;
console.log(`[DB] SQLite at ${DB_PATH} — ${dbCount} txs, ${labelCount} labels`);

// Prepared statements
const stmtInsert = db.prepare(`
  INSERT OR IGNORE INTO txs (ts, chain, sym, ca, amt, price, usd, supply_pct, ex_from, ex_to, addr_from, addr_to, hash, tx_type, tx_tag, from_age, to_age, from_ens, to_ens, to_flags, from_arkham, to_arkham, signals)
  VALUES (@ts, @chain, @sym, @ca, @amt, @price, @usd, @supply_pct, @ex_from, @ex_to, @addr_from, @addr_to, @hash, @tx_type, @tx_tag, @from_age, @to_age, @from_ens, @to_ens, @to_flags, @from_arkham, @to_arkham, @signals)
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

// ── LP Position Manager / Pool / Gauge (제외 대상) ──
const LP_MANAGERS = new Set([
  // ─ Uniswap (모든 체인 동일 주소) ─
  '0xc36442b4a4522e871399cd717abdd847ab11fe88', // V3 NFT PM (ETH/ARB/POLY/BASE/OP/BSC)
  '0xbd216513d74c8cf14cf4747e6aaa6420ff64ee9e', // V4 PM
  '0xa51afafe0263b40edaef0df8781ea9aa03e381a3', // V4 PoolManager (ETH)
  '0x498581ff718922c3f8e6a244956af099b2652b2b', // V4 PoolManager (BASE)
  // ─ PancakeSwap V3 (BSC/ETH/ARB/BASE 동일) ─
  '0x46a15b0b27311cedf172ab29e4f4766fbe7f4364', // V3 NonfungiblePositionManager
  '0x556b9306565093c855aea9ae92a594704c2cd59e', // MasterChef V3
  '0x41ff9aa7e16b8b1a8a8dc4f0efacd93d02d071c9', // PancakeSwap MasterChef V2 (BSC)
  // ─ SushiSwap V3 ─
  '0x2214a42d8e2a1d20635c2cb0664422c528b6a432', // V3 NFT PM (multi-chain)
  '0xf0cbce1942a68beb3d1b73f0dd86c8dcc363ef49', // Sushi V3 PM (alt)
  // ─ Aerodrome (Base) ─
  '0xf2a9280cec5d1bf46232b94580cd62165325567a', // Slipstream NFT PM (active)
  '0x827922686190790b37229fd06084350e74485b72', // CL Slipstream NFT PM (alt)
  '0xeC8E5342B19977B4eF8892e02D8DAEcfa1315831', // Slipstream Pool Factory
  '0xbe6d8f0d05cc4be24d5167a3ef062215be6d18a5', // Slipstream Swap Router
  '0xcf77a3ba9a5ca399b7c97c74d54e5b1beb874e43', // Aerodrome Router (V2)
  '0x82321f3beb69f503380d6b233857d5c43562e2d0', // CL200-WETH/AERO Pool
  // ─ Velodrome (Optimism — Aerodrome fork) ─
  '0x416b433906b1b72fa758e166e239c43d68dc6f29', // Slipstream NFT PM (Velodrome)
  '0xa062ae8a9c5e11aaa026fc2670b0d65ccc8b2858', // Slipstream NFT PM (alt)
  // ─ Camelot (Arbitrum) ─
  '0x00c7f3082833e796a5b3e4bd59f6642ff44dcd15', // V3 (Algebra) NFT PM
  '0xc873fecbd354f5a56e00e710b90ef4201db2448d', // V2 Router
  // ─ Thena (BSC — Algebra) ─
  '0x0927a5abbd02ed73ba83fc93bd9900b1c2e52348', // NonfungiblePositionManager
  // ─ QuickSwap (Polygon) ─
  '0xa5e0829caced8ffdd4de3c43696c57f7d7a678ff', // V2 Router
  '0xf5b509bb0909a69b1c207e495f687a596c168e12', // V3 NFT PM (Algebra)
  '0x8aac493fd8c78536ef193dbfa3ba2ed7d76dcb46', // V3 PM (alt)
  // ─ Trader Joe (ARB/BSC/AVAX) ─
  '0xb4315e873dbcf96ffd0acd8ea43f689d8c20fb30', // V2.1 LBRouter
  '0xb87215f1ec803d51c0fe3a8da6a5e34cda20bb4f', // V2 PM (alt)
  // ─ BaseSwap (Base) ─
  '0x327df1e6de05895d2ab08513aadd9313fe505d86', // V2 Router
  // ─ SwapBased (Base) ─
  '0xaaa3b1f1bd7bcc97fd1917c18ade665c5d31f067', // V2 Router
  // ─ Maverick V2 (multi-chain) ─
  '0x0000000000a38854e1f0e9c1075ec6c7e2cc6a5d', // PositionManager
  '0x0000007a005e3e8efc83a8c2eaa628fb6b8b0e3f', // Router
  // ─ Curve ─
  '0x99a58482bd75cbab83b27ec03ca68ff489b5788f', // Router
  '0xfa9a30350048b2bf66865ee20363067c66f67e58', // Curve Router NG
  // ─ Balancer Vault ─
  '0xba12222222228d8ba445958a75a0704d566bf2c8', // V2 Vault
  '0xba1333333333a1ba1108e8412f11850a5c319ba9', // V3 Vault
  // ─ Ramses (Arbitrum) ─
  '0xaa277cb7914b7e5514946da92cb9de332ce610ef', // V2 NFT PM
  '0x1aa07e8377d70b033ba139e007d51edf689b2ed3', // CL NFT PM
  // ─ KyberSwap Elastic ─
  '0xe222fbe074a436145b255442d919e4e3a6c6a480', // PositionManager
  '0x2b1c7b41f6a8f2b2bc45c3233a5d5fb3cd6dc9a8', // Anti-snipe PM
  // ─ Iziswap ─
  '0x110dE362cc436D7f54210f96b8C7652C2617887D', // PM
  // ─ DODO V3 ─
  '0xa356867fdcea8e71aeaf87805808803806231fdc', // Proxy
  '0xa2398842f37465f89540430bdc00219fa9e4d28a', // V2 Proxy
].map(s => s.toLowerCase()));
const DEX_BLACKLIST = LP_MANAGERS;

// 동적: token CA의 LP pair는 자동 감지 후 추가됨
// (Aerodrome CL pool 같은 건 toAddress가 pool contract 주소)

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
  wsFull: {},                // chain → WebSocket (EVM full 모드)
  wsSolFull: [],             // Solana WebSocket 연결들

  priceCache: new Map(),
  metaCache: new Map(),
  binanceWL: null,
  binanceWLts: 0,
  seenHashes: new Set(),
  stats: { detected: 0, sent: 0, errors: 0, startedAt: Date.now(), solMsgs: 0, solNotifs: 0, solSubs: 0, solParsed: 0 },
  recentTxs: [],
  clients: new Set(),
  caList: { eth: [], bsc: [], arb: [], base: [], poly: [], sol: [] },  // 풀모드 CA 구독 대상
  caListTs: 0,
  lpAddresses: new Set(),    // 동적으로 감지된 LP/pair (handleLog에서 제외)
};


// ── Util ──
function log(...args) { console.log(`[${new Date().toISOString()}]`, ...args); }
function fN(n) {
  if (n >= 1e9) return (n / 1e9).toFixed(2) + 'B';
  if (n >= 1e6) return (n / 1e6).toFixed(2) + 'M';
  if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K';
  return n.toFixed(0);
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

// ── HTTP RPC (PublicNode 우선 → Alchemy 폴백) ──
const ALCHEMY_HTTP = {
  bsc:  `https://bnb-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}`,
  eth:  `https://eth-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}`,
  arb:  `https://arb-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}`,
  base: `https://base-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}`,
  poly: `https://polygon-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}`,
};

async function rpcCall(chain, method, params) {
  const body = JSON.stringify({ jsonrpc: '2.0', method, params, id: 1 });
  const opts = { method: 'POST', headers: { 'Content-Type': 'application/json' }, body, signal: AbortSignal.timeout(8000) };

  // 1차: PublicNode (무료)
  try {
    const res = await fetch(CHAINS[chain].http, opts);
    const d = await res.json();
    if (!d.error) return d.result;
  } catch (e) {}

  // 2차: Alchemy 폴백 (CU 소비)
  const res = await fetch(ALCHEMY_HTTP[chain], { ...opts, signal: AbortSignal.timeout(8000) });
  const d = await res.json();
  if (d.error) throw new Error(d.error.message);
  return d.result;
}

// ── 지갑 나이 조회 (Etherscan V2 + SQLite 캐시) ──
db.exec(`
  CREATE TABLE IF NOT EXISTS wallet_age (
    chain_addr TEXT PRIMARY KEY,
    first_ts INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
  )
`);
const stmtWalletGet = db.prepare('SELECT first_ts FROM wallet_age WHERE chain_addr = ?');
const stmtWalletSet = db.prepare('INSERT OR REPLACE INTO wallet_age (chain_addr, first_ts, updated_at) VALUES (?, ?, ?)');

async function getWalletAge(chain, addr) {
  const key = `${chain}:${addr.toLowerCase()}`;

  // 1. SQLite 캐시 (30일 TTL)
  try {
    const row = stmtWalletGet.get(key);
    if (row && Date.now() - row.first_ts > 0) {
      const days = Math.floor((Date.now() - row.first_ts) / 86400000);
      return { days, firstTs: row.first_ts };
    }
  } catch (e) {}

  // 2. Etherscan V2 API — 첫 번째 일반 TX 조회
  const cid = CHAIN_ID[chain];
  if (!cid) return null;
  try {
    const url = `https://api.etherscan.io/v2/api?chainid=${cid}&module=account&action=txlist&address=${addr}&startblock=0&endblock=99999999&page=1&offset=1&sort=asc&apikey=${ETH_KEY}`;
    const r = await fetch(url, { signal: AbortSignal.timeout(8000) });
    if (!r.ok) return null;
    const d = await r.json();
    if (d.status === '1' && d.result?.length > 0) {
      const firstTs = parseInt(d.result[0].timeStamp) * 1000;
      const days = Math.floor((Date.now() - firstTs) / 86400000);
      try { stmtWalletSet.run(key, firstTs, Date.now()); } catch (e) {}
      return { days, firstTs };
    }
  } catch (e) {}
  return null;
}

// ── 단일토큰 지갑 체크 (Etherscan V2 + SQLite 캐시) ──
db.exec(`
  CREATE TABLE IF NOT EXISTS single_token_cache (
    chain_addr TEXT PRIMARY KEY,
    is_single INTEGER NOT NULL,
    token_ca TEXT,
    updated_at INTEGER NOT NULL
  )
`);
const stmtSingleGet = db.prepare('SELECT is_single, token_ca, updated_at FROM single_token_cache WHERE chain_addr = ?');
const stmtSingleSet = db.prepare('INSERT OR REPLACE INTO single_token_cache (chain_addr, is_single, token_ca, updated_at) VALUES (?, ?, ?, ?)');

async function checkSingleToken(chain, addr, currentCA) {
  const key = `${chain}:${addr.toLowerCase()}`;

  // 1. SQLite 캐시 (1일 TTL)
  try {
    const row = stmtSingleGet.get(key);
    if (row && Date.now() - row.updated_at < 86400000) {
      // 캐시된 결과가 현재 토큰과 같으면 단일토큰
      return row.is_single === 1 && row.token_ca === currentCA.toLowerCase();
    }
  } catch (e) {}

  // 2. Etherscan V2 — 최근 토큰 전송 조회
  const cid = CHAIN_ID[chain];
  if (!cid) return false;
  try {
    const url = `https://api.etherscan.io/v2/api?chainid=${cid}&module=account&action=tokentx&address=${addr}&page=1&offset=20&sort=desc&apikey=${ETH_KEY}`;
    const r = await fetch(url, { signal: AbortSignal.timeout(8000) });
    if (!r.ok) return false;
    const d = await r.json();
    if (d.status !== '1' || !d.result?.length) {
      // 토큰 TX 없음 = 첫 수신 → 단일토큰
      try { stmtSingleSet.run(key, 1, currentCA.toLowerCase(), Date.now()); } catch (e) {}
      return true;
    }
    const uniqueTokens = new Set(d.result.map(tx => tx.contractAddress.toLowerCase()));
    const isSingle = uniqueTokens.size === 1 && uniqueTokens.has(currentCA.toLowerCase());
    try { stmtSingleSet.run(key, isSingle ? 1 : 0, currentCA.toLowerCase(), Date.now()); } catch (e) {}
    return isSingle;
  } catch (e) { return false; }
}

function formatAge(age) {
  if (!age) return '?d';
  if (age.days >= 365) return Math.floor(age.days / 365) + 'y';
  return age.days + 'd';
}

// ── 이상거래 신호 탐지 ──
// 반환: [{code, emoji, label, desc, severity}]
function detectSignals(ctx) {
  const out = [];
  const { chain, sym, amt, usd, supplyPct, from, to, fromEx, toEx, fromAge, toAge, fromArkham, toArkham, txClass } = ctx;
  const now = Date.now();

  // A. 지갑 행동 이상 (단일 TX)
  if (fromAge && fromAge.days >= 730) {
    out.push({ code: 'diamond', emoji: '💎', label: '장기보유 이탈', desc: `${Math.floor(fromAge.days/365)}년+ 보유 지갑 첫 이체`, severity: 'high' });
  } else if (fromAge && fromAge.days >= 365) {
    out.push({ code: 'dormant', emoji: '🌙', label: '휴면지갑 활성', desc: `${Math.floor(fromAge.days/365)}년 휴면 지갑 활성화`, severity: 'high' });
  }

  // 탈출 패턴: 신규지갑 → 거래소
  if (toEx && fromAge && fromAge.days <= 7) {
    out.push({ code: 'escape', emoji: '🎭', label: '탈출 의심', desc: `신규지갑(${fromAge.days}d) → 거래소 입금`, severity: 'high' });
  }

  // Arkham 기반 신호
  const fundRe = /fund|vc|venture|capital|foundation|treasury/i;
  if (fromArkham && fundRe.test((fromArkham.type || '') + ' ' + (fromArkham.name || ''))) {
    out.push({ code: 'fund_out', emoji: '🏛', label: '펀드 출고', desc: `${fromArkham.name} 이동`, severity: 'high' });
  }
  if (toArkham && fundRe.test((toArkham.type || '') + ' ' + (toArkham.name || ''))) {
    out.push({ code: 'fund_in', emoji: '🏛', label: '펀드 입고', desc: `→ ${toArkham.name}`, severity: 'med' });
  }
  if (fromArkham && toArkham && !fromEx && !toEx) {
    out.push({ code: 'w2w', emoji: '🤝', label: 'Whale↔Whale', desc: `${fromArkham.name} → ${toArkham.name}`, severity: 'med' });
  }

  // C. 크기 이상
  if (supplyPct >= 1) {
    out.push({ code: 'supply', emoji: '🐋', label: '공급량 1%+', desc: `전체 공급의 ${supplyPct.toFixed(2)}%`, severity: 'high' });
  }
  if (usd >= 5e6) {
    out.push({ code: 'mega', emoji: '💰', label: '메가 TX', desc: `$${(usd/1e6).toFixed(1)}M 단일 이체`, severity: 'critical' });
  }

  // B. 패턴 신호 (DB 쿼리)
  try {
    // 집중 매집: 5분 내 같은 토큰 수신 3건+ 합 $1M+
    const accum = db.prepare(`
      SELECT COUNT(DISTINCT addr_to) AS uniq, SUM(usd) AS total, COUNT(*) AS cnt
      FROM txs WHERE sym=? AND chain=? AND ts>=?
    `).get(sym, chain, now - 5*60*1000);
    if (accum && accum.uniq >= 3 && accum.total >= 1e6) {
      out.push({ code: 'accum', emoji: '📈', label: '집중 매집', desc: `5분 내 ${accum.uniq}개 지갑 수신 $${(accum.total/1e6).toFixed(1)}M`, severity: 'high' });
    }

    // 분산 덤핑: 10분 내 3개+ CEX 입금
    const dump = db.prepare(`
      SELECT COUNT(DISTINCT ex_to) AS cnt, SUM(usd) AS total
      FROM txs WHERE sym=? AND chain=? AND ts>=? AND ex_to IS NOT NULL
    `).get(sym, chain, now - 10*60*1000);
    if (dump && dump.cnt >= 3) {
      out.push({ code: 'dump', emoji: '📉', label: '분산 덤핑', desc: `10분 내 ${dump.cnt}개 거래소 입금 $${(dump.total/1e6).toFixed(1)}M`, severity: 'critical' });
    }

    // 출금 러시: 10분 내 3개+ CEX 출금
    const rush = db.prepare(`
      SELECT COUNT(DISTINCT ex_from) AS cnt, SUM(usd) AS total
      FROM txs WHERE sym=? AND chain=? AND ts>=? AND ex_from IS NOT NULL
    `).get(sym, chain, now - 10*60*1000);
    if (rush && rush.cnt >= 3) {
      out.push({ code: 'rush', emoji: '💸', label: '출금 러시', desc: `10분 내 ${rush.cnt}개 거래소 출금 $${(rush.total/1e6).toFixed(1)}M`, severity: 'high' });
    }

    // 조직적 이동: 10분 내 5개+ 다른 지갑 (같은 방향)
    const coord = db.prepare(`
      SELECT COUNT(DISTINCT addr_from) AS uniq FROM txs
      WHERE sym=? AND chain=? AND ts>=?
    `).get(sym, chain, now - 10*60*1000);
    if (coord && coord.uniq >= 5) {
      out.push({ code: 'coord', emoji: '🎪', label: '조직적 이동', desc: `10분 내 ${coord.uniq}개 지갑 동시 이동`, severity: 'high' });
    }

    // 볼륨 폭증: 1h 거래수 > 7d 평균 × 5
    const h1 = db.prepare(`SELECT COUNT(*) AS c FROM txs WHERE sym=? AND chain=? AND ts>=?`).get(sym, chain, now - 3600000);
    const d7 = db.prepare(`SELECT COUNT(*) AS c FROM txs WHERE sym=? AND chain=? AND ts>=?`).get(sym, chain, now - 7*86400000);
    const d7avgH = d7.c / (7 * 24);
    if (h1.c >= 5 && d7avgH > 0 && h1.c >= d7avgH * 5) {
      out.push({ code: 'vol_spike', emoji: '🌊', label: '볼륨 급증', desc: `1h ${h1.c}건 (7d 평균 ${d7avgH.toFixed(1)}건/h 대비 ${(h1.c/d7avgH).toFixed(1)}x)`, severity: 'med' });
    }

    // 순환 이체: 1h 내 A→B, B→A 존재
    const circ = db.prepare(`
      SELECT COUNT(*) AS c FROM txs
      WHERE chain=? AND ts>=? AND addr_from=? AND addr_to=?
    `).get(chain, now - 3600000, to, from);
    if (circ && circ.c > 0) {
      out.push({ code: 'circular', emoji: '🔄', label: '순환 이체', desc: `1h 내 역방향 이체 감지`, severity: 'critical' });
    }

    // 🔀 수령 후 처분 — 이 지갑이 최근에 같은 토큰을 받았고 지금 송금 중
    // 조건: 송신자가 거래소가 아니고(보통 지갑), 24h 내 같은 토큰 수령 이력 있음
    if (!fromEx && txClass && txClass.type !== 'cex_in') {
      const prior = db.prepare(`
        SELECT MIN(ts) AS first_recv, MAX(ts) AS last_recv, COUNT(*) AS cnt, SUM(usd) AS total
        FROM txs
        WHERE addr_to = ? AND sym = ? AND chain = ?
          AND ts >= ? AND ts < ?
      `).get(from.toLowerCase(), sym, chain, now - 24*3600*1000, now - 10*1000); // 10초 버퍼(자기 자신 제외)
      if (prior && prior.cnt > 0) {
        const hoursAgo = Math.max(1, Math.floor((now - prior.last_recv) / 3600000));
        const dest = toEx ? `거래소(${toEx})` : 'DEX/지갑';
        const severity = toEx ? 'critical' : 'high'; // 거래소 입금이면 더 위험
        out.push({
          code: 'swap_after',
          emoji: '🔀',
          label: '수령 후 처분',
          desc: `${hoursAgo}h 전 ${sym} 수령 (${prior.cnt}회, $${(prior.total/1e6).toFixed(2)}M) → ${dest}`,
          severity,
        });
      }
    }
  } catch (e) { /* DB query fail silently */ }

  return out;
}

// ── ENS reverse lookup (ETH mainnet only, SQLite 캐시) ──
db.exec(`
  CREATE TABLE IF NOT EXISTS ens_cache (
    addr TEXT PRIMARY KEY,
    name TEXT,
    updated_at INTEGER NOT NULL
  )
`);
const stmtEnsGet = db.prepare('SELECT name, updated_at FROM ens_cache WHERE addr = ?');
const stmtEnsSet = db.prepare('INSERT OR REPLACE INTO ens_cache (addr, name, updated_at) VALUES (?, ?, ?)');

function namehash(name) {
  let node = new Uint8Array(32);
  if (name) {
    const labels = name.split('.');
    for (let i = labels.length - 1; i >= 0; i--) {
      const labelHash = Buffer.from(keccak256(labels[i]), 'hex');
      const combined = Buffer.concat([node, labelHash]);
      node = Buffer.from(keccak256(combined), 'hex');
    }
  }
  return '0x' + Buffer.from(node).toString('hex');
}

async function resolveENS(addr) {
  const lo = addr.toLowerCase();

  // 1. SQLite 캐시 (30일 TTL)
  try {
    const row = stmtEnsGet.get(lo);
    if (row && Date.now() - row.updated_at < 30 * 86400 * 1000) {
      return row.name || null;
    }
  } catch (e) {}

  // 2. ENS reverse lookup via ETH mainnet RPC (PublicNode 무료)
  try {
    const reverseNode = namehash(lo.slice(2) + '.addr.reverse');
    // ENS Registry: resolver(bytes32 node)
    const resolverHex = await ethRpc('eth_call', [{
      to: '0x00000000000C2E074eC69A0dFb2997BA6C7d2e1e',
      data: '0x0178b8bf' + reverseNode.slice(2),
    }, 'latest']);

    if (!resolverHex || resolverHex === '0x' || resolverHex === '0x' + '0'.repeat(64)) {
      try { stmtEnsSet.run(lo, null, Date.now()); } catch (e) {}
      return null;
    }

    const resolverAddr = '0x' + resolverHex.slice(-40);
    // Resolver: name(bytes32 node)
    const nameHex = await ethRpc('eth_call', [{
      to: resolverAddr,
      data: '0x691f3431' + reverseNode.slice(2),
    }, 'latest']);

    const ensName = abiDecodeString(nameHex);
    try { stmtEnsSet.run(lo, ensName || null, Date.now()); } catch (e) {}
    return ensName || null;
  } catch (e) {
    return null;
  }
}

// ETH mainnet RPC (PublicNode → Alchemy 폴백)
async function ethRpc(method, params) {
  const body = JSON.stringify({ jsonrpc: '2.0', method, params, id: 1 });
  const opts = { method: 'POST', headers: { 'Content-Type': 'application/json' }, body, signal: AbortSignal.timeout(6000) };
  try {
    const res = await fetch('https://ethereum-rpc.publicnode.com', opts);
    const d = await res.json();
    if (!d.error) return d.result;
  } catch (e) {}
  const res = await fetch(ALCHEMY_HTTP.eth, { ...opts, signal: AbortSignal.timeout(6000) });
  const d = await res.json();
  return d.result;
}

// ── Arkham Intelligence 라벨 (서버 사이드 + SQLite 공유 캐시) ──
db.exec(`
  CREATE TABLE IF NOT EXISTS arkham_cache (
    addr TEXT PRIMARY KEY,
    name TEXT,
    type TEXT,
    service TEXT,
    note TEXT,
    chain TEXT,
    updated_at INTEGER NOT NULL
  )
`);
const stmtArkhamGet = db.prepare('SELECT name, type, service, note, chain, updated_at FROM arkham_cache WHERE addr = ?');
const stmtArkhamSet = db.prepare(`
  INSERT OR REPLACE INTO arkham_cache (addr, name, type, service, note, chain, updated_at)
  VALUES (?, ?, ?, ?, ?, ?, ?)
`);

async function resolveArkham(addr) {
  if (!ARKHAM_KEY) return null;
  const lo = (addr || '').toLowerCase();
  if (!lo || lo.length < 20) return null;

  // 1. SQLite 캐시 (30일 TTL — null 결과도 캐시)
  try {
    const row = stmtArkhamGet.get(lo);
    if (row && Date.now() - row.updated_at < 30 * 86400000) {
      return row.name ? { name: row.name, type: row.type, service: row.service, note: row.note, chain: row.chain } : null;
    }
  } catch (e) {}

  // 2. Arkham API 호출
  try {
    const res = await fetch(`https://api.arkm.com/intelligence/address/${lo}`, {
      headers: { 'API-Key': ARKHAM_KEY },
      signal: AbortSignal.timeout(8000),
    });
    if (!res.ok) {
      // rate limit / error → null 결과 저장 (짧은 TTL 효과는 없지만 중복 호출 방지)
      try { stmtArkhamSet.run(lo, null, null, null, null, null, Date.now()); } catch (e) {}
      return null;
    }
    const d = await res.json();
    // 응답 구조: {ethereum: {arkhamEntity: {...}}, bsc: {...}, ...}
    let entity = null;
    for (const chain in d) {
      const obj = d[chain];
      if (obj && obj.arkhamEntity && obj.arkhamEntity.name) {
        entity = {
          name: obj.arkhamEntity.name,
          type: obj.arkhamEntity.type || '',
          service: obj.arkhamEntity.service || '',
          note: obj.arkhamEntity.note || '',
          chain,
        };
        break;
      }
    }
    try {
      stmtArkhamSet.run(
        lo,
        entity?.name || null,
        entity?.type || null,
        entity?.service || null,
        entity?.note || null,
        entity?.chain || null,
        Date.now()
      );
    } catch (e) {}
    return entity;
  } catch (e) {
    return null;
  }
}

// ── LP 감지 DB (SQLite 영구 캐시) ──
db.exec(`
  CREATE TABLE IF NOT EXISTS addr_type (
    addr TEXT PRIMARY KEY,
    is_lp INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
  )
`);
const stmtAddrGet = db.prepare('SELECT is_lp FROM addr_type WHERE addr = ?');
const stmtAddrSet = db.prepare('INSERT OR REPLACE INTO addr_type (addr, is_lp, updated_at) VALUES (?, ?, ?)');

async function isLPPair(chain, addr) {
  const lo = addr.toLowerCase();
  // 1. 메모리
  if (state.lpAddresses.has(lo)) return true;
  if (state.notLpCache.has(lo)) return false;
  if (LP_MANAGERS.has(lo)) { state.lpAddresses.add(lo); return true; }

  // 2. SQLite
  try {
    const row = stmtAddrGet.get(lo);
    if (row) {
      if (row.is_lp) { state.lpAddresses.add(lo); return true; }
      else { state.notLpCache.add(lo); return false; }
    }
  } catch (e) {}

  // 3. RPC (첫 조회만 — CU 소비)
  try {
    const code = await rpcCall(chain, 'eth_getCode', [addr, 'latest']);
    if (!code || code === '0x' || code.length < 4) {
      state.notLpCache.add(lo);
      try { stmtAddrSet.run(lo, 0, Date.now()); } catch (e) {}
      return false;
    }
    const t0 = await rpcCall(chain, 'eth_call', [{ to: addr, data: '0x0dfe1681' }, 'latest']);
    if (t0 && t0 !== '0x' && t0.length >= 66) {
      state.lpAddresses.add(lo);
      try { stmtAddrSet.run(lo, 1, Date.now()); } catch (e) {}
      return true;
    }
    state.notLpCache.add(lo);
    try { stmtAddrSet.run(lo, 0, Date.now()); } catch (e) {}
    return false;
  } catch (e) {
    state.notLpCache.add(lo);
    return false;
  }
}

state.notLpCache = state.notLpCache || new Set();

// ── 메타 DB (SQLite 영구 캐시 — Alchemy CU 절약) ──
db.exec(`
  CREATE TABLE IF NOT EXISTS token_meta (
    chain_ca TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    decimals INTEGER NOT NULL DEFAULT 18,
    total_supply REAL DEFAULT 0,
    updated_at INTEGER NOT NULL
  )
`);
const stmtMetaGet = db.prepare('SELECT * FROM token_meta WHERE chain_ca = ?');
const stmtMetaSet = db.prepare(`
  INSERT OR REPLACE INTO token_meta (chain_ca, symbol, decimals, total_supply, updated_at)
  VALUES (@chain_ca, @symbol, @decimals, @total_supply, @updated_at)
`);

async function getTokenMeta(chain, ca) {
  const key = `${chain}:${ca.toLowerCase()}`;

  // 1. 메모리 캐시
  if (state.metaCache.has(key)) return state.metaCache.get(key);

  // 2. SQLite 영구 캐시 (7일 TTL)
  try {
    const row = stmtMetaGet.get(key);
    if (row && Date.now() - row.updated_at < 7 * 86400 * 1000) {
      const meta = { symbol: row.symbol, decimals: row.decimals, totalSupply: row.total_supply };
      state.metaCache.set(key, meta);
      return meta;
    }
  } catch (e) {}

  // 3. RPC 호출 (Alchemy CU 소비 — 첫 조회만)
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

    // 메모리 + SQLite 저장
    state.metaCache.set(key, meta);
    try {
      stmtMetaSet.run({ chain_ca: key, symbol, decimals, total_supply: totalSupply, updated_at: Date.now() });
    } catch (e) {}

    return meta;
  } catch (e) {
    return { symbol: '?', decimals: 18, totalSupply: 0 };
  }
}

// ── Price — 캐스케이드 조회 + 교차 검증 ──
// 순서: Binance > CoinGecko > CMC > CoinGlass > DexScreener
// 첫 성공 시 즉시 반환 (API 절약), 실패 시 다음으로
// 교차 검증: 첫 결과와 DexScreener 비교 (5x 이상 차이면 낮은 쪽)
const PLATFORM_MAP = {eth:'ethereum',bsc:'binance-smart-chain',arb:'arbitrum-one',base:'base',poly:'polygon-pos',sol:'solana'};

async function getPrice(symbol, ca, chain) {
  const caLo = (ca || '').toLowerCase();
  const sym = (symbol || '').toUpperCase();

  // CA 단위 캐시 (5분)
  if (caLo) {
    const cached = state.priceCache.get(caLo);
    if (cached && Date.now() - cached.ts < 5 * 60 * 1000) return cached.price;
  }

  let price = 0;
  let source = '';

  // 1. Binance (가장 정확, 상장 토큰)
  if (!price && sym) {
    try {
      const r = await fetch(`https://api.binance.com/api/v3/ticker/price?symbol=${sym}USDT`,
        { signal: AbortSignal.timeout(4000) });
      if (r.ok) {
        const d = await r.json();
        const p = parseFloat(d.price);
        if (p > 0) { price = p; source = 'binance'; }
      }
    } catch (e) {}
  }

  // 2. CoinGecko (CA 기반, 신뢰도 높음)
  if (!price && caLo) {
    try {
      const platform = PLATFORM_MAP[chain];
      if (platform) {
        const r = await fetch(`https://api.coingecko.com/api/v3/simple/token_price/${platform}?contract_addresses=${caLo}&vs_currencies=usd`,
          { signal: AbortSignal.timeout(4000) });
        if (r.ok) {
          const d = await r.json();
          const p = d[caLo]?.usd;
          if (p > 0) { price = p; source = 'coingecko'; }
        }
      }
    } catch (e) {}
  }

  // 3. CoinMarketCap (심볼 기반)
  if (!price && sym && CMC_KEY) {
    try {
      const r = await fetch(`https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest?symbol=${sym}&convert=USD`,
        { headers: { 'X-CMC_PRO_API_KEY': CMC_KEY }, signal: AbortSignal.timeout(4000) });
      if (r.ok) {
        const d = await r.json();
        const p = d.data?.[sym]?.quote?.USD?.price;
        if (p > 0) { price = p; source = 'cmc'; }
      }
    } catch (e) {}
  }

  // 4. CoinGlass (선물 가격)
  if (!price && sym && CG_API_KEY) {
    try {
      const r = await fetch(`https://open-api-v4.coinglass.com/api/futures/coins-markets?symbol=${sym}`,
        { headers: { 'CG-API-KEY': CG_API_KEY }, signal: AbortSignal.timeout(4000) });
      if (r.ok) {
        const d = await r.json();
        if (d.code === '0' && d.data?.length) {
          const p = parseFloat(d.data[0].price);
          if (p > 0) { price = p; source = 'coinglass'; }
        }
      }
    } catch (e) {}
  }

  // 5. DexScreener (CA 기반, 클러스터 중앙값 — 마지막 폴백)
  let dexPrice = 0;
  if (caLo) {
    try {
      const r = await fetch(`https://api.dexscreener.com/latest/dex/tokens/${caLo}`,
        { signal: AbortSignal.timeout(4000) });
      if (r.ok) {
        const d = await r.json();
        const valid = (d.pairs || [])
          .filter(p => p.priceUsd && parseFloat(p.priceUsd) > 0 && (p.liquidity?.usd || 0) >= 1000);
        if (valid.length) {
          const prices = valid.map(p => parseFloat(p.priceUsd)).sort((a, b) => a - b);
          const cluster = prices.filter(p => p <= prices[0] * 10);
          const mid = Math.floor(cluster.length / 2);
          dexPrice = cluster.length % 2 === 0 ? (cluster[mid-1]+cluster[mid])/2 : cluster[mid];
        }
      }
    } catch (e) {}

    if (!price && dexPrice > 0) {
      price = dexPrice;
      source = 'dexscreener';
    }
  }

  // 교차 검증: 상위 소스 가격 vs DexScreener 가격
  // 5x 이상 차이 → 낮은 쪽 채택 (동명 토큰/가격 뒤집힘 방어)
  if (price > 0 && dexPrice > 0 && source !== 'dexscreener') {
    const ratio = price / dexPrice;
    if (ratio > 5 || ratio < 0.2) {
      price = Math.min(price, dexPrice);
      source += '+dex_cross';
    }
  }

  if (price > 0 && caLo) {
    state.priceCache.set(caLo, { price, ts: Date.now() });
  }
  return price;
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
  const newCAList = { eth: [], bsc: [], arb: [], base: [], poly: [], sol: [] };
  const seen = new Set();

  topCoins.forEach(c => {
    const sym = (c.symbol || '').toUpperCase();
    if (!BNB.has(sym)) return;
    if (EXCLUDE.has(sym)) return;
    const platforms = idToPlatforms[c.id];
    if (!platforms) return;
    // EVM chains
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
    // Solana
    const solMint = platforms['solana'];
    if (solMint && solMint.length >= 32 && !solMint.startsWith('0x')) {
      const key = 'sol:' + solMint;
      if (!seen.has(key)) {
        newCAList.sol.push(solMint);
        seen.add(key);
      }
    }
  });

  state.caList = newCAList;
  state.caListTs = Date.now();
  const total = Object.values(newCAList).reduce((s, a) => s + a.length, 0);
  log(`CA list 빌드 완료: ${total}개 (eth:${newCAList.eth.length} bsc:${newCAList.bsc.length} arb:${newCAList.arb.length} base:${newCAList.base.length} poly:${newCAList.poly.length} sol:${newCAList.sol.length})`);

  // SQLite에 CA 리스트 캐시 (CoinGecko 실패 대비 폴백)
  if (total > 0) {
    try {
      db.exec(`CREATE TABLE IF NOT EXISTS ca_cache (id INTEGER PRIMARY KEY CHECK(id=1), data TEXT, updated_at INTEGER)`);
      db.prepare('INSERT OR REPLACE INTO ca_cache (id, data, updated_at) VALUES (1, ?, ?)').run(JSON.stringify(newCAList), Date.now());
      log(`CA list DB 캐시 저장 완료 (${total}개)`);
    } catch (e) { log('CA cache save err:', e.message); }
  }
  return newCAList;
}

// CA 리스트 DB 폴백 로드
function loadCACacheFromDB() {
  try {
    db.exec(`CREATE TABLE IF NOT EXISTS ca_cache (id INTEGER PRIMARY KEY CHECK(id=1), data TEXT, updated_at INTEGER)`);
    const row = db.prepare('SELECT data, updated_at FROM ca_cache WHERE id = 1').get();
    if (row && row.data) {
      const cached = JSON.parse(row.data);
      const total = Object.values(cached).reduce((s, a) => s + a.length, 0);
      const ageH = ((Date.now() - row.updated_at) / 3600000).toFixed(1);
      log(`CA list DB 폴백 로드: ${total}개 (${ageH}시간 전 저장)`);
      return cached;
    }
  } catch (e) { log('CA cache load err:', e.message); }
  return null;
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
  // 신호 포맷: 이모지 + (한글설명)
  let signalStr = '';
  if (r.signals && r.signals.length) {
    const sevOrder = { critical: 4, high: 3, med: 2, low: 1 };
    const sorted = r.signals.slice().sort((a, b) => (sevOrder[b.severity] || 0) - (sevOrder[a.severity] || 0));
    signalStr = `\n🚨 <b>이상신호</b>\n` +
      sorted.map(s => `  ${s.emoji} ${s.label}${s.desc ? ' — ' + s.desc : ''}`).join('\n') + '\n';
  }
  return `${tier} <b>${r.sym}</b> 전송 감지\n\n` +
    `💰 가치: <b>$${fN(r.usd)}</b>\n` +
    `📊 가격: $${r.price < 1 ? r.price.toFixed(6) : r.price.toFixed(4)}\n` +
    `📦 수량: ${fN(r.amt)} ${r.sym}${supplyStr}\n` +
    `🔗 네트워크: ${CHAINS[r.chain].name}\n` +
    `🕐 ${new Date(r.ts).toLocaleString('ko-KR', { timeZone: 'Asia/Seoul' })}\n\n` +
    `🏦 From: ${r.fromArkham ? '<b>' + r.fromArkham + '</b> ' : (r.fromENS ? '<b>' + r.fromENS + '</b> ' : '')}<code>${r.from}</code>${r.fromEx ? ' (' + r.fromEx + ')' : ''} [${r.fromAge}]\n` +
    `👤 To: ${r.toArkham ? '<b>' + r.toArkham + '</b> ' : (r.toENS ? '<b>' + r.toENS + '</b> ' : '')}<code>${r.to}</code>${r.toEx ? ' (' + r.toEx + ')' : ''} [${r.toAge}]\n` +
    (r.toFlags?.length ? `⚠️ <b>${r.toFlags.join(' ')}</b>\n` : '') +
    signalStr +
    `\n<a href="${CHAINS[r.chain].exp}/tx/${r.hash}">TX 보기</a> · ` +
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
    const price = await getPrice(meta.symbol, ca, chain);
    if (!price) return;
    const usd = amt * price;
    if (usd < MIN_USD) return;

    // sanity: 비정상적으로 큰 USD (>$1B) → 가격 오류 의심, drop + 로깅
    if (usd > 1e9) {
      console.log(`[SANITY] dropped ${meta.symbol} ${chain} amt=${amt} price=${price} usd=${usd} ca=${ca}`);
      return;
    }

    // ── 동적 LP pair 감지 (100K+ 통과한 후 검사 — 비용 적음) ──
    // from/to가 LP pair이면 drop + 다음부터는 빠른 정적 거름
    const fromIsLP = await isLPPair(chain, from);
    if (fromIsLP) return;
    const toIsLP = await isLPPair(chain, to);
    if (toIsLP) return;

    // 발행량 %
    const supplyPct = meta.totalSupply > 0 ? (amt / meta.totalSupply * 100) : 0;

    // 트랜잭션 유형 분류
    const txClass = classifyTxType(from, to, fromEx, toEx);

    // 지갑 나이 + ENS 조회 (병렬)
    const [fromAge, toAge, fromENS, toENS, fromArkham, toArkham] = await Promise.all([
      getWalletAge(chain, from).catch(() => null),
      getWalletAge(chain, to).catch(() => null),
      resolveENS(from).catch(() => null),
      resolveENS(to).catch(() => null),
      resolveArkham(from).catch(() => null),
      resolveArkham(to).catch(() => null),
    ]);

    // ── 수신자 플래그 ──
    const toFlags = [];
    // 1) 신규지갑 (30일 이내 생성)
    if (toAge && toAge.days <= 30) toFlags.push('신규지갑');
    // 2) 단일토큰 지갑 (해당 코인만 보유)
    const singleToken = await checkSingleToken(chain, to, ca).catch(() => false);
    if (singleToken) toFlags.push('단일토큰');

    // ── 이상거래 신호 탐지 ──
    const signals = detectSignals({
      chain, sym: meta.symbol, ca, amt, usd, supplyPct,
      from, to, fromEx, toEx,
      fromAge, toAge, fromArkham, toArkham, txClass,
    });

    state.stats.detected++;
    const r = {
      chain, sym: meta.symbol, ca, amt, price, usd,
      from, to, fromEx, toEx, hash, supplyPct,
      fromAge: formatAge(fromAge), toAge: formatAge(toAge),
      fromENS: fromENS || null, toENS: toENS || null,
      fromArkham: fromArkham?.name || null,
      toArkham: toArkham?.name || null,
      toFlags: toFlags.length ? toFlags : null,
      signals: signals.length ? signals : null,
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
        from_age: r.fromAge || null, to_age: r.toAge || null,
        from_ens: r.fromENS || null, to_ens: r.toENS || null,
        to_flags: r.toFlags ? r.toFlags.join(',') : null,
        from_arkham: r.fromArkham || null, to_arkham: r.toArkham || null,
        signals: r.signals ? JSON.stringify(r.signals) : null,
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

// ════════════════════════════════════════════════════════════
// SOLANA SUPPORT
// ════════════════════════════════════════════════════════════
const SOL_WSS = 'wss://api.mainnet-beta.solana.com';
const SOL_HTTP = 'https://api.mainnet-beta.solana.com';
const SOL_EXP = 'https://solscan.io';


async function solRpc(method, params) {
  const body = JSON.stringify({ jsonrpc: '2.0', method, params, id: 1 });
  const res = await fetch(SOL_HTTP, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body,
    signal: AbortSignal.timeout(15000),
  });
  const d = await res.json();
  if (d.error) throw new Error(d.error.message);
  return d.result;
}

// Solana 토큰 메타 캐시 (SQLite)
db.exec(`
  CREATE TABLE IF NOT EXISTS sol_token_meta (
    mint TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    decimals INTEGER NOT NULL DEFAULT 9,
    updated_at INTEGER NOT NULL
  )
`);
const stmtSolMetaGet = db.prepare('SELECT * FROM sol_token_meta WHERE mint = ?');
const stmtSolMetaSet = db.prepare('INSERT OR REPLACE INTO sol_token_meta (mint, symbol, decimals, updated_at) VALUES (?, ?, ?, ?)');

async function getSolTokenMeta(mint) {
  // 1. SQLite 캐시 (7일)
  try {
    const row = stmtSolMetaGet.get(mint);
    if (row && Date.now() - row.updated_at < 7 * 86400000) {
      return { symbol: row.symbol, decimals: row.decimals };
    }
  } catch (e) {}

  // 2. Jupiter Token API
  try {
    const r = await fetch(`https://token.jup.ag/strict`, { signal: AbortSignal.timeout(10000) });
    if (r.ok) {
      const tokens = await r.json();
      // 캐시에 모두 저장
      const tx = db.transaction((list) => {
        for (const t of list) {
          try { stmtSolMetaSet.run(t.address, t.symbol, t.decimals, Date.now()); } catch (e) {}
        }
      });
      tx(tokens);
      const found = tokens.find(t => t.address === mint);
      if (found) return { symbol: found.symbol, decimals: found.decimals };
    }
  } catch (e) { log('Jupiter token list err:', e.message); }

  // 3. RPC 폴백 (decimals만)
  try {
    const info = await solRpc('getAccountInfo', [mint, { encoding: 'jsonParsed' }]);
    const parsed = info?.value?.data?.parsed?.info;
    if (parsed) {
      const meta = { symbol: '?', decimals: parsed.decimals || 9 };
      try { stmtSolMetaSet.run(mint, meta.symbol, meta.decimals, Date.now()); } catch (e) {}
      return meta;
    }
  } catch (e) {}
  return null;
}

// Jupiter 토큰 리스트 초기 로드 (부팅 시 1회)
async function preloadSolTokens() {
  try {
    const r = await fetch('https://token.jup.ag/strict', { signal: AbortSignal.timeout(15000) });
    if (r.ok) {
      const tokens = await r.json();
      const tx = db.transaction((list) => {
        for (const t of list) {
          try { stmtSolMetaSet.run(t.address, t.symbol, t.decimals, Date.now()); } catch (e) {}
        }
      });
      tx(tokens);
      log(`[SOL] Jupiter 토큰 ${tokens.length}개 캐시 완료`);
    }
  } catch (e) { log('[SOL] Jupiter preload err:', e.message); }
}

async function handleSolanaTransaction(sig) {
  const dedup = 'sol-' + sig;
  if (state.seenHashes.has(dedup)) return;
  state.seenHashes.add(dedup);

  try {
    const tx = await solRpc('getTransaction', [sig, {
      encoding: 'jsonParsed',
      maxSupportedTransactionVersion: 0,
      commitment: 'confirmed',
    }]);
    if (!tx || !tx.meta || tx.meta.err) return;

    state.stats.solParsed++;
    // 모든 instruction에서 SPL 토큰 전송 찾기
    const allIx = [
      ...(tx.transaction.message.instructions || []),
      ...(tx.meta.innerInstructions || []).flatMap(ii => ii.instructions || []),
    ];

    for (const ix of allIx) {
      if (ix.program !== 'spl-token' || !ix.parsed) continue;
      const type = ix.parsed.type;
      if (type !== 'transfer' && type !== 'transferChecked') continue;

      const info = ix.parsed.info;
      let mint = null, amt = 0, decimals = 9;

      if (type === 'transferChecked') {
        mint = info.mint;
        amt = parseFloat(info.tokenAmount?.uiAmount || 0);
        decimals = info.tokenAmount?.decimals || 9;
      } else {
        // transfer: source 계정에서 mint 추출 필요
        // preBalance/postBalance에서 mint 찾기
        const srcIdx = tx.transaction.message.accountKeys.findIndex(
          k => (k.pubkey || k) === info.source
        );
        if (srcIdx >= 0 && tx.meta.preTokenBalances) {
          const tb = tx.meta.preTokenBalances.find(b => b.accountIndex === srcIdx);
          if (tb) {
            mint = tb.mint;
            decimals = tb.uiTokenAmount?.decimals || 9;
            amt = parseFloat(info.amount) / Math.pow(10, decimals);
          }
        }
        if (!mint) {
          // postTokenBalances에서도 시도
          const dstIdx = tx.transaction.message.accountKeys.findIndex(
            k => (k.pubkey || k) === info.destination
          );
          if (dstIdx >= 0 && tx.meta.postTokenBalances) {
            const tb = tx.meta.postTokenBalances.find(b => b.accountIndex === dstIdx);
            if (tb) {
              mint = tb.mint;
              decimals = tb.uiTokenAmount?.decimals || 9;
              amt = parseFloat(info.amount) / Math.pow(10, decimals);
            }
          }
        }
      }

      if (!mint || amt <= 0) continue;

      // 토큰 메타
      const meta = await getSolTokenMeta(mint);
      if (!meta || !meta.symbol || meta.symbol === '?') continue;
      if (EXCLUDE.has(meta.symbol)) continue;

      // 가격
      const price = await getPrice(meta.symbol, mint, 'sol');
      if (!price) continue;
      const usd = amt * price;
      if (usd < MIN_USD) continue;
      if (usd > 1e9) continue; // sanity

      // from/to: authority = 실제 지갑 주소
      const from = info.authority || info.source || '';
      const to = info.destination || '';
      const fromEx = null;
      const toEx = null;
      if (fromEx && toEx) continue; // 거래소 내부 이체 제외

      const txClass = { type: 'p2p', label: '개인이체', tag: '' };
      if (fromEx && !toEx) { txClass.type = 'cex_out'; txClass.label = '거래소출금'; txClass.tag = fromEx; }
      else if (!fromEx && toEx) { txClass.type = 'cex_in'; txClass.label = '거래소입금'; txClass.tag = toEx; }

      state.stats.detected++;
      const r = {
        chain: 'sol', sym: meta.symbol, ca: mint, amt, price, usd,
        from, to, fromEx, toEx, hash: sig, supplyPct: 0,
        fromAge: '?d', toAge: '?d',
        fromENS: null, toENS: null,
        toFlags: null,
        tx_type: txClass.type,
        tx_label: txClass.label,
        tx_tag: txClass.tag,
        ts: (tx.blockTime || Math.floor(Date.now() / 1000)) * 1000,
      };
      log_msg(`[SOL] ${meta.symbol} $${fN(usd)} ${fromEx || from.slice(0,8)}→${toEx || to.slice(0,8)}…`);

      // DB 저장
      try {
        stmtInsert.run({
          ts: r.ts, chain: 'sol', sym: r.sym, ca: r.ca,
          amt: r.amt, price: r.price, usd: r.usd, supply_pct: 0,
          ex_from: r.fromEx || null, ex_to: r.toEx || null,
          addr_from: r.from, addr_to: r.to, hash: r.hash,
          tx_type: r.tx_type || null, tx_tag: r.tx_tag || null,
          from_age: null, to_age: null,
          from_ens: null, to_ens: null,
          to_flags: null,
        });
      } catch (e) {}

      // 메모리 큐
      state.recentTxs.unshift(r);
      if (state.recentTxs.length > 200) state.recentTxs = state.recentTxs.slice(0, 200);

      // 브로드캐스트
      const payload = JSON.stringify({ type: 'tx', data: r });
      for (const client of state.clients) {
        if (client.readyState === WebSocket.OPEN) {
          try { client.send(payload); } catch (e) {}
        }
      }

      // 텔레그램
      const tier = usd >= 1e6 ? '1M+' : usd >= 5e5 ? '500K+' : '100K+';
      await sendTelegram(
        `${tier} <b>${meta.symbol}</b> 전송 감지 (SOL)\n\n` +
        `가치: <b>$${fN(usd)}</b>\n` +
        `가격: $${price < 1 ? price.toFixed(6) : price.toFixed(4)}\n` +
        `수량: ${fN(amt)} ${meta.symbol}\n` +
        `네트워크: Solana\n\n` +
        `From: <code>${from}</code>${fromEx ? ' (' + fromEx + ')' : ''}\n` +
        `To: <code>${to}</code>${toEx ? ' (' + toEx + ')' : ''}\n\n` +
        `<a href="${SOL_EXP}/tx/${sig}">TX 보기</a> · ` +
        `<a href="${SOL_EXP}/token/${mint}">토큰</a>`
      );
    }
  } catch (e) {
    if (!e.message?.includes('not found')) log('[SOL] handleTx err:', e.message);
  }
}

// Solana Full 모드: 토큰 mint 기반 구독 (여러 WS 연결)
function connectSolanaFull() {
  const mints = state.caList.sol || [];
  if (mints.length === 0) {
    log('[SOL-full] mint 리스트 비어있음, 스킵');
    return;
  }

  // 공용 Solana RPC는 연결당 ~10개 구독 제한
  const CHUNK = 10;
  const chunks = [];
  for (let i = 0; i < mints.length; i += CHUNK) chunks.push(mints.slice(i, i + CHUNK));
  log(`[SOL-full] ${mints.length}개 mint, ${chunks.length}개 WS 연결 시작...`);

  if (!state.wsSolFull) state.wsSolFull = [];

  chunks.forEach((chunk, ci) => {
    const ws = new WebSocket(SOL_WSS);

    ws.on('open', () => {
      log(`[SOL-full #${ci}] 연결됨 — ${chunk.length}개 mint 구독`);
      chunk.forEach((mint, i) => {
        ws.send(JSON.stringify({
          jsonrpc: '2.0',
          id: 4000 + ci * 100 + i,
          method: 'logsSubscribe',
          params: [
            { mentions: [mint] },
            { commitment: 'confirmed' },
          ],
        }));
      });
    });

    ws.on('message', async (data) => {
      state.stats.solMsgs++;
      try {
        const msg = JSON.parse(data.toString());
        if (msg.id != null && msg.result != null) state.stats.solSubs++;
        if (msg.method === 'logsNotification') {
          state.stats.solNotifs++;
          const v = msg.params?.result?.value;
          if (!v || v.err) return;
          // 사전 필터: TransferChecked만 처리 (Transfer는 mint 미참조라 어차피 못 옴)
          const hasTransfer = (v.logs || []).some(l => l.includes('TransferChecked'));
          if (!hasTransfer) return;
          if (v.signature) await handleSolanaTransaction(v.signature);
        }
      } catch (e) {}
    });

    ws.on('error', (err) => log(`[SOL-full #${ci}] error:`, err.message));

    ws.on('close', () => {
      log(`[SOL-full #${ci}] 끊김 — 10초 후 재연결`);
      setTimeout(() => {
        const idx = state.wsSolFull.indexOf(ws);
        if (idx >= 0) state.wsSolFull.splice(idx, 1);
        connectSolanaFullChunk(chunk, ci);
      }, 10000);
    });

    const pingTimer = setInterval(() => {
      if (ws.readyState === WebSocket.OPEN) {
        try { ws.ping(); } catch (e) {}
      } else { clearInterval(pingTimer); }
    }, 30000);

    state.wsSolFull.push(ws);
  });
}

function connectSolanaFullChunk(chunk, ci) {
  const ws = new WebSocket(SOL_WSS);
  ws.on('open', () => {
    log(`[SOL-full #${ci}] 재연결 — ${chunk.length}개 mint`);
    chunk.forEach((mint, i) => {
      ws.send(JSON.stringify({
        jsonrpc: '2.0', id: 4000 + ci * 100 + i,
        method: 'logsSubscribe',
        params: [{ mentions: [mint] }, { commitment: 'confirmed' }],
      }));
    });
  });
  ws.on('message', async (data) => {
    state.stats.solMsgs++;
    try {
      const msg = JSON.parse(data.toString());
      if (msg.id != null && msg.result != null) state.stats.solSubs++;
      if (msg.method === 'logsNotification') {
        state.stats.solNotifs++;
        const v = msg.params?.result?.value;
        if (!v || v.err) return;
        const hasTransfer = (v.logs || []).some(l => l.includes('TransferChecked'));
        if (!hasTransfer) return;
        if (v.signature) await handleSolanaTransaction(v.signature);
      }
    } catch (e) {}
  });
  ws.on('error', (err) => log(`[SOL-full #${ci}] error:`, err.message));
  ws.on('close', () => {
    log(`[SOL-full #${ci}] 끊김 — 10초 후 재연결`);
    setTimeout(() => connectSolanaFullChunk(chunk, ci), 10000);
  });
  const pt = setInterval(() => {
    if (ws.readyState === WebSocket.OPEN) { try { ws.ping(); } catch (e) {} }
    else clearInterval(pt);
  }, 30000);
  state.wsSolFull.push(ws);
}

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


// ── HTTP server (헬스체크 + 최근 트랜잭션 + WebSocket 업그레이드) ──
function setCors(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', '*');
}

const httpServer = http.createServer(async (req, res) => {
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
      mode: 'full',
      min_usd: MIN_USD,
      detected: state.stats.detected,
      sent: state.stats.sent,
      errors: state.stats.errors,
      clients: state.clients.size,
      recent_txs: state.recentTxs.length,
      db_total: dbTotal,
      db_path: DB_PATH,
      chains: {
        full: Object.keys(state.wsFull).map(c => ({ chain: c, connected: state.wsFull[c]?.readyState === WebSocket.OPEN, ca_count: state.caList[c]?.length || 0 })),
        sol: {
          connections: (state.wsSolFull || []).filter(ws => ws.readyState === WebSocket.OPEN).length,
          mints: (state.caList.sol || []).length,
          msgs: state.stats.solMsgs,
          subs: state.stats.solSubs,
          notifs: state.stats.solNotifs,
          parsed: state.stats.solParsed,
        },
      },
      ca_total: totalCA,
      ca_per_chain: Object.fromEntries(Object.entries(state.caList).map(([k, v]) => [k, v.length])),
      cache: {
        prices: state.priceCache.size,
        meta_mem: state.metaCache.size,
        meta_db: db.prepare('SELECT COUNT(*) AS c FROM token_meta').get().c,
        lp_db: db.prepare('SELECT COUNT(*) AS c FROM addr_type').get().c,
        seen: state.seenHashes.size,
      },
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
        fromAge: row.from_age, toAge: row.to_age, fromENS: row.from_ens, toENS: row.to_ens, toFlags: row.to_flags ? row.to_flags.split(',') : null, fromArkham: row.from_arkham, toArkham: row.to_arkham, signals: row.signals ? (function(){try{return JSON.parse(row.signals);}catch(e){return null;}})() : null,
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
          ca,
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

      // 토큰별 신호 집계
      const sigStmt = db.prepare(`
        SELECT signals FROM txs WHERE sym=? AND chain=? AND ts>=? AND signals IS NOT NULL
      `);
      const sevOrd = { critical: 4, high: 3, med: 2, low: 1 };
      rows.forEach(r => {
        const sigMap = {};
        const sigRows = sigStmt.all(r.sym, r.chain, fromTs);
        sigRows.forEach(sr => {
          try {
            const sigs = JSON.parse(sr.signals);
            if (!Array.isArray(sigs)) return;
            sigs.forEach(s => {
              if (!sigMap[s.code]) sigMap[s.code] = { emoji: s.emoji, label: s.label, count: 0, severity: s.severity };
              sigMap[s.code].count++;
            });
          } catch (e) {}
        });
        r.signals = Object.values(sigMap)
          .sort((a, b) => (sevOrd[b.severity] || 0) - (sevOrd[a.severity] || 0) || b.count - a.count)
          .slice(0, 5);
      });

      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: true, days, count: rows.length, data: rows }));
    } catch (e) {
      res.writeHead(500);
      res.end(JSON.stringify({ ok: false, error: e.message }));
    }
    return;
  }

  // /analytics/watchlist → 주목 코인 Top 20 (복합 점수)
  if (req.url?.startsWith('/analytics/watchlist')) {
    try {
      const now = Date.now();

      // 7일 기준 토큰별 집계
      const rows = db.prepare(`
        SELECT
          sym, chain, ca,
          COUNT(*) AS cnt_7d,
          SUM(usd) AS total_usd_7d,
          AVG(usd) AS avg_usd,
          MAX(usd) AS max_usd,
          COUNT(DISTINCT addr_to) AS unique_receivers,
          COUNT(DISTINCT addr_from) AS unique_senders,
          MAX(ts) AS last_ts,
          MIN(ts) AS first_ts
        FROM txs
        WHERE ts >= ?
        GROUP BY sym, chain
      `).all(now - 7 * 86400000);

      // 24h 집계 (서브쿼리)
      const h24Map = {};
      const h24rows = db.prepare(`
        SELECT sym, chain, COUNT(*) AS cnt_24h, SUM(usd) AS usd_24h
        FROM txs WHERE ts >= ?
        GROUP BY sym, chain
      `).all(now - 86400000);
      h24rows.forEach(r => { h24Map[r.sym + ':' + r.chain] = r; });

      // 1h 집계 (급등 감지)
      const h1Map = {};
      const h1rows = db.prepare(`
        SELECT sym, chain, COUNT(*) AS cnt_1h
        FROM txs WHERE ts >= ?
        GROUP BY sym, chain
      `).all(now - 3600000);
      h1rows.forEach(r => { h1Map[r.sym + ':' + r.chain] = r; });

      // 신규 여부 (3일 이내 첫 등장)
      const newTokens = new Set();
      db.prepare(`
        SELECT sym, chain FROM txs
        GROUP BY sym, chain
        HAVING MIN(ts) >= ?
      `).all(now - 3 * 86400000).forEach(r => newTokens.add(r.sym + ':' + r.chain));

      // 신규지갑/단일토큰 플래그 집계 (to_flags 컬럼)
      const flagMap = {};
      try {
        db.prepare(`
          SELECT sym, chain, to_flags FROM txs
          WHERE ts >= ? AND to_flags IS NOT NULL AND to_flags != ''
        `).all(now - 7 * 86400000).forEach(r => {
          const key = r.sym + ':' + r.chain;
          if (!flagMap[key]) flagMap[key] = { newWallet: 0, singleToken: 0 };
          const flags = r.to_flags.split(',');
          if (flags.some(f => f.includes('신규'))) flagMap[key].newWallet++;
          if (flags.some(f => f.includes('단일'))) flagMap[key].singleToken++;
        });
      } catch (e) {}

      // 토큰별 이상신호 집계 (최근 24시간)
      const signalMap = {};
      try {
        db.prepare(`
          SELECT sym, chain, signals FROM txs
          WHERE ts >= ? AND signals IS NOT NULL
        `).all(now - 86400000).forEach(r => {
          try {
            const sigs = JSON.parse(r.signals);
            if (!Array.isArray(sigs) || !sigs.length) return;
            const key = r.sym + ':' + r.chain;
            if (!signalMap[key]) signalMap[key] = {};
            sigs.forEach(s => {
              if (!signalMap[key][s.code]) signalMap[key][s.code] = { emoji: s.emoji, label: s.label, count: 0, severity: s.severity };
              signalMap[key][s.code].count++;
            });
          } catch (e) {}
        });
      } catch (e) {}

      // 복합 점수 계산
      const scored = rows.map(r => {
        const key = r.sym + ':' + r.chain;
        const h24 = h24Map[key] || { cnt_24h: 0, usd_24h: 0 };
        const h1 = h1Map[key] || { cnt_1h: 0 };
        const isNew = newTokens.has(key);
        const avgDaily = r.cnt_7d / 7;

        // 점수 요소 (0~100)
        let score = 0;

        // 1. 빈도 (최대 25점) — 24h 횟수
        score += Math.min(25, h24.cnt_24h * 2.5);

        // 2. 금액 (최대 25점) — 24h 총 USD (로그 스케일)
        if (h24.usd_24h > 0) score += Math.min(25, Math.log10(h24.usd_24h / 10000) * 5);

        // 3. 급등 (최대 20점) — 1시간 횟수 vs 7일 평균
        if (avgDaily > 0 && h1.cnt_1h > avgDaily * 0.5) {
          score += Math.min(20, (h1.cnt_1h / avgDaily) * 5);
        }

        // 4. 수신자 다양성 (최대 15점) — 많은 지갑으로 분산
        score += Math.min(15, r.unique_receivers * 1.5);

        // 5. 신규 토큰 보너스 (10점)
        if (isNew) score += 10;

        // 6. 최대 단건 (최대 5점)
        if (r.max_usd >= 1e6) score += 5;
        else if (r.max_usd >= 500000) score += 3;
        else if (r.max_usd >= 100000) score += 1;

        score = Math.min(100, Math.round(score));

        const flags = flagMap[key] || { newWallet: 0, singleToken: 0 };
        // 토큰별 신호: 심각도 순 정렬
        const sevOrder = { critical: 4, high: 3, med: 2, low: 1 };
        const topSignals = Object.values(signalMap[key] || {})
          .sort((a, b) => (sevOrder[b.severity] || 0) - (sevOrder[a.severity] || 0) || b.count - a.count)
          .slice(0, 5);
        return {
          sym: r.sym, chain: r.chain, ca: r.ca, score,
          cnt_7d: r.cnt_7d, cnt_24h: h24.cnt_24h, cnt_1h: h1.cnt_1h,
          total_usd_7d: r.total_usd_7d, usd_24h: h24.usd_24h,
          avg_usd: r.avg_usd, max_usd: r.max_usd,
          unique_receivers: r.unique_receivers, unique_senders: r.unique_senders,
          is_new: isNew, last_ts: r.last_ts,
          new_wallet_cnt: flags.newWallet, single_token_cnt: flags.singleToken,
          signals: topSignals,
        };
      });

      scored.sort((a, b) => b.score - a.score);
      const top = scored.slice(0, 50);

      // FDV/MC 조회 (캐스케이드: CoinGecko > CMC > DexScreener)
      await Promise.all(top.map(async (t) => {
        const sym = t.sym;
        const caLo = (t.ca || '').toLowerCase();
        const platform = PLATFORM_MAP[t.chain];

        // 1. CoinGecko (CA 기반 — MC/FDV 정확)
        if (platform && caLo) {
          try {
            const r = await fetch(`https://api.coingecko.com/api/v3/coins/${platform}/contract/${caLo}`,
              { signal: AbortSignal.timeout(5000) });
            if (r.ok) {
              const d = await r.json();
              const md = d.market_data;
              if (md) {
                t.mc = md.market_cap?.usd || 0;
                t.fdv = md.fully_diluted_valuation?.usd || 0;
                t.price = md.current_price?.usd || 0;
                if (t.mc > 0 || t.fdv > 0) {
                  t.mcLabel = t.mc > 0 && t.mc < 10e6 ? 'MICRO' : t.mc < 50e6 ? 'SMALL' : t.mc < 200e6 ? 'MID' : '';
                  return;
                }
              }
            }
          } catch (e) {}
        }

        // 2. CMC (심볼 기반)
        if (sym && CMC_KEY) {
          try {
            const r = await fetch(`https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest?symbol=${sym}&convert=USD`,
              { headers: { 'X-CMC_PRO_API_KEY': CMC_KEY }, signal: AbortSignal.timeout(5000) });
            if (r.ok) {
              const d = await r.json();
              const info = d.data?.[sym];
              if (info) {
                t.mc = info.quote?.USD?.market_cap || 0;
                t.fdv = info.quote?.USD?.fully_diluted_market_cap || 0;
                t.price = info.quote?.USD?.price || 0;
                if (t.mc > 0 || t.fdv > 0) {
                  t.mcLabel = t.mc > 0 && t.mc < 10e6 ? 'MICRO' : t.mc < 50e6 ? 'SMALL' : t.mc < 200e6 ? 'MID' : '';
                  return;
                }
              }
            }
          } catch (e) {}
        }

        // 3. DexScreener (마지막 폴백)
        if (caLo) {
          try {
            const r = await fetch(`https://api.dexscreener.com/latest/dex/tokens/${caLo}`,
              { signal: AbortSignal.timeout(5000) });
            if (r.ok) {
              const d = await r.json();
              const valid = (d.pairs || [])
                .filter(p => p.priceUsd && parseFloat(p.priceUsd) > 0 && (p.liquidity?.usd || 0) >= 1000);
              if (valid.length) {
                valid.sort((a, b) => (b.liquidity?.usd || 0) - (a.liquidity?.usd || 0));
                const best = valid[0];
                t.fdv = best.fdv || 0;
                t.mc = best.marketCap || best.fdv || 0;
                t.liq = best.liquidity?.usd || 0;
                t.price = parseFloat(best.priceUsd) || 0;
              }
            }
          } catch (e) {}
        }

        // 라벨
        t.mcLabel = t.mc > 0 && t.mc < 10e6 ? 'MICRO' : t.mc > 0 && t.mc < 50e6 ? 'SMALL' : t.mc > 0 && t.mc < 200e6 ? 'MID' : '';
      }));

      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: true, count: top.length, data: top }));
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

  // /labels — 라벨 전체 조회
  if (req.url === '/labels' && req.method === 'GET') {
    try {
      const rows = stmtLabelGetAll.all();
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: true, count: rows.length, data: rows }));
    } catch (e) {
      res.writeHead(500);
      res.end(JSON.stringify({ ok: false, error: e.message }));
    }
    return;
  }

  // POST /labels — 라벨 생성/수정
  if (req.url === '/labels' && req.method === 'POST') {
    let body = '';
    req.on('data', chunk => { body += chunk; if (body.length > 10000) req.destroy(); });
    req.on('end', () => {
      try {
        const d = JSON.parse(body);
        const addr = (d.addr || '').toLowerCase().trim();
        const label = (d.label || '').trim().slice(0, 80);
        const color = (d.color || '').slice(0, 20) || null;
        const note = (d.note || '').slice(0, 500) || null;
        const createdBy = (d.created_by || 'anonymous').slice(0, 50);
        if (!addr || !label) {
          res.writeHead(400);
          res.end(JSON.stringify({ ok: false, error: 'addr and label required' }));
          return;
        }
        const now = Date.now();
        stmtLabelSet.run(addr, label, color, note, createdBy, now, now);
        const row = stmtLabelGet.get(addr);
        // 모든 클라이언트에게 broadcast
        const payload = JSON.stringify({ type: 'label', action: 'set', data: row });
        for (const client of state.clients) {
          if (client.readyState === WebSocket.OPEN) {
            try { client.send(payload); } catch (e) {}
          }
        }
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: true, data: row }));
      } catch (e) {
        res.writeHead(500);
        res.end(JSON.stringify({ ok: false, error: e.message }));
      }
    });
    return;
  }

  // DELETE /labels?addr=0x... — 라벨 삭제
  if (req.url?.startsWith('/labels') && req.method === 'DELETE') {
    try {
      const u = new URL(req.url, 'http://x');
      const addr = (u.searchParams.get('addr') || '').toLowerCase().trim();
      if (!addr) {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: 'addr required' }));
        return;
      }
      stmtLabelDel.run(addr);
      const payload = JSON.stringify({ type: 'label', action: 'del', addr });
      for (const client of state.clients) {
        if (client.readyState === WebSocket.OPEN) {
          try { client.send(payload); } catch (e) {}
        }
      }
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: true }));
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
      fromAge: row.from_age, toAge: row.to_age, fromENS: row.from_ens, toENS: row.to_ens, toFlags: row.to_flags ? row.to_flags.split(',') : null, fromArkham: row.from_arkham, toArkham: row.to_arkham, signals: row.signals ? (function(){try{return JSON.parse(row.signals);}catch(e){return null;}})() : null,
    }));
    const total = db.prepare('SELECT COUNT(*) AS c FROM txs').get().c;
    let labels = [];
    try { labels = stmtLabelGetAll.all(); } catch (e) {}
    clientWs.send(JSON.stringify({
      type: 'init',
      data,
      labels,
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
  log(`MIN_USD=${MIN_USD}, chains=${Object.keys(CHAINS).join(',')}`);
  await loadBinanceWhitelist();

  // CA 리스트 빌드 (CoinGecko top 1000 ∩ Binance) — 실패 시 DB 폴백 + 재시도
  await buildCAList();
  let totalCA = Object.values(state.caList).reduce((s, a) => s + a.length, 0);
  if (totalCA === 0) {
    log('CA 빌드 실패 — DB 폴백 시도');
    const cached = loadCACacheFromDB();
    if (cached) {
      state.caList = cached;
      totalCA = Object.values(cached).reduce((s, a) => s + a.length, 0);
    }
  }
  if (totalCA === 0) {
    log('CA 폴백도 없음 — 60초 후 재시도 예약');
    setTimeout(async () => {
      await buildCAList();
      const t = Object.values(state.caList).reduce((s, a) => s + a.length, 0);
      if (t > 0) {
        log(`CA 재시도 성공 (${t}개) — WebSocket 연결 시작`);
        for (const chain of Object.keys(CHAINS)) connectChainFull(chain);
      } else {
        log('CA 재시도도 실패 — 수동 확인 필요');
      }
    }, 60000);
  }
  // 매일 1회 갱신
  setInterval(() => { buildCAList().catch(e => log('CA rebuild err:', e.message)); }, 86400 * 1000);

  if (totalCA > 0) {
    for (const chain of Object.keys(CHAINS)) {
      connectChainFull(chain);
    }
  }

  // Solana는 공용 RPC 한계로 비활성화 (Helius 필요)
  // await preloadSolTokens();
  // connectSolanaFull();

  await sendTelegram(
    `🚀 <b>Whale Worker 시작</b>\n` +
    `6체인 WebSocket 연결 (EVM 5 + Solana)\n` +
    `임계값: $${fN(MIN_USD)}+\n` +
    `EVM 구독: ${totalCA - (state.caList.sol||[]).length}개 CA\n` +
    `SOL: ${(state.caList.sol||[]).length}개 mint\n` +
    `DEX/Bridge ${DEX_BLACKLIST.size}개 블랙리스트\n` +
    `Arkham: ${ARKHAM_KEY ? '✅ 활성' : '❌ 비활성 (ARKHAM_KEY 미설정)'}`
  );
})();

// Graceful shutdown
process.on('SIGTERM', () => {
  log('SIGTERM 받음 — 종료');
  Object.values(state.wsFull).forEach(ws => { try { ws.close(); } catch (e) {} });
  (state.wsSolFull || []).forEach(ws => { try { ws.close(); } catch (e) {} });
  try { db.close(); } catch (e) {}
  process.exit(0);
});
