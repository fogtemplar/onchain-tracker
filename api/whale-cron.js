// ════════════════════════════════════════════════════════════
// 🐋 Whale Cron — Vercel Serverless Function
// 1분마다 거래소 hot wallet 폴링 → EXCLUDE/Binance/USD 필터
// → Telegram 푸시 + 메모리 캐시 (in-process)
// ════════════════════════════════════════════════════════════

// 봇 설정 (env 우선, fallback 하드코딩)
const BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || '7890873311:AAGMVgMBcFsWg9mcWE5Vi9ernwNPwoq0GVk';
const CHAT_ID   = process.env.TELEGRAM_CHAT_ID   || '-1003743061931';
const ETH_KEY   = process.env.ETHERSCAN_KEY      || 'MFC6RKPAYYCWID4YEH9ZM7FJWTZPY9HM4Z';
const MIN_USD   = parseInt(process.env.MIN_USD   || '100000');

// 체인별 chainId
const CHAIN_ID = { eth:1, bsc:56, arb:42161, base:8453, poly:137 };
const CHAIN_NAME = { eth:'ETH', bsc:'BSC', arb:'ARB', base:'BASE', poly:'POLY' };
const CHAIN_EXP = {
  eth:'https://etherscan.io', bsc:'https://bscscan.com',
  arb:'https://arbiscan.io', base:'https://basescan.org', poly:'https://polygonscan.com'
};

// 메이저/스테이블 EXCLUDE (브라우저 코드와 동일)
const EXCLUDE = new Set([
  'BTC','WBTC','ETH','WETH','XRP','BNB','SOL','TRX','DOGE','ADA','AVAX',
  'TON','HBAR','SUI','BCH','LTC','XLM','DOT','POL','MATIC','NEAR','ATOM',
  'APT','ALGO','VET','FIL','ICP','XDC','KAS','FLR','ETC','CRO','MNT',
  'ARB','OP','INJ','SEI','STX','TIA','THETA','IOTA','NEO','STRK','ZK','LINK',
  'USDT','USDC','DAI','BUSD','TUSD','FDUSD','PYUSD','USDE','LUSD','FRAX',
  'GUSD','USDP','USDD','CRVUSD','GHO','MIM','EURS','USDS','USD1','USDG',
  'USDF','USDTB','USTB','USDY','USYC','USD0','RLUSD','BFUSD','YLDS',
  'EURC','USX','USDAI','USDA','AUSD','REUSD','NUSD','FDIT','SATUSD',
  'BUIDL','OUSG','JTRSY','EUTBL','BCAP','HASH','FIGR_HELOC','XAUT','PAXG',
  'LEO','OKB','BGB','HTX','KCS','GT','WBT','NEXO','HT',
  'UNI','AAVE','MKR','COMP','CRV','SUSHI','LDO','SNX','CAKE','RPL','PENDLE',
  'MORPHO','SKY','ONDO','ENA','JUP','ENS','GRT','PYTH','RAY','HNT',
  'STETH','RETH','WSTETH','CBETH','SFRXETH','EZETH','WEETH','RSETH',
  'TAO','RENDER','FET','HYPE','PI','QNT','XMR','ZEC','DASH','WLD',
  'PUMP','BONK','SHIB','PEPE','TRUMP','CC','RAIN','NIGHT','STABLE',
  'MEMECORE','ASTER','FLOKI','WIF','PENGU'
]);

// 거래소 hot wallet (체인별 — 메이저 거래소 위주)
const EXCHANGES = {
  bsc: [
    {name:'Binance', addr:'0x8894e0a0c962cb723c1976a4421c95949be2d4e3'},
    {name:'Binance', addr:'0xf977814e90da44bfa03b6295a0616a897441acec'},
    {name:'OKX',     addr:'0x79f7d32fc680f6d20b12e5f3e3bd5fbf2d73e22d'},
    {name:'Bybit',   addr:'0xe2fc31f816a9b3a5d7f77ffa59e40e25e6e0d50'},
    {name:'Gate',    addr:'0x68b22215ff74e3606bd5e6c1de8c2d68180c85f7'},
    {name:'KuCoin',  addr:'0x46705dfff24256421a05d056c29e81bdc09723b8'},
    {name:'MEXC',    addr:'0x4f2d4cc2eab56e8973a8b9ade5d8e3c341e12625'},
    {name:'Bitget',  addr:'0x0639556f03714a74a5feeaf5736a4a64ff70d206'},
  ],
  eth: [
    {name:'Binance', addr:'0x28c6c06298d514db089934071355e5743bf21d60'},
    {name:'Binance', addr:'0xbe0eb53f46cd790cd13851d5eff43d12404d33e8'},
    {name:'Binance', addr:'0xf977814e90da44bfa03b6295a0616a897441acec'},
    {name:'OKX',     addr:'0x6cc5f688a315f3dc28a7781717a9a798a59fda7b'},
    {name:'Bybit',   addr:'0xf89d7b9c864f589bbf53a82105107622b35eaa40'},
    {name:'Coinbase',addr:'0x71660c4005ba85c37ccec55d0c4493e66fe775d3'},
    {name:'Kraken',  addr:'0x2910543af39aba0cd09dbb2d50200b3e800a63d2'},
    {name:'Gate',    addr:'0x0d0707963952f2fba59dd06f2b425ace40b492fe'},
    {name:'KuCoin',  addr:'0x2b5634c42055806a59e9107ed44d43c426e99d2a'},
    {name:'HTX',     addr:'0xab5c66752a9e8167967685f1450532fb96d5d24f'},
    {name:'MEXC',    addr:'0x75e89d5979e4f6fba9f97c104c2f0afb3f1dcb88'},
  ],
  arb: [
    {name:'Binance', addr:'0xb38e8c17e38363af6ebdcb3dae12e0243582891d'},
    {name:'Bybit',   addr:'0x1db92e2eebc8e0c075a02bea49a2935bcd2dfcf4'},
    {name:'OKX',     addr:'0x461249076d88d0bb5b2f7bb2cd0ffdadf18bc1e3'},
  ],
  base: [
    {name:'Binance', addr:'0x3304e22ddaa22bcdc5fca2269b418046ae7b566a'},
    {name:'Coinbase',addr:'0x3304e22ddaa22bcdc5fca2269b418046ae7b566a'},
    {name:'Bybit',   addr:'0x3d9819210a31b4961b30ef54be2aed79b9c9cd3b'},
  ],
  poly: [
    {name:'Binance', addr:'0xab5c66752a9e8167967685f1450532fb96d5d24f'},
    {name:'OKX',     addr:'0x2716b1b3dea3a8d16ef5ca5e5e617a76daa23be2'},
  ]
};

// 거래소 주소 셋 (CEX → CEX 제외용)
const EX_SET = new Set();
Object.keys(EXCHANGES).forEach(ch => EXCHANGES[ch].forEach(e => EX_SET.add(e.addr.toLowerCase())));

// 가격 캐시 (in-memory, cron warm 사이 유지)
globalThis.__priceCache = globalThis.__priceCache || {};
globalThis.__seenHashes = globalThis.__seenHashes || new Set();
globalThis.__binanceWL = globalThis.__binanceWL || null;
globalThis.__binanceWLts = globalThis.__binanceWLts || 0;
globalThis.__lastPollTs = globalThis.__lastPollTs || {};

async function fetchBinanceWhitelist(){
  if(globalThis.__binanceWL && globalThis.__binanceWL.size > 0 && Date.now()-globalThis.__binanceWLts < 24*3600*1000){
    return globalThis.__binanceWL;
  }
  const bases = new Set();
  // Vercel 미국 서버에서 binance.com이 차단될 수 있음 → CoinGecko로 폴백
  try {
    // CoinGecko top 1000 (시총)
    for(let page=1; page<=4; page++){
      const r = await fetch(`https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=250&page=${page}`);
      if(r.ok){
        const d = await r.json();
        if(Array.isArray(d)) d.forEach(c => bases.add((c.symbol||'').toUpperCase()));
      }
      await new Promise(rs => setTimeout(rs, 200));
    }
  } catch(e){ console.warn('cg fallback err:', e.message); }

  // Binance도 시도 (DC region이면 통과)
  try {
    const sr = await fetch('https://api.binance.com/api/v3/exchangeInfo');
    if(sr.ok){
      const sd = await sr.json();
      (sd.symbols||[]).filter(s => s.status==='TRADING' &&
        ['USDT','USDC','BTC','FDUSD'].includes(s.quoteAsset))
        .forEach(s => bases.add(s.baseAsset.toUpperCase()));
    }
  } catch(e){}

  globalThis.__binanceWL = bases;
  globalThis.__binanceWLts = Date.now();
  return bases;
}

async function getPrice(symbol, ca, chain){
  const key = symbol.toUpperCase();
  const cached = globalThis.__priceCache[key];
  if(cached && Date.now()-cached.ts < 5*60*1000) return cached.price;

  // 1. Binance
  try {
    const r = await fetch(`https://api.binance.com/api/v3/ticker/price?symbol=${key}USDT`);
    if(r.ok){
      const d = await r.json();
      const p = parseFloat(d.price);
      if(p > 0){
        globalThis.__priceCache[key] = {price:p, ts:Date.now()};
        return p;
      }
    }
  } catch(e){}

  // 2. DexScreener
  try {
    const r = await fetch(`https://api.dexscreener.com/latest/dex/tokens/${ca}`);
    if(r.ok){
      const d = await r.json();
      const valid = (d.pairs||[]).filter(p => p.priceUsd && parseFloat(p.priceUsd) > 0);
      if(valid.length){
        valid.sort((a,b) => (b.liquidity?.usd||0)-(a.liquidity?.usd||0));
        const p = parseFloat(valid[0].priceUsd);
        globalThis.__priceCache[key] = {price:p, ts:Date.now()};
        return p;
      }
    }
  } catch(e){}

  return 0;
}

async function ethScan(chain, params){
  const cid = CHAIN_ID[chain];
  const qs = Object.keys(params).map(k => `${k}=${encodeURIComponent(params[k])}`).join('&');
  const url = `https://api.etherscan.io/v2/api?chainid=${cid}&${qs}&apikey=${ETH_KEY}`;
  try {
    const r = await fetch(url);
    const d = await r.json();
    if(d.status === '1') return d.result;
    return null;
  } catch(e){ return null; }
}

function fN(n){
  if(n >= 1e9) return (n/1e9).toFixed(2)+'B';
  if(n >= 1e6) return (n/1e6).toFixed(2)+'M';
  if(n >= 1e3) return (n/1e3).toFixed(1)+'K';
  return n.toFixed(0);
}

function escapeMd(s){
  return String(s).replace(/[_*[\]()~`>#+\-=|{}.!]/g, '\\$&');
}

async function sendTelegram(text){
  try {
    await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({
        chat_id: CHAT_ID,
        text: text,
        parse_mode: 'HTML',
        disable_web_page_preview: true
      })
    });
  } catch(e){ console.warn('telegram err:', e.message); }
}

async function pollChain(chain, BNB){
  const exchanges = EXCHANGES[chain] || [];

  // 모든 거래소 병렬 폴링
  const txArrays = await Promise.all(exchanges.map(ex =>
    ethScan(chain, {
      module:'account', action:'tokentx',
      address: ex.addr, page:1, offset:30, sort:'desc'
    }).then(r => ({ex, txs: Array.isArray(r) ? r : []}))
  ));

  // 1차 필터: 메타데이터 기반 (가격 조회 전)
  const candidates = [];
  for(const {ex, txs} of txArrays){
    for(const tx of txs){
      const dedupeKey = `${chain}-${tx.hash}-${tx.from}-${tx.to}`;
      if(globalThis.__seenHashes.has(dedupeKey)) continue;

      const from = (tx.from||'').toLowerCase();
      const to   = (tx.to||'').toLowerCase();
      if(from !== ex.addr.toLowerCase()) continue;
      if(EX_SET.has(to)) continue;

      const sym = (tx.tokenSymbol||'').toUpperCase();
      if(!sym || EXCLUDE.has(sym)) continue;
      if(BNB && BNB.size > 0 && !BNB.has(sym)) continue;

      const dec = parseInt(tx.tokenDecimal||'18');
      let amt = 0;
      try { amt = Number(BigInt(tx.value||'0')) / Math.pow(10, dec); } catch(e){}
      if(amt <= 0) continue;

      candidates.push({chain, sym, ca: tx.contractAddress, amt,
        from, to, hash: tx.hash, ex: ex.name, dedupeKey,
        ts: parseInt(tx.timeStamp||'0')*1000});
    }
  }

  // 2차 필터: 가격 조회 (병렬, 심볼별 dedup)
  const uniqueSymbols = [...new Set(candidates.map(c => c.sym+'|'+c.ca))];
  const priceMap = {};
  await Promise.all(uniqueSymbols.map(async key => {
    const [sym, ca] = key.split('|');
    priceMap[key] = await getPrice(sym, ca, chain);
  }));

  // 3차: USD 필터 + dedup
  const results = [];
  for(const c of candidates){
    const price = priceMap[c.sym+'|'+c.ca] || 0;
    if(!price) continue;
    const usd = c.amt * price;
    if(usd < MIN_USD) continue;
    globalThis.__seenHashes.add(c.dedupeKey);
    results.push({...c, price, usd});
  }
  return results;
}

function formatMessage(r){
  const exp = CHAIN_EXP[r.chain];
  const tier = r.usd >= 1e6 ? '🔴 1M+' : r.usd >= 5e5 ? '🟠 500K+' : '🟡 100K+';
  return `${tier} <b>${r.sym}</b> · $${fN(r.usd)}\n`+
    `🔗 ${CHAIN_NAME[r.chain]}\n`+
    `📊 가격: $${r.price.toFixed(r.price < 1 ? 6 : 4)}\n`+
    `📦 수량: ${fN(r.amt)} ${r.sym}\n`+
    `🏦 ${r.ex} → 👤 <code>${r.to.slice(0,10)}…${r.to.slice(-6)}</code>\n`+
    `🕐 ${new Date(r.ts).toLocaleString('ko-KR',{timeZone:'Asia/Seoul'})}\n`+
    `<a href="${exp}/tx/${r.hash}">TX 보기</a> · <a href="${exp}/token/${r.ca}">토큰</a>`;
}

export default async function handler(req, res){
  const startTs = Date.now();

  try {
    const BNB = await fetchBinanceWhitelist();
    const allResults = [];

    // 5체인 병렬 폴링
    const chains = ['bsc','eth','arb','base','poly'];
    const chainResults = await Promise.all(chains.map(ch => pollChain(ch, BNB)));
    chainResults.forEach(rs => rs.forEach(r => allResults.push(r)));

    // USD 큰 순 정렬
    allResults.sort((a,b) => b.usd - a.usd);

    // 텔레그램 전송 (최대 10건/실행)
    const toSend = allResults.slice(0, 10);
    for(const r of toSend){
      await sendTelegram(formatMessage(r));
      await new Promise(r => setTimeout(r, 200)); // rate limit
    }

    // seenHashes 크기 제한
    if(globalThis.__seenHashes.size > 5000){
      const arr = Array.from(globalThis.__seenHashes);
      globalThis.__seenHashes = new Set(arr.slice(-3000));
    }

    res.status(200).json({
      ok: true,
      duration_ms: Date.now() - startTs,
      detected: allResults.length,
      sent: toSend.length,
      cache_size: globalThis.__seenHashes.size,
      bnb_size: BNB.size
    });
  } catch(e){
    res.status(500).json({ok:false, error: e.message});
  }
}
