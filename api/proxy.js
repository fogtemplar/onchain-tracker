export default async function handler(req, res) {
  const { service, path } = req.query;

  if (!service || !path) {
    return res.status(400).json({ error: 'Missing service or path' });
  }

  const BASE = {
    etherscan:      'https://api.etherscan.io',
    coinglass:      'https://open-api-v4.coinglass.com',
    arkham:         'https://api.arkm.com',
    dexscreener:    'https://api.dexscreener.com',
    binance:        'https://fapi.binance.com',
    'binance-spot': 'https://api.binance.com',
    coingecko:      'https://api.coingecko.com',
  };

  const base = BASE[service];
  if (!base) {
    return res.status(400).json({ error: 'Unknown service: ' + service });
  }

  const url = base + path;

  // Forward relevant headers from client
  const fwdHeaders = {};
  const skip = new Set(['host', 'connection', 'content-length', 'transfer-encoding']);
  for (const [k, v] of Object.entries(req.headers)) {
    if (!skip.has(k.toLowerCase())) {
      fwdHeaders[k] = v;
    }
  }
  // Remove Vercel/browser-specific headers
  delete fwdHeaders['x-forwarded-for'];
  delete fwdHeaders['x-forwarded-proto'];
  delete fwdHeaders['x-forwarded-host'];
  delete fwdHeaders['x-vercel-id'];
  delete fwdHeaders['x-vercel-ip-country'];
  delete fwdHeaders['x-real-ip'];

  try {
    const upstream = await fetch(url, {
      method: req.method || 'GET',
      headers: fwdHeaders,
      body: req.method === 'POST' ? JSON.stringify(req.body) : undefined,
      signal: AbortSignal.timeout(25000),
    });

    const data = await upstream.text();

    // CORS headers
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', '*');
    res.setHeader('Content-Type', upstream.headers.get('content-type') || 'application/json');

    return res.status(upstream.status).send(data);
  } catch (e) {
    return res.status(502).json({ error: 'Proxy error: ' + e.message });
  }
}
