# 🐋 Onchain Whale Worker

Real-time whale transfer tracker. Connects to Alchemy WebSocket on 5 chains
(BSC/ETH/ARB/BASE/POLY), filters for $100K+ exchange withdrawals of altcoins,
pushes alerts to Telegram instantly.

## Deploy on Railway

1. Railway → New Project → Deploy from GitHub repo
2. Settings → **Root Directory**: `worker`
3. Variables (Settings → Variables):
   - `ALCHEMY_KEY` — Alchemy API key
   - `BOT_TOKEN` — Telegram bot token
   - `CHAT_ID` — Telegram chat/channel ID
   - `MIN_USD` (optional, default 100000)
4. Deploy

## Local run

```bash
cd worker
npm install
ALCHEMY_KEY=xxx BOT_TOKEN=xxx CHAT_ID=xxx node index.js
```

## Health check

`GET /health` — uptime, detection stats, connection status.
