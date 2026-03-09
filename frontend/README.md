## MarketPulse Frontend

For product architecture and backend pipeline context, see the root `README.md`.

This frontend currently ships the first end-to-end product feature:
- semantic article search by keyword similarity

It calls the local Next.js proxy endpoint:
- `GET /api/semantic-search`

The proxy forwards requests to backend FastAPI:
- `GET /articles/semantic-search`

## Backend + Frontend setup

1) Start backend API (from `backend/`):

```bash
uvicorn app.main:app --reload
```

2) (Optional) Set backend base URL for the proxy in `frontend/.env.local`:

```env
MARKETPULSE_API_BASE_URL=http://127.0.0.1:8000
```

3) Start frontend app (from `frontend/`):

```bash
npm run dev
```

Open `http://localhost:3000`.

## Current UI scope

- semantic query input
- source/date/result-limit filters
- ranked result cards with similarity score
- loading, empty, and error states
- roadmap panels for upcoming macro features

## Next planned frontend modules

- theme detection dashboard
- article entity detail view
- narrative graph explorer
- macro signal timeline
