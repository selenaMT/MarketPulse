## MarketPulse Frontend

For product architecture and backend pipeline context, see the root `README.md`.

This frontend currently ships:
- semantic article search by keyword similarity
- source/date/result-limit filtering
- grounded chat answer panel

Theme-related UI and proxy routes are intentionally disabled while a new theme solution is being rebuilt.

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
