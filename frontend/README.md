## MarketPulse Frontend

For product architecture and backend pipeline context, see the root `README.md`.

This frontend currently ships:
- semantic article search by keyword similarity
- hot theme ranking panel
- theme evolution timeline panel
- related developments panel

It calls local Next.js proxy endpoints:
- `GET /api/semantic-search`
- `GET /api/themes/hot`
- `GET /api/themes/:themeRef`
- `GET /api/themes/:themeRef/timeline`
- `GET /api/themes/:themeRef/related`

The proxy forwards requests to backend FastAPI:
- `GET /articles/semantic-search`

## Backend + Frontend setup

### Option 1: Local development (requires Node.js installed)

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
### Option 2: Docker development (recommended if Node.js not installed locally)

1) Start both services with Docker Compose (from project root):

```bash
docker-compose up --build
```

This will:
- Build and run the backend container with live reload
- Build and run the frontend container with npm install and dev server
- Mount source code volumes for live development

Open `http://localhost:3000`.

### Authentication
The UI includes sign‑in and sign‑out controls in the header. Users may register a new account or log in with an existing email/password; the token is stored in `localStorage` and automatically sent with authenticated requests to the backend. Logging out clears the token.

## Current UI scope

- semantic query input
- source/date/result-limit filters
- ranked result cards with similarity score
- loading, empty, and error states
- hot themes with trend and score
- selected theme stats and timeline
- related themes and supporting developments
