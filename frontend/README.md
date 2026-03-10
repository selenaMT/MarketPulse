## MarketPulse Frontend

For product architecture and backend pipeline context, see the root `README.md`.

This frontend currently ships:
- semantic article search by keyword similarity
- source/date/result-limit filtering
- grounded chat answer panel
- authentication modal and token-backed session context

Theme-related UI and proxy routes are intentionally disabled while a new theme solution is being rebuilt.

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

The UI includes sign-in and sign-out controls in the header. Users can register or log in with email/password; token is stored in `localStorage` and sent with authenticated requests. Logging out clears the token.
