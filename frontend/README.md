# Frontend (React + Vite)

## Environment
Create `.env` from `.env.example`:

```bash
cp .env.example .env
```

Set backend URL:

```env
VITE_API_BASE_URL=http://<BACKEND_HOST>:8000
```

## Run (development)
```bash
npm install
npm run dev
```

## Production build
```bash
npm run build
npm run preview
```

## Reverse proxy note
In production, serve `dist/` via Nginx and proxy `/api/` to Django backend.
