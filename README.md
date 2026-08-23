# WealthOcean Trading Platform — Deployment Guide

Two separate repos. No `.env` files anywhere — everything through platform environment variables.

```
woi-frontend  →  GitHub  →  Vercel
woi-api       →  GitHub  →  Railway (+ PostgreSQL)
```

---

## Part 1 — Deploy Backend to Railway (do this FIRST)

### 1.1 Push to GitHub

Open GitHub Desktop:
- File → Add Local Repository → select `woi-api` folder → "create a repository"
- Summary: `Initial commit` → Commit to main
- Click **Publish repository** → name it `woi-api` → Publish

### 1.2 Create Railway project

1. Go to railway.app → **New Project**
2. Click **+ Create** → **Database** → **Add PostgreSQL**
3. Click **+ Create** again → **GitHub Repo** → select `woi-api`

### 1.3 Link database to backend

In Railway, click your `woi-api` service → **Variables** tab → **+ New Variable** → **Add Reference** → select **Postgres** → `DATABASE_URL`

### 1.4 Add these variables (Variables tab → Raw Editor → paste)

```
SECRET_KEY=woi2026supersecretkeychangethislater0000000000
ENCRYPTION_KEY=4f8a2b9c7d1e6f3a5b8c2d9e7f1a4b6c8d2e9f3a5b7c1d4e6f8a2b9c7d1e6f3a
ALLOWED_ORIGINS=*
DHAN_BASE_URL=https://api.dhan.co
MASTER_EMAIL=pramod@wealthocean.in
MASTER_PASSWORD=YourStrongPassword123
MASTER_NAME=Pramod K
```

The master account is created automatically on first startup. Change `MASTER_EMAIL` / `MASTER_PASSWORD` to whatever you want.

### 1.5 Generate a public URL

Backend service → **Settings** → **Networking** → **Generate Domain**

You get something like `https://woi-api-production-a1b2.up.railway.app`

**Copy this URL — you need it for Vercel.**

### 1.6 Verify it works

Open `https://YOUR-RAILWAY-URL/docs` in your browser. You should see the interactive API documentation with all 18 endpoints.

---

## Part 2 — Deploy Frontend to Vercel

### 2.1 Push to GitHub

GitHub Desktop:
- File → Add Local Repository → select `woi-frontend` → "create a repository"
- Commit → **Publish repository** → name `woi-frontend`

### 2.2 Import to Vercel

1. vercel.com → **Add New** → **Project**
2. Import `woi-frontend`
3. Project Name: `woi-frontend`
4. Framework: Next.js (auto-detected)

### 2.3 Add environment variables BEFORE deploying

Expand **Environment Variables** → click **Import .env** → paste:

```
NEXT_PUBLIC_API_URL=https://YOUR-RAILWAY-URL-HERE
NEXTAUTH_SECRET=woi2026nextauthsecretkey32charsminimum
```

Replace `YOUR-RAILWAY-URL-HERE` with the Railway domain from step 1.5.

### 2.4 Deploy

Click **Deploy**. Takes about 60 seconds.

### 2.5 Add NEXTAUTH_URL after first deploy

After deploying you'll get a URL like `https://woi-frontend.vercel.app`

Go to Vercel → your project → **Settings** → **Environment Variables** → add:

```
NEXTAUTH_URL = https://woi-frontend.vercel.app
```

Then **Deployments** tab → three dots on latest → **Redeploy**.

### 2.6 Tighten CORS (optional but recommended)

Go back to Railway → `woi-api` → Variables → change:

```
ALLOWED_ORIGINS=https://woi-frontend.vercel.app
```

Railway auto-redeploys.

---

## Logging in

**Master:** the email and password you set as `MASTER_EMAIL` / `MASTER_PASSWORD` in Railway.

**Clients:** two ways to get an account
1. Client self-registers on the login page (Create account tab)
2. Master creates them from **Clients** page → Add client

---

## What each page does

### Master
| Page | What it does |
|---|---|
| Dashboard | Total clients, combined P&L, alerts for missing API/proxy |
| Clients | Full client table, add new clients, test any client's Dhan connection |
| All Trades | Every order across all clients |
| P&L Overview | Client-wise P&L with return % |
| Settings | Set static IP proxy on behalf of any client |

### Client
| Page | What it does |
|---|---|
| Dashboard | Own P&L summary, recent orders, open positions |
| Profile | Personal details, active index, paper trading toggle |
| API Credentials | Dhan Client ID / PIN / TOTP / Access Token + **Test connection** button + optional proxy |
| Trades | Orders (All/Pending/Executed/Rejected), Positions (All/Open/Closed) |
| Portfolio | Calendar P&L — click any date for that day's breakdown, plus fund summary |

---

## How the Dhan connection test works

1. Client enters credentials → `PUT /api/credentials/dhan`
2. All 4 fields encrypted with AES-256-GCM before hitting the database
3. Client clicks **Test** → `POST /api/credentials/dhan/test`
4. Backend decrypts the access token, calls `GET https://api.dhan.co/fundlimit`
5. Routes through proxy if the client configured one
6. Result: green "Connected" with fund balance, or red with the exact error
7. Status saved to `is_active` + `last_verified` / `last_error`

Master can test any client's connection from the Clients page.

---

## Local development (optional)

**Backend:**
```bash
cd woi-api
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
set DATABASE_URL=postgresql://... (or leave blank for SQLite)
set MASTER_EMAIL=admin@test.com
set MASTER_PASSWORD=admin123
uvicorn app.main:app --reload --port 8000
```

**Frontend:**
```bash
cd woi-frontend
npm install
set NEXT_PUBLIC_API_URL=http://localhost:8000
set NEXTAUTH_SECRET=localdevsecret32characterslongok
npm run dev
```

---

## Troubleshooting

| Problem | Fix |
|---|---|
| Vercel build fails | Check that `package.json` has no Prisma. This version doesn't. |
| "Failed to fetch" on login | `NEXT_PUBLIC_API_URL` is wrong or Railway is asleep. Open the Railway URL directly to wake it. |
| CORS error in browser console | Set `ALLOWED_ORIGINS=*` in Railway temporarily |
| Can't log in as master | Check Railway logs for `[bootstrap] Master user created`. If missing, `MASTER_EMAIL`/`MASTER_PASSWORD` aren't set. |
| Dhan test says token expired | Generate a fresh access token in the Dhan app — they expire every 24h |
