# PsyAssist AI

Ψηφιακός βοηθός κλινικής υποστήριξης για έναν/μία ψυχολόγο. Ιδιωτικό,
single-tenant, EU-hosted. Διαχειρίζεται **special-category health data** υπό το
GDPR. UI στα Ελληνικά· κώδικας/σχόλια στα Αγγλικά.

> Πλήρες project brief: [`docs/PsyAssist-AI-Brief.md`](docs/PsyAssist-AI-Brief.md)
> Ασφάλεια & GDPR: [`SECURITY.md`](SECURITY.md)

## Tech stack

Next.js (App Router) · TypeScript · Tailwind CSS v4 · Supabase (Postgres +
pgvector + Auth/2FA + Storage + RLS) · lucide-react.

## Setup

```bash
npm install
cp .env.example .env.local   # fill in Supabase + encryption key
npm run dev
```

Generate the app-layer encryption key:

```bash
node -e "console.log(require('crypto').randomBytes(32).toString('base64'))"
```

## Database

SQL migrations live in `supabase/migrations/`:

- `0001_init.sql` — schema (clients, sessions, history, facts, assessments,
  library, embeddings, suggestions, audit) + pgvector.
- `0002_rls.sql` — Row-Level Security, deny-by-default, scoped per therapist.

Apply with the Supabase CLI (`supabase db push`) against your EU project.

## Build status (Phase 1 — Foundation)

Done: project scaffold, full data model + RLS, app-layer encryption helper,
Supabase client wrappers, audit logging, Greek locale, app shell (sidebar,
dashboard, clients).

Next: Auth + 2FA flow, Clients/Sessions CRUD wired to Supabase, client profile
tabs, history sections, global search, GDPR export/delete. Then Phase 2 (AI).
