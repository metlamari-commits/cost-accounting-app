# PsyAssist AI — Project Brief for Claude Code

> Clinical case-management web app for a single psychologist (Niki Moschovaki).
> Private, single-tenant, EU-hosted. Handles **special-category health data** under GDPR.
> UI language: **Greek**. Codebase/comments: English.

---

## 0. Read this first (non-negotiable constraints)

This app stores **real therapy patients' clinical records**. That is "special category personal data" (Art. 9 GDPR). Build accordingly from day one — security is not a "phase 2" feature here.

- **EU data residency only.** Database, file storage, and any AI/LLM endpoint must process data inside the EU.
- **No patient data leaves the system unprotected.** Before any text is sent to an external LLM, names and direct identifiers must be **pseudonymised** (replace real names with stable tokens like `CLIENT_A`).
- **Single user, but full auth.** One therapist account today, but design for login + 2FA + audit logging from the start.
- The AI is **clinical decision *support*** for a licensed professional. It never "diagnoses" autonomously. Every AI output is a suggestion the therapist explicitly accepts or rejects. UI copy must reflect this.

---

## 1. What we are building

A personal AI Clinical Assistant. The therapist should be able to *ask questions* instead of *searching notes*:

> "Τι είχε αναφέρει η Μαρία για τη σχέση με τον πατέρα της;"

Core capabilities:
1. Electronic client folder (per-patient record).
2. Per-session notes with structured fields.
3. **Dynamic history** that the AI keeps up to date (e.g. note mentions a death → AI proposes adding it to the "Losses" section).
4. Per-client AI chat that knows that client's full history.
5. A clinical knowledge base (upload psychology books/articles, indexed for retrieval).
6. A clinical consultant that combines the client's record + the knowledge base to suggest interventions.
7. Assessment instruments, progress charts, search, timeline.

---

## 2. Recommended tech stack

Aligned to a **Next.js + Supabase** stack (Supabase gives Postgres + pgvector + Auth with 2FA + Storage + Row-Level-Security in one EU-hosted service).

| Layer | Choice |
|---|---|
| Frontend | Next.js (App Router) + React + TypeScript + Tailwind |
| UI components | shadcn/ui + lucide-react |
| Charts | Recharts |
| Backend | Next.js Route Handlers / Server Actions |
| Database | Supabase Postgres (EU region) |
| Vector search | pgvector inside the same Postgres |
| Auth | Supabase Auth + MFA (TOTP 2FA) |
| File storage | Supabase Storage (EU) |
| AI / LLM | EU-resident LLM API, provider-agnostic via a single lib/ai.ts wrapper. Sign a DPA. Pseudonymise before sending. |
| Embeddings | EU-resident embeddings endpoint |
| Speech-to-text | Whisper (self-hosted or EU API) — phase 3 |
| Hosting | Vercel (EU functions) or self-host |

---

## 3. Data model (Postgres)

UUID primary keys, created_at/updated_at everywhere, Row-Level Security so rows are only visible to the owning therapist.

- **therapists** — id, email, display_name (the single user account)
- **clients** — id, therapist_id, code, full_name (encrypted), age, occupation, marital_status, referral, therapy_start_date, session_frequency, avatar_url, status
- **history_sections** — id, client_id, category ENUM(family_father, family_mother, family_siblings, relationships, work, health, trauma, losses, diagnosis, protective_factors, risk_factors), content, last_updated_by ('therapist'|'ai')
- **sessions** — id, client_id, session_number, date, duration_minutes, notes, themes[], goals[], interventions[], homework[], status ('draft'|'completed')
- **session_analyses** — id, session_id, summary, main_themes[], emotions[], cognitive_patterns[], recurring_patterns[], therapy_foci[] (AI generated, regenerable)
- **facts** — id, client_id, text, category, source_session_id, event_date, confidence, status ('active'|'archived')
- **timeline_events** — id, client_id, event_date, label, linked_session_id, category
- **assessments** — id, client_id, instrument ENUM(PHQ-9, GAD-7, PCL-5, DASS-21, CORE-OM), date, total_score, subscores (jsonb), raw_responses (jsonb)
- **library_documents** — id, therapist_id, title, category, file_type, storage_path, indexed_at
- **document_chunks** — id, document_id, chunk_text, embedding vector(N) (pgvector)
- **session_embeddings** — id, session_id, client_id, chunk_text, embedding vector(N)
- **ai_suggestions** — id, client_id, source_session_id, type ('add_fact'|'update_history'|'add_timeline_event'), payload (jsonb), proposed_text, status ('pending'|'accepted'|'rejected')
- **audit_logs** — id, therapist_id, action, entity_type, entity_id, ip_address, created_at

---

## 4. Feature modules

4.1 **Dashboard** — greeting, stat cards (Συνεδρίες σήμερα / Εκκρεμείς σημειώσεις / Νέοι πελάτες / Συνολικοί πελάτες), today's sessions list with status badges + quick actions, recent clients, right-hand AI Assistant panel with proactive items.

4.2 **Clients + Client profile** — list → profile tabs: Επισκόπηση · Ιστορικό · Συνεδρίες · Αρχεία · Αξιολογήσεις · AI Summary · Χρονογραμμή. Επισκόπηση = Βασικά Στοιχεία + AI Περίληψη + key-theme chips. Ιστορικό = editable history_sections, AI-updated ones marked.

4.3 **Session page** — tabs Σημειώσεις · Θέματα · Στόχοι · Παρεμβάσεις · Homework · AI Ανάλυση. Rich-text notes. Live AI detection panel proposing history/fact/timeline updates with [Προσθήκη]/[Όχι τώρα]. On completion → generate analysis + embeddings + extract facts.

4.4 **Per-client AI chat** — RAG over that client's sessions + facts + history. Must cite session/date. Never invents facts.

4.5 **Timeline** — vertical, grouped by year, events clickable → open linked session.

4.6 **Library** — upload PDF/DOCX/EPUB/article
