# Security & GDPR — PsyAssist AI

This app stores real therapy patients' clinical records: **special-category
personal data** (GDPR Art. 9). Security is a Phase 1 requirement, not a later
add-on. See `docs/PsyAssist-AI-Brief.md` §0 and §6.

## Principles

- **EU data residency only.** Supabase (EU project), file storage, and any
  AI/LLM or embeddings endpoint must process data inside the EU. Sign a DPA
  with every processor.
- **Pseudonymisation before AI.** Real names and direct identifiers are
  replaced with stable tokens (e.g. `CLIENT_A`) before any text leaves for an
  external LLM. Re-mapping happens only in the rendered UI, never in transit.
- **Encryption.** TLS in transit. Direct identifiers (e.g. `clients.full_name`)
  are encrypted at the application layer (`src/lib/crypto.ts`, AES-256-GCM)
  before insert; the database stores ciphertext only. Plus at-rest encryption.
- **Auth.** Supabase Auth with email+password and TOTP 2FA required.
- **Row-Level Security.** Every table has RLS enabled, deny-by-default, scoped
  to the owning therapist (`supabase/migrations/0002_rls.sql`).
- **Audit logging.** Every clinical-record read/mutation is logged
  (`audit_logs`, append-only). See `src/lib/audit.ts`.
- **Patient rights.** Export a client's full record; hard-delete a client
  including embeddings and storage files.
- **AI is decision support.** Every AI output is a suggestion the therapist
  explicitly accepts or rejects — never an autonomous diagnosis.
- **Backups.** Automated, encrypted, with a tested restore.

## Key management

`CLIENT_ENCRYPTION_KEY` is a 32-byte secret (base64), server-only, never
exposed to the browser. Rotate via re-encryption migration. Never commit
real keys — see `.env.example`.

## Reporting

Report security concerns privately to the maintainer; do not open public issues
for vulnerabilities involving patient data.
