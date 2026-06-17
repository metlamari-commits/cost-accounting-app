-- PsyAssist AI — initial schema
-- Phase 1 Foundation: full clinical data model with Row-Level Security.
--
-- Security model (see docs/PsyAssist-AI-Brief.md §6):
--   * Every clinical table is scoped to the owning therapist via therapist_id
--     (directly, or transitively through client_id).
--   * RLS is deny-by-default: policies grant access ONLY to auth.uid() == therapist.
--   * clients.full_name and other direct identifiers are encrypted at the
--     application layer before insert (see src/lib/crypto.ts); the columns hold
--     ciphertext, never plaintext.
--   * pgvector powers retrieval for the (Phase 2) RAG features.

create extension if not exists "uuid-ossp";
create extension if not exists vector;

-- Embedding dimensionality. Adjust to match the chosen EU embeddings model.
-- (e.g. 1536 for many text-embedding models.)

-- ─────────────────────────────────────────────────────────────────────────
-- Enums
-- ─────────────────────────────────────────────────────────────────────────
create type history_category as enum (
  'family_father', 'family_mother', 'family_siblings', 'relationships',
  'work', 'health', 'trauma', 'losses', 'diagnosis',
  'protective_factors', 'risk_factors'
);

create type session_status as enum ('draft', 'completed');
create type fact_status as enum ('active', 'archived');
create type updated_by as enum ('therapist', 'ai');
create type assessment_instrument as enum ('PHQ-9', 'GAD-7', 'PCL-5', 'DASS-21', 'CORE-OM');
create type suggestion_type as enum ('add_fact', 'update_history', 'add_timeline_event');
create type suggestion_status as enum ('pending', 'accepted', 'rejected');

-- ─────────────────────────────────────────────────────────────────────────
-- Helper: updated_at trigger
-- ─────────────────────────────────────────────────────────────────────────
create or replace function set_updated_at()
returns trigger language plpgsql as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

-- ─────────────────────────────────────────────────────────────────────────
-- therapists (the single user account; row mirrors auth.users)
-- ─────────────────────────────────────────────────────────────────────────
create table therapists (
  id           uuid primary key references auth.users(id) on delete cascade,
  email        text not null unique,
  display_name text,
  created_at   timestamptz not null default now(),
  updated_at   timestamptz not null default now()
);

-- ─────────────────────────────────────────────────────────────────────────
-- clients
-- full_name holds APP-LAYER CIPHERTEXT, never plaintext.
-- ─────────────────────────────────────────────────────────────────────────
create table clients (
  id                 uuid primary key default uuid_generate_v4(),
  therapist_id       uuid not null references therapists(id) on delete cascade,
  code               text not null,
  full_name          text not null,            -- encrypted
  age                int,
  occupation         text,
  marital_status     text,
  referral           text,
  therapy_start_date date,
  session_frequency  text,
  avatar_url         text,
  status             text not null default 'active',
  created_at         timestamptz not null default now(),
  updated_at         timestamptz not null default now(),
  unique (therapist_id, code)
);
create index clients_therapist_idx on clients(therapist_id);

-- ─────────────────────────────────────────────────────────────────────────
-- history_sections
-- ─────────────────────────────────────────────────────────────────────────
create table history_sections (
  id              uuid primary key default uuid_generate_v4(),
  client_id       uuid not null references clients(id) on delete cascade,
  category        history_category not null,
  content         text not null default '',
  last_updated_by updated_by not null default 'therapist',
  created_at      timestamptz not null default now(),
  updated_at      timestamptz not null default now(),
  unique (client_id, category)
);
create index history_sections_client_idx on history_sections(client_id);

-- ─────────────────────────────────────────────────────────────────────────
-- sessions
-- ─────────────────────────────────────────────────────────────────────────
create table sessions (
  id               uuid primary key default uuid_generate_v4(),
  client_id        uuid not null references clients(id) on delete cascade,
  session_number   int,
  date             date not null default current_date,
  duration_minutes int,
  notes            text not null default '',
  themes           text[] not null default '{}',
  goals            text[] not null default '{}',
  interventions    text[] not null default '{}',
  homework         text[] not null default '{}',
  status           session_status not null default 'draft',
  created_at       timestamptz not null default now(),
  updated_at       timestamptz not null default now()
);
create index sessions_client_idx on sessions(client_id);

-- ─────────────────────────────────────────────────────────────────────────
-- session_analyses (AI generated, regenerable) — Phase 2
-- ─────────────────────────────────────────────────────────────────────────
create table session_analyses (
  id                 uuid primary key default uuid_generate_v4(),
  session_id         uuid not null references sessions(id) on delete cascade,
  summary            text,
  main_themes        text[] not null default '{}',
  emotions           text[] not null default '{}',
  cognitive_patterns text[] not null default '{}',
  recurring_patterns text[] not null default '{}',
  therapy_foci       text[] not null default '{}',
  created_at         timestamptz not null default now(),
  updated_at         timestamptz not null default now()
);
create index session_analyses_session_idx on session_analyses(session_id);

-- ─────────────────────────────────────────────────────────────────────────
-- facts
-- ─────────────────────────────────────────────────────────────────────────
create table facts (
  id                uuid primary key default uuid_generate_v4(),
  client_id         uuid not null references clients(id) on delete cascade,
  text              text not null,
  category          text,
  source_session_id uuid references sessions(id) on delete set null,
  event_date        date,
  confidence        real,
  status            fact_status not null default 'active',
  created_at        timestamptz not null default now(),
  updated_at        timestamptz not null default now()
);
create index facts_client_idx on facts(client_id);

-- ─────────────────────────────────────────────────────────────────────────
-- timeline_events
-- ─────────────────────────────────────────────────────────────────────────
create table timeline_events (
  id               uuid primary key default uuid_generate_v4(),
  client_id        uuid not null references clients(id) on delete cascade,
  event_date       date not null,
  label            text not null,
  linked_session_id uuid references sessions(id) on delete set null,
  category         text,
  created_at       timestamptz not null default now(),
  updated_at       timestamptz not null default now()
);
create index timeline_events_client_idx on timeline_events(client_id);

-- ─────────────────────────────────────────────────────────────────────────
-- assessments
-- ─────────────────────────────────────────────────────────────────────────
create table assessments (
  id            uuid primary key default uuid_generate_v4(),
  client_id     uuid not null references clients(id) on delete cascade,
  instrument    assessment_instrument not null,
  date          date not null default current_date,
  total_score   int,
  subscores     jsonb,
  raw_responses jsonb,
  created_at    timestamptz not null default now(),
  updated_at    timestamptz not null default now()
);
create index assessments_client_idx on assessments(client_id);

-- ─────────────────────────────────────────────────────────────────────────
-- library_documents + chunks (clinical knowledge base) — Phase 3
-- ─────────────────────────────────────────────────────────────────────────
create table library_documents (
  id           uuid primary key default uuid_generate_v4(),
  therapist_id uuid not null references therapists(id) on delete cascade,
  title        text not null,
  category     text,
  file_type    text,
  storage_path text,
  indexed_at   timestamptz,
  created_at   timestamptz not null default now(),
  updated_at   timestamptz not null default now()
);
create index library_documents_therapist_idx on library_documents(therapist_id);

create table document_chunks (
  id          uuid primary key default uuid_generate_v4(),
  document_id uuid not null references library_documents(id) on delete cascade,
  chunk_text  text not null,
  embedding   vector(1536),
  created_at  timestamptz not null default now()
);
create index document_chunks_document_idx on document_chunks(document_id);
create index document_chunks_embedding_idx on document_chunks
  using ivfflat (embedding vector_cosine_ops) with (lists = 100);

-- ─────────────────────────────────────────────────────────────────────────
-- session_embeddings (per-client RAG) — Phase 2
-- ─────────────────────────────────────────────────────────────────────────
create table session_embeddings (
  id         uuid primary key default uuid_generate_v4(),
  session_id uuid not null references sessions(id) on delete cascade,
  client_id  uuid not null references clients(id) on delete cascade,
  chunk_text text not null,
  embedding  vector(1536),
  created_at timestamptz not null default now()
);
create index session_embeddings_client_idx on session_embeddings(client_id);
create index session_embeddings_embedding_idx on session_embeddings
  using ivfflat (embedding vector_cosine_ops) with (lists = 100);

-- ─────────────────────────────────────────────────────────────────────────
-- ai_suggestions (approval flow) — Phase 2
-- ─────────────────────────────────────────────────────────────────────────
create table ai_suggestions (
  id                uuid primary key default uuid_generate_v4(),
  client_id         uuid not null references clients(id) on delete cascade,
  source_session_id uuid references sessions(id) on delete set null,
  type              suggestion_type not null,
  payload           jsonb,
  proposed_text     text,
  status            suggestion_status not null default 'pending',
  created_at        timestamptz not null default now(),
  updated_at        timestamptz not null default now()
);
create index ai_suggestions_client_idx on ai_suggestions(client_id);

-- ─────────────────────────────────────────────────────────────────────────
-- audit_logs
-- ─────────────────────────────────────────────────────────────────────────
create table audit_logs (
  id           uuid primary key default uuid_generate_v4(),
  therapist_id uuid not null references therapists(id) on delete cascade,
  action       text not null,
  entity_type  text,
  entity_id    uuid,
  ip_address   text,
  created_at   timestamptz not null default now()
);
create index audit_logs_therapist_idx on audit_logs(therapist_id);

-- ─────────────────────────────────────────────────────────────────────────
-- updated_at triggers
-- ─────────────────────────────────────────────────────────────────────────
do $$
declare t text;
begin
  foreach t in array array[
    'therapists','clients','history_sections','sessions','session_analyses',
    'facts','timeline_events','assessments','library_documents',
    'ai_suggestions'
  ] loop
    execute format(
      'create trigger %I_set_updated_at before update on %I
         for each row execute function set_updated_at();', t, t);
  end loop;
end $$;
