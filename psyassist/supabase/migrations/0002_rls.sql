-- PsyAssist AI — Row-Level Security policies (deny-by-default)
--
-- Every table has RLS enabled. A row is visible/mutable ONLY when it belongs to
-- the authenticated therapist (auth.uid()). Tables that reference a client
-- inherit ownership through clients.therapist_id.

-- Enable RLS on every table.
alter table therapists        enable row level security;
alter table clients           enable row level security;
alter table history_sections  enable row level security;
alter table sessions          enable row level security;
alter table session_analyses  enable row level security;
alter table facts             enable row level security;
alter table timeline_events   enable row level security;
alter table assessments       enable row level security;
alter table library_documents enable row level security;
alter table document_chunks   enable row level security;
alter table session_embeddings enable row level security;
alter table ai_suggestions    enable row level security;
alter table audit_logs        enable row level security;

-- ── therapists: a therapist sees only their own row ──
create policy therapists_self on therapists
  for all using (id = auth.uid()) with check (id = auth.uid());

-- ── helper predicates via owning client ──
-- A client row is owned when clients.therapist_id = auth.uid().

-- clients
create policy clients_owner on clients
  for all using (therapist_id = auth.uid())
  with check (therapist_id = auth.uid());

-- Generic "owned through client_id" tables.
create policy history_sections_owner on history_sections
  for all using (exists (select 1 from clients c where c.id = client_id and c.therapist_id = auth.uid()))
  with check (exists (select 1 from clients c where c.id = client_id and c.therapist_id = auth.uid()));

create policy sessions_owner on sessions
  for all using (exists (select 1 from clients c where c.id = client_id and c.therapist_id = auth.uid()))
  with check (exists (select 1 from clients c where c.id = client_id and c.therapist_id = auth.uid()));

create policy facts_owner on facts
  for all using (exists (select 1 from clients c where c.id = client_id and c.therapist_id = auth.uid()))
  with check (exists (select 1 from clients c where c.id = client_id and c.therapist_id = auth.uid()));

create policy timeline_events_owner on timeline_events
  for all using (exists (select 1 from clients c where c.id = client_id and c.therapist_id = auth.uid()))
  with check (exists (select 1 from clients c where c.id = client_id and c.therapist_id = auth.uid()));

create policy assessments_owner on assessments
  for all using (exists (select 1 from clients c where c.id = client_id and c.therapist_id = auth.uid()))
  with check (exists (select 1 from clients c where c.id = client_id and c.therapist_id = auth.uid()));

create policy ai_suggestions_owner on ai_suggestions
  for all using (exists (select 1 from clients c where c.id = client_id and c.therapist_id = auth.uid()))
  with check (exists (select 1 from clients c where c.id = client_id and c.therapist_id = auth.uid()));

create policy session_embeddings_owner on session_embeddings
  for all using (exists (select 1 from clients c where c.id = client_id and c.therapist_id = auth.uid()))
  with check (exists (select 1 from clients c where c.id = client_id and c.therapist_id = auth.uid()));

-- session_analyses owned through sessions → clients
create policy session_analyses_owner on session_analyses
  for all using (exists (
    select 1 from sessions s join clients c on c.id = s.client_id
    where s.id = session_id and c.therapist_id = auth.uid()))
  with check (exists (
    select 1 from sessions s join clients c on c.id = s.client_id
    where s.id = session_id and c.therapist_id = auth.uid()));

-- library_documents owned directly by therapist
create policy library_documents_owner on library_documents
  for all using (therapist_id = auth.uid())
  with check (therapist_id = auth.uid());

-- document_chunks owned through library_documents
create policy document_chunks_owner on document_chunks
  for all using (exists (
    select 1 from library_documents d
    where d.id = document_id and d.therapist_id = auth.uid()))
  with check (exists (
    select 1 from library_documents d
    where d.id = document_id and d.therapist_id = auth.uid()));

-- audit_logs: therapist can read own logs; inserts are allowed for own id.
-- (No update/delete — audit trail is append-only.)
create policy audit_logs_select on audit_logs
  for select using (therapist_id = auth.uid());
create policy audit_logs_insert on audit_logs
  for insert with check (therapist_id = auth.uid());
