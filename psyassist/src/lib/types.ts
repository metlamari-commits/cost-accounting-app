// Domain types mirroring the Postgres schema (supabase/migrations).
// No `any` on data models (Brief §9).

export type HistoryCategory =
  | "family_father"
  | "family_mother"
  | "family_siblings"
  | "relationships"
  | "work"
  | "health"
  | "trauma"
  | "losses"
  | "diagnosis"
  | "protective_factors"
  | "risk_factors";

export type SessionStatus = "draft" | "completed";
export type FactStatus = "active" | "archived";
export type UpdatedBy = "therapist" | "ai";
export type AssessmentInstrument =
  | "PHQ-9"
  | "GAD-7"
  | "PCL-5"
  | "DASS-21"
  | "CORE-OM";
export type SuggestionType = "add_fact" | "update_history" | "add_timeline_event";
export type SuggestionStatus = "pending" | "accepted" | "rejected";

export interface Therapist {
  id: string;
  email: string;
  display_name: string | null;
  created_at: string;
  updated_at: string;
}

export interface Client {
  id: string;
  therapist_id: string;
  code: string;
  full_name: string; // decrypted for UI; ciphertext at rest
  age: number | null;
  occupation: string | null;
  marital_status: string | null;
  referral: string | null;
  therapy_start_date: string | null;
  session_frequency: string | null;
  avatar_url: string | null;
  status: string;
  created_at: string;
  updated_at: string;
}

export interface HistorySection {
  id: string;
  client_id: string;
  category: HistoryCategory;
  content: string;
  last_updated_by: UpdatedBy;
  created_at: string;
  updated_at: string;
}

export interface Session {
  id: string;
  client_id: string;
  session_number: number | null;
  date: string;
  duration_minutes: number | null;
  notes: string;
  themes: string[];
  goals: string[];
  interventions: string[];
  homework: string[];
  status: SessionStatus;
  created_at: string;
  updated_at: string;
}

export interface Assessment {
  id: string;
  client_id: string;
  instrument: AssessmentInstrument;
  date: string;
  total_score: number | null;
  subscores: Record<string, number> | null;
  raw_responses: Record<string, unknown> | null;
  created_at: string;
  updated_at: string;
}

export interface TimelineEvent {
  id: string;
  client_id: string;
  event_date: string;
  label: string;
  linked_session_id: string | null;
  category: string | null;
  created_at: string;
  updated_at: string;
}

export interface AuditLog {
  id: string;
  therapist_id: string;
  action: string;
  entity_type: string | null;
  entity_id: string | null;
  ip_address: string | null;
  created_at: string;
}
