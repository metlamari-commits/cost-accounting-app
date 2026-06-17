import { createClient } from "@/lib/supabase/server";

export type AuditAction =
  | "create"
  | "read"
  | "update"
  | "delete"
  | "export"
  | "login";

/**
 * Append an audit-log entry for a clinical-record mutation (Brief §9).
 * Append-only by RLS; failures are swallowed so they never break the request,
 * but should be surfaced to monitoring in production.
 */
export async function logAudit(params: {
  action: AuditAction;
  entityType?: string;
  entityId?: string;
  ipAddress?: string;
}) {
  const supabase = await createClient();
  const {
    data: { user },
  } = await supabase.auth.getUser();
  if (!user) return;

  await supabase.from("audit_logs").insert({
    therapist_id: user.id,
    action: params.action,
    entity_type: params.entityType ?? null,
    entity_id: params.entityId ?? null,
    ip_address: params.ipAddress ?? null,
  });
}
