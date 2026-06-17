import type { LucideIcon } from "lucide-react";

export function StatCard({
  label,
  value,
  icon: Icon,
}: {
  label: string;
  value: number | string;
  icon: LucideIcon;
}) {
  return (
    <div className="rounded-xl border border-border bg-surface p-5 shadow-sm">
      <div className="flex items-center justify-between">
        <span className="text-sm text-muted">{label}</span>
        <span className="flex size-9 items-center justify-center rounded-lg bg-accent-soft text-accent">
          <Icon className="size-[18px]" />
        </span>
      </div>
      <p className="mt-3 text-3xl font-semibold tracking-tight">{value}</p>
    </div>
  );
}
