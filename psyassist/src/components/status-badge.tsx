import { cn } from "@/lib/utils";
import { el } from "@/locales/el";

type Status = "completed" | "pending" | "scheduled" | "draft";

const styles: Record<Status, string> = {
  completed: "bg-green/10 text-green",
  pending: "bg-amber/10 text-amber",
  scheduled: "bg-blue/10 text-blue",
  draft: "bg-muted/10 text-muted",
};

export function StatusBadge({ status }: { status: Status }) {
  return (
    <span
      className={cn(
        "inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-medium",
        styles[status],
      )}
    >
      {el.status[status]}
    </span>
  );
}
