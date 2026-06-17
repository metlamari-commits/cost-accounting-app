import { CalendarClock, FileClock, UserPlus, Users } from "lucide-react";
import { StatCard } from "@/components/stat-card";
import { el } from "@/locales/el";

export default function DashboardPage() {
  // Phase 1 foundation: static placeholders. Wired to Supabase queries next.
  const stats = [
    { label: el.dashboard.todaySessions, value: 0, icon: CalendarClock },
    { label: el.dashboard.pendingNotes, value: 0, icon: FileClock },
    { label: el.dashboard.newClients, value: 0, icon: UserPlus },
    { label: el.dashboard.totalClients, value: 0, icon: Users },
  ];

  return (
    <div className="mx-auto max-w-6xl space-y-8">
      <header>
        <h1 className="text-2xl font-semibold">
          {el.dashboard.greeting}, Niki 👋
        </h1>
        <p className="mt-1 text-sm text-muted">{el.app.tagline}</p>
      </header>

      <section className="grid grid-cols-2 gap-4 lg:grid-cols-4">
        {stats.map((s) => (
          <StatCard key={s.label} {...s} />
        ))}
      </section>

      <section className="grid gap-6 lg:grid-cols-3">
        <div className="rounded-xl border border-border bg-surface p-6 lg:col-span-2">
          <h2 className="mb-4 text-base font-semibold">
            {el.dashboard.todaySchedule}
          </h2>
          <p className="text-sm text-muted">{el.dashboard.noSessionsToday}</p>
        </div>

        <div className="rounded-xl border border-border bg-surface p-6">
          <h2 className="mb-2 text-base font-semibold">
            {el.dashboard.aiAssistant}
          </h2>
          <p className="text-xs text-muted">{el.ai.disclaimer}</p>
        </div>
      </section>
    </div>
  );
}
