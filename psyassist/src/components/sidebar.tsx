"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import {
  LayoutDashboard,
  Users,
  CalendarDays,
  NotebookPen,
  Sparkles,
  BookOpen,
  ClipboardList,
  BarChart3,
  Settings,
  HeartPulse,
} from "lucide-react";
import { cn } from "@/lib/utils";
import { el } from "@/locales/el";

const items = [
  { href: "/dashboard", label: el.nav.dashboard, icon: LayoutDashboard },
  { href: "/clients", label: el.nav.clients, icon: Users },
  { href: "/sessions", label: el.nav.sessions, icon: NotebookPen },
  { href: "/calendar", label: el.nav.calendar, icon: CalendarDays },
  { href: "/assistant", label: el.nav.assistant, icon: Sparkles },
  { href: "/library", label: el.nav.library, icon: BookOpen },
  { href: "/assessments", label: el.nav.assessments, icon: ClipboardList },
  { href: "/statistics", label: el.nav.statistics, icon: BarChart3 },
  { href: "/settings", label: el.nav.settings, icon: Settings },
];

export function Sidebar() {
  const pathname = usePathname();

  return (
    <aside className="flex h-full w-64 flex-col border-r border-border bg-surface">
      <div className="flex items-center gap-2 px-5 py-5">
        <span className="flex size-9 items-center justify-center rounded-xl bg-accent text-accent-foreground">
          <HeartPulse className="size-5" />
        </span>
        <span className="text-lg font-semibold">{el.app.name}</span>
      </div>

      <nav className="flex-1 space-y-1 px-3">
        {items.map(({ href, label, icon: Icon }) => {
          const active = pathname === href || pathname.startsWith(href + "/");
          return (
            <Link
              key={href}
              href={href}
              className={cn(
                "flex items-center gap-3 rounded-xl px-3 py-2.5 text-sm font-medium transition-colors",
                active
                  ? "bg-accent text-accent-foreground"
                  : "text-muted hover:bg-accent-soft hover:text-foreground",
              )}
            >
              <Icon className="size-[18px]" />
              {label}
            </Link>
          );
        })}
      </nav>

      <div className="m-3 flex items-center gap-3 rounded-xl border border-border p-3">
        <span className="flex size-9 items-center justify-center rounded-full bg-accent-soft text-sm font-semibold text-accent">
          ΝΜ
        </span>
        <div className="min-w-0">
          <p className="truncate text-sm font-medium">Niki Moschovaki</p>
          <p className="truncate text-xs text-muted">Ψυχολόγος</p>
        </div>
      </div>
    </aside>
  );
}
