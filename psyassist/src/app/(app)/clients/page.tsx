import { Plus, Search } from "lucide-react";
import { el } from "@/locales/el";

export default function ClientsPage() {
  return (
    <div className="mx-auto max-w-6xl space-y-6">
      <header className="flex items-center justify-between">
        <h1 className="text-2xl font-semibold">{el.nav.clients}</h1>
        <button className="inline-flex items-center gap-2 rounded-xl bg-accent px-4 py-2 text-sm font-medium text-accent-foreground hover:opacity-90">
          <Plus className="size-4" />
          {el.actions.newClient}
        </button>
      </header>

      <div className="flex items-center gap-2 rounded-xl border border-border bg-surface px-3 py-2">
        <Search className="size-4 text-muted" />
        <input
          type="search"
          placeholder={el.actions.search}
          className="w-full bg-transparent text-sm outline-none placeholder:text-muted"
        />
      </div>

      <div className="rounded-xl border border-border bg-surface p-10 text-center text-sm text-muted">
        Δεν υπάρχουν πελάτες ακόμη. Πατήστε «{el.actions.newClient}» για να
        ξεκινήσετε.
      </div>
    </div>
  );
}
