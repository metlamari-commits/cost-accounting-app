// Single source of truth for all user-facing Greek strings (Brief §7, §9).
import type { HistoryCategory, AssessmentInstrument } from "@/lib/types";

export const el = {
  app: {
    name: "PsyAssist AI",
    tagline: "Ψηφιακός βοηθός κλινικής υποστήριξης",
  },
  nav: {
    dashboard: "Πίνακας ελέγχου",
    clients: "Πελάτες",
    sessions: "Συνεδρίες",
    calendar: "Ημερολόγιο",
    assistant: "AI Assistant",
    library: "Βιβλιοθήκη",
    assessments: "Αξιολογήσεις",
    statistics: "Στατιστικά",
    settings: "Ρυθμίσεις",
  },
  dashboard: {
    greeting: "Καλώς ήρθες",
    todaySessions: "Συνεδρίες σήμερα",
    pendingNotes: "Εκκρεμείς σημειώσεις",
    newClients: "Νέοι πελάτες",
    totalClients: "Συνολικοί πελάτες",
    todaySchedule: "Σημερινό πρόγραμμα",
    recentClients: "Πρόσφατοι πελάτες",
    aiAssistant: "AI Assistant",
    noSessionsToday: "Δεν υπάρχουν συνεδρίες για σήμερα.",
  },
  clientTabs: {
    overview: "Επισκόπηση",
    history: "Ιστορικό",
    sessions: "Συνεδρίες",
    files: "Αρχεία",
    assessments: "Αξιολογήσεις",
    aiSummary: "AI Περίληψη",
    timeline: "Χρονογραμμή",
  },
  sessionTabs: {
    notes: "Σημειώσεις",
    themes: "Θέματα",
    goals: "Στόχοι",
    interventions: "Παρεμβάσεις",
    homework: "Εργασία για το σπίτι",
    aiAnalysis: "AI Ανάλυση",
  },
  status: {
    completed: "Ολοκληρωμένη",
    pending: "Εκκρεμές",
    scheduled: "Προγραμματισμένη",
    draft: "Πρόχειρο",
  },
  actions: {
    add: "Προσθήκη",
    notNow: "Όχι τώρα",
    save: "Αποθήκευση",
    cancel: "Ακύρωση",
    edit: "Επεξεργασία",
    delete: "Διαγραφή",
    export: "Εξαγωγή",
    newClient: "Νέος πελάτης",
    newSession: "Νέα συνεδρία",
    search: "Αναζήτηση",
  },
  ai: {
    disclaimer:
      "Οι προτάσεις της AI είναι υποστηρικτικές. Κάθε πρόταση εγκρίνεται ή απορρίπτεται από τον/την θεραπευτή/τρια.",
    updatedByAi: "Ενημερώθηκε από AI",
    sources: "Πηγές",
  },
} as const;

export const historyCategoryLabels: Record<HistoryCategory, string> = {
  family_father: "Οικογένεια — Πατέρας",
  family_mother: "Οικογένεια — Μητέρα",
  family_siblings: "Οικογένεια — Αδέλφια",
  relationships: "Σχέσεις",
  work: "Εργασία",
  health: "Υγεία",
  trauma: "Τραύμα",
  losses: "Απώλειες",
  diagnosis: "Διάγνωση",
  protective_factors: "Προστατευτικοί παράγοντες",
  risk_factors: "Παράγοντες κινδύνου",
};

export const assessmentLabels: Record<AssessmentInstrument, string> = {
  "PHQ-9": "PHQ-9 (Κατάθλιψη)",
  "GAD-7": "GAD-7 (Άγχος)",
  "PCL-5": "PCL-5 (PTSD)",
  "DASS-21": "DASS-21",
  "CORE-OM": "CORE-OM",
};
