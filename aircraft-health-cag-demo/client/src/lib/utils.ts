import { clsx, type ClassValue } from "clsx";

export function cn(...inputs: ClassValue[]) {
  return clsx(inputs);
}

export function formatDate(dateStr: string | null | undefined): string | null {
  if (!dateStr) return null;
  const d = new Date(dateStr);
  if (isNaN(d.getTime())) return dateStr;
  return d.toLocaleDateString("en-US", { year: "numeric", month: "short", day: "numeric" });
}

export function formatTimestamp(ts: number | null | undefined): string {
  if (!ts) return "Unknown";
  return new Date(ts).toLocaleDateString("en-US", { year: "numeric", month: "short", day: "numeric" });
}

export function urgencyColor(daysOrHours: number | null, type: "days" | "hours"): string {
  if (daysOrHours === null) return "text-zinc-400";
  const threshold = type === "days" ? 30 : 20;
  const warning = type === "days" ? 60 : 50;
  if (daysOrHours <= threshold) return "text-red-400";
  if (daysOrHours <= warning) return "text-yellow-400";
  return "text-emerald-400";
}

export function severityColor(severity: string): string {
  switch (severity.toLowerCase()) {
    case "grounding": return "text-red-400 bg-red-950/40 border-red-800/50";
    case "non-grounding": return "text-yellow-400 bg-yellow-950/40 border-yellow-800/50";
    case "cosmetic": return "text-zinc-400 bg-zinc-900/40 border-zinc-700/50";
    case "caution": return "text-orange-400 bg-orange-950/40 border-orange-800/50";
    case "warning": return "text-amber-400 bg-amber-950/40 border-amber-800/50";
    case "critical": return "text-red-400 bg-red-950/40 border-red-900/50";
    default: return "text-zinc-400 bg-zinc-900/40 border-zinc-700/50";
  }
}
