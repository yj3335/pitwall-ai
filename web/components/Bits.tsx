import { compoundLetter } from "@/lib/format";

const TYRE_VAR: Record<string, string> = {
  SOFT: "var(--tyre-soft)",
  MEDIUM: "var(--tyre-medium)",
  HARD: "var(--tyre-hard)",
  INTERMEDIATE: "var(--tyre-inter)",
  WET: "var(--tyre-wet)",
};

/** Compound as a lettered badge: the letter carries identity, the ring colour
 *  follows the Pirelli convention (never colour alone). */
export function TyreBadge({ compound, age }: { compound: string; age?: number }) {
  const ring = TYRE_VAR[compound] ?? "var(--text-muted)";
  return (
    <span className="inline-flex items-center gap-1.5" title={compound.toLowerCase()}>
      <span
        className="tyre-badge inline-flex h-5 w-5 items-center justify-center rounded-full text-[11px] font-bold"
        style={{ border: `2px solid ${ring}`, color: "var(--text-primary)" }}
        aria-label={compound.toLowerCase()}
        data-testid="tyre-badge"
      >
        {compoundLetter(compound)}
      </span>
      {age != null && <span className="tnum secondary text-xs">{age}</span>}
    </span>
  );
}

export type StatusKind = "good" | "warning" | "serious" | "critical";
const STATUS_ICON: Record<StatusKind, string> = { good: "✓", warning: "!", serious: "▲", critical: "✕" };

/** Status pill: colour + icon + label, so state never rides on hue alone. */
export function StatusPill({ kind, label, testId }: { kind: StatusKind; label: string; testId?: string }) {
  return (
    <span
      data-testid={testId}
      className="inline-flex shrink-0 items-center gap-1.5 whitespace-nowrap rounded-full px-2 py-0.5 text-xs"
      style={{ border: `1px solid var(--${kind})`, color: "var(--text-primary)" }}
    >
      <span aria-hidden style={{ color: `var(--${kind})` }} className="font-bold">
        {STATUS_ICON[kind]}
      </span>
      {label}
    </span>
  );
}

/** Stat tile: label, value (proportional figures), optional context line. */
export function StatTile({ label, value, sub, testId }: { label: string; value: string; sub?: string; testId?: string }) {
  return (
    <div className="card px-4 py-3" data-testid={testId}>
      <div className="secondary text-xs">{label}</div>
      <div className="mt-1 text-2xl font-semibold">{value}</div>
      {sub && <div className="muted mt-0.5 text-xs">{sub}</div>}
    </div>
  );
}

export function SectionTitle({ title, sub, right }: { title: string; sub?: string; right?: React.ReactNode }) {
  return (
    <div className="mb-3 flex items-end justify-between gap-3">
      <div>
        <h2 className="text-sm font-semibold">{title}</h2>
        {sub && <p className="muted text-xs">{sub}</p>}
      </div>
      {right}
    </div>
  );
}

const TRACK_STATUS: Record<string, { kind: StatusKind; label: string }> = {
  GREEN: { kind: "good", label: "Green flag" },
  YELLOW: { kind: "warning", label: "Yellow" },
  VSC: { kind: "warning", label: "Virtual safety car" },
  SC: { kind: "serious", label: "Safety car" },
  RED: { kind: "critical", label: "Red flag" },
};

export function TrackStatusPill({ status }: { status: string }) {
  const s = TRACK_STATUS[status] ?? { kind: "warning" as StatusKind, label: status };
  return <StatusPill kind={s.kind} label={s.label} testId="track-status" />;
}
