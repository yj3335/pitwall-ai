import { fireEvent, render, screen, within } from "@testing-library/react";
import { StrategyChart, domainFor } from "@/components/StrategyChart";
import { TowerTable } from "@/components/Tower";
import { TraceTimeline, FinalCallCard } from "@/components/Trace";
import { UndercutChart, undercutRows } from "@/components/Undercut";
import { emptyRun, reduceRun } from "@/lib/run";
import type { RunEvent, Tower } from "@/lib/types";
import scripted from "@/e2e/fixtures/run_GP08_scripted.json";
import gp10 from "@/e2e/fixtures/run_GP10.json";
import tower from "@/e2e/fixtures/tower.json";

const scriptedRun = (scripted as unknown as RunEvent[]).reduce(reduceRun, emptyRun());
const gp10Run = (gp10 as unknown as RunEvent[]).reduce(reduceRun, emptyRun());

describe("StrategyChart", () => {
  const sim = scriptedRun.simulation!;
  test("one row per strategy, best legal plan labelled, illegal plans marked", () => {
    render(<StrategyChart rows={sim.table} recommended={sim.recommended} rollouts={sim.rollouts} />);
    const rows = screen.getAllByTestId("strategy-row");
    expect(rows).toHaveLength(6);
    // legal plans first, sorted by expected delta; the best is labelled
    expect(rows[0].getAttribute("data-strategy")).toBe(sim.recommended);
    expect(within(rows[0]).getByText("best")).toBeInTheDocument();
    const illegal = sim.table.filter((r) => !r.legal).length;
    expect(screen.getAllByText("illegal")).toHaveLength(illegal);
  });
  test("hover and keyboard focus show a value-first tooltip; table view carries every value", () => {
    render(<StrategyChart rows={sim.table} recommended={sim.recommended} rollouts={sim.rollouts} />);
    fireEvent.focus(screen.getAllByTestId("strategy-row")[1]);
    expect(screen.getByTestId("strategy-tooltip")).toHaveTextContent(/expected/);
    fireEvent.click(screen.getByTestId("strategy-view-toggle"));
    expect(screen.getByTestId("strategy-table").querySelectorAll("tbody tr")).toHaveLength(6);
  });
  test("an off-scale plan does not flatten the axis", () => {
    const [, hi] = domainFor(sim.table);
    const worst = Math.max(...sim.table.map((r) => r.expected_delta_s));
    expect(hi).toBeLessThanOrEqual(Math.min(worst, 60) + 10);
  });
});

describe("TowerTable", () => {
  test("renders the recorded live tower with lettered tyre badges", () => {
    const t = tower as unknown as Tower;
    render(<TowerTable drivers={t.drivers} />);
    expect(screen.getAllByTestId("tower-row")).toHaveLength(t.drivers.length);
    const letters = screen.getAllByTestId("tyre-badge").map((b) => b.textContent);
    expect(letters.every((l) => ["S", "M", "H", "I", "W", "?"].includes(l ?? ""))).toBe(true);
    expect(screen.getByText("Leader")).toBeInTheDocument();
  });
});

describe("UndercutChart", () => {
  test("diverging components include the penalty hold and net gain", () => {
    const u = gp10Run.simulation!.undercut!;
    const rows = undercutRows(u);
    expect(rows.map((r) => r.label)).toContain("Penalty hold at the stop");
    expect(rows[rows.length - 1].ms).toBeCloseTo(u.expected_gain_ms);
    render(<UndercutChart undercut={u} />);
    expect(screen.getByTestId("undercut-verdict")).toHaveTextContent(u.expected_gap_after_ms < 0 ? "works" : "fails");
  });
});

describe("Trace and final call", () => {
  test("shows the self-correction cycle and the compliant final call", () => {
    render(<><TraceTimeline run={scriptedRun} /><FinalCallCard run={scriptedRun} /></>);
    expect(screen.getAllByTestId("trace-step")).toHaveLength(8);
    expect(screen.getByTestId("verifier-revise")).toBeInTheDocument();
    expect(screen.getByLabelText("revision")).toBeInTheDocument();
    expect(screen.getByTestId("final-action")).toHaveTextContent("Pit now · Soft");
    expect(screen.getByTestId("compliance")).toHaveTextContent("compliant");
    expect(screen.getByTestId("revisions")).toHaveTextContent("1");
  });
});
