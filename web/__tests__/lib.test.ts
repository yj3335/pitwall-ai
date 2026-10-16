import { compact, compoundLetter, fmtAction, fmtGap, fmtLap, fmtSigned } from "@/lib/format";
import { emptyRun, reduceRun, verifierOutcome } from "@/lib/run";
import { linear, niceDomain, ticks } from "@/lib/scale";
import type { RunEvent } from "@/lib/types";
import scripted from "@/e2e/fixtures/run_GP08_scripted.json";
import gp10 from "@/e2e/fixtures/run_GP10.json";

describe("format", () => {
  test("gaps, laps and signed seconds", () => {
    expect(fmtGap(-1)).toBe("–"); // OpenF1 unknown
    expect(fmtGap(1534)).toBe("+1.5");
    expect(fmtGap(0, { leader: true })).toBe("Leader");
    expect(fmtLap(99_686)).toBe("1:39.686");
    expect(fmtSigned(-2.04)).toBe("−2.0");
    expect(fmtSigned(0.04)).toBe("0.0");
    expect(compact(113_469)).toBe("113.5K");
    expect(compact(1284)).toBe("1,284");
  });
  test("compound letters and actions", () => {
    expect(["SOFT", "MEDIUM", "HARD", "INTERMEDIATE", "WET", "BOGUS"].map(compoundLetter).join("")).toBe("SMHIW?");
    expect(fmtAction("PIT_NOW_INTER")).toBe("Pit now · Intermediate");
  });
});

describe("scale", () => {
  test("nice ticks and linear mapping", () => {
    expect(ticks(0, 10, 5)).toEqual([0, 2, 4, 6, 8, 10]);
    expect(niceDomain(-0.3, 4.7)).toEqual([-1, 5]);
    const x = linear([0, 10], [100, 200]);
    expect(x(5)).toBe(150);
  });
});

describe("run reducer", () => {
  test("follows a recorded cyclic run (real graph, scripted model)", () => {
    const events = scripted as unknown as RunEvent[];
    const v = events.reduce(reduceRun, emptyRun());
    expect(v.status).toBe("succeeded");
    expect(v.steps.map((s) => s.node)).toEqual([
      "telemetry_olap", "fia_regulation_rag", "monte_carlo_mcp", "strategy_synthesizer",
      "verifier", "strategy_synthesizer", "verifier", "finalize",
    ]);
    expect(v.revisions).toBe(1);
    expect(verifierOutcome(v.steps[4])).toBe("revise");
    expect(verifierOutcome(v.steps[6])).toBe("pass");
    expect(v.simulation?.table).toHaveLength(6);
    expect(v.simulation?.rollouts).toBe(10_000);
    expect(v.hits.length).toBeGreaterThan(0);
    expect(v.final?.recommended_action).toBe("PIT_NOW_SOFT");
    expect(v.assessment?.compliant).toBe(true);
  });
  test("is idempotent under replay after a reconnect", () => {
    const events = scripted as unknown as RunEvent[];
    const once = events.reduce(reduceRun, emptyRun());
    const twice = events.reduce(reduceRun, once);
    expect(twice.steps).toHaveLength(once.steps.length);
  });
  test("carries undercut and traffic from the Monte Carlo step (recorded real run)", () => {
    const v = (gp10 as unknown as RunEvent[]).reduce(reduceRun, emptyRun());
    expect(v.simulation?.undercut?.lap_deltas_ms).toHaveLength(2);
    expect(v.simulation?.undercut?.attacker_penalty_hold_ms).toBe(5000);
    expect(v.edition).toBe("2025-i5");
  });
});
