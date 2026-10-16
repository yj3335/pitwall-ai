import { expect, test } from "@playwright/test";

// Drives the real stack end to end: Kafka + Go processor + Go MCP + FastAPI +
// Qdrant + local Ollama models. Needs `make run` and a replay in progress or
// finished (`make replay SESSION=9165`). Opt in with PITWALL_E2E_LIVE=1.
test.skip(!process.env.PITWALL_E2E_LIVE, "set PITWALL_E2E_LIVE=1 with the stack running");
test.setTimeout(240_000);

test("real models: run a 2023 Singapore decision and get a compliant call", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByTestId("tower-row").first()).toBeVisible({ timeout: 30_000 });
  await page.getByTestId("scenario-select").selectOption("GP02");
  await page.getByTestId("run-button").click();
  await expect(page.getByTestId("trace-step").first()).toBeVisible({ timeout: 60_000 });
  await expect(page.getByTestId("final-call")).toBeVisible({ timeout: 200_000 });
  await expect(page.getByTestId("compliance")).toContainText("compliant · 2023-i6");
  await expect(page.getByTestId("strategy-row")).toHaveCount(6);
  const citations = await page.getByTestId("citation").allTextContents();
  expect(citations.some((c) => c.includes("FIA 2023 Sporting Regulations"))).toBe(true);
  expect(citations.some((c) => c.includes("FIA 2025"))).toBe(false); // the issue in force on the race date only
});
