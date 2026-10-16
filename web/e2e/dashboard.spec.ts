import { expect, test, type Page, type Route } from "@playwright/test";
import fs from "node:fs";
import path from "node:path";

// The control plane is replaced by payloads recorded from the real system
// (scripts in docs/TESTING.md): the live tower/feeds of a Singapore 2023
// replay, real agent runs on real models, and one run of the real graph with
// the scripted model used by the Python tests (for the self-correction path).
const API = process.env.NEXT_PUBLIC_API_URL ?? "http://127.0.0.1:8080";
const fx = (name: string) => JSON.parse(fs.readFileSync(path.join(__dirname, "fixtures", name), "utf8"));
const cors = { "access-control-allow-origin": "*", "access-control-allow-headers": "*" };

function sse(events: { event: string; data: unknown; id?: number }[]) {
  return events.map((e) => `${e.id !== undefined ? `id: ${e.id}\n` : ""}event: ${e.event}\ndata: ${JSON.stringify(e.data)}\n\n`).join("");
}

async function mockApi(page: Page, opts: { run: string; posts?: unknown[] }) {
  const tower = fx("tower.json");
  const triggers = fx("triggers.json");
  const rc = fx("race_control.json");
  const runEvents = fx(opts.run) as { id: number; type: string }[];
  await page.route(`${API}/**`, async (route: Route) => {
    const req = route.request();
    const url = new URL(req.url());
    if (req.method() === "OPTIONS") return route.fulfill({ status: 204, headers: { ...cors, "access-control-allow-methods": "*" } });
    const json = (body: unknown, status = 200) => route.fulfill({ status, headers: { ...cors, "content-type": "application/json" }, body: JSON.stringify(body) });
    const stream = (body: string) => route.fulfill({ status: 200, headers: { ...cors, "content-type": "text/event-stream" }, body });
    switch (true) {
      case url.pathname === "/api/status":
        return json(fx("status.json"));
      case url.pathname === "/api/scenarios":
        return json(fx("scenarios.json"));
      case url.pathname === "/api/live/stream":
        return stream(
          "retry: 600000\n\n" +
            sse([
              { event: "tower", data: tower, id: 0 },
              ...triggers.slice(-5).map((t: unknown) => ({ event: "trigger", data: t })),
              ...rc.slice(-3).map((m: { message: string }) => ({ event: "race_control", data: { ...m, parsed: { kind: "INFO", strategic: false } } })),
              { event: "tower", data: tower, id: 1 },
            ]),
        );
      case url.pathname === "/api/strategy/runs" && req.method() === "POST":
        opts.posts?.push(JSON.parse(req.postData() ?? "{}"));
        return json({ run_id: "run-e2e", status: "queued", events_url: "/api/strategy/runs/run-e2e/events" }, 202);
      case url.pathname === "/api/strategy/runs/run-e2e/events":
        return stream(sse(runEvents.map((e) => ({ event: e.type, data: e, id: e.id }))));
      case url.pathname === "/api/race-control/trigger":
        opts.posts?.push(JSON.parse(req.postData() ?? "{}"));
        return json({ event: { kind: "VSC_DEPLOYED" }, published: { message: "VIRTUAL SAFETY CAR DEPLOYED" }, run: { run_id: "run-e2e", status: "queued", events_url: "" } }, 202);
      default:
        return json({ detail: `unmocked ${url.pathname}` }, 404);
    }
  });
}

test("live tower, stream KPIs and feeds render from the stream", async ({ page }) => {
  await mockApi(page, { run: "run_GP02.json" });
  await page.goto("/");
  const tower = fx("tower.json");
  await expect(page.getByTestId("tower-row")).toHaveCount(tower.drivers.length);
  await expect(page.getByTestId("tower-row").first()).toContainText(tower.drivers[0].driver_code);
  await expect(page.getByTestId("tower-row").first()).toContainText("Leader");
  const badges = await page.getByTestId("tyre-badge").allTextContents();
  expect(badges.every((b) => ["S", "M", "H", "I", "W"].includes(b))).toBe(true);
  await expect(page.getByTestId("kpi-events")).toContainText("60.0K");
  await expect(page.getByTestId("live-pill")).toContainText("live stream");
  await expect(page.getByTestId("trigger-feed").locator("li")).toHaveCount(5);
  await expect(page.getByTestId("rc-feed").locator("li")).toHaveCount(3);
});

test("a recorded cyclic run streams its trace, self-correction and final call", async ({ page }) => {
  const posts: unknown[] = [];
  await mockApi(page, { run: "run_GP08_scripted.json", posts });
  await page.goto("/");
  await page.getByTestId("scenario-select").selectOption("GP08");
  await expect(page.getByTestId("scenario-question")).toContainText("Leclerc started on mediums");
  await page.getByTestId("run-button").click();
  expect(posts[0]).toEqual({ scenario_id: "GP08", mode: "agent", fault_rate: 0 });
  await expect(page.getByTestId("trace-step")).toHaveCount(8);
  await expect(page.getByTestId("verifier-revise")).toBeVisible();
  await expect(page.getByTestId("final-action")).toHaveText("Pit now · Soft");
  await expect(page.getByTestId("compliance")).toContainText("compliant");
  await expect(page.getByTestId("revisions")).toHaveText("1");
  await expect(page.getByTestId("strategy-row")).toHaveCount(6);
  await expect(page.getByTestId("citation").first()).toBeVisible();
});

test("strategy chart: hover tooltip and table view on a real run", async ({ page }) => {
  await mockApi(page, { run: "run_GP10.json" });
  await page.goto("/");
  await page.getByTestId("scenario-select").selectOption("GP10");
  await page.getByTestId("run-button").click();
  await expect(page.getByTestId("final-call")).toBeVisible();
  await page.getByTestId("strategy-row").nth(1).hover();
  await expect(page.getByTestId("strategy-tooltip")).toContainText("expected");
  await page.getByTestId("strategy-view-toggle").click();
  await expect(page.getByTestId("strategy-table").locator("tbody tr")).toHaveCount(6);
  await expect(page.getByTestId("undercut")).toContainText("Penalty hold at the stop");
  await expect(page.getByTestId("undercut-verdict")).toBeVisible();
  await expect(page.getByTestId("traffic")).toBeVisible();
});

test("race-control trigger posts the message for the selected car", async ({ page }) => {
  const posts: { message?: string; driver_number?: number }[] = [];
  await mockApi(page, { run: "run_GP02.json", posts: posts as unknown[] });
  await page.goto("/");
  await expect(page.getByTestId("tower-row").first()).toBeVisible();
  const third = fx("tower.json").drivers[2];
  await page.getByTestId("tower-row").nth(2).click();
  await expect(page.getByTestId("rc-car")).toHaveValue(String(third.driver_number));
  await page.getByTestId("rc-message").selectOption({ index: 3 }); // time penalty preset
  await page.getByTestId("rc-trigger").click();
  await expect(page.getByTestId("rc-result")).toContainText("Published to Kafka");
  expect(posts[0].message).toBe(`FIA STEWARDS: 5 SECOND TIME PENALTY FOR CAR ${third.driver_number} - CAUSING A COLLISION`);
  expect(posts[0].driver_number).toBe(third.driver_number);
  await expect(page.getByTestId("final-call")).toBeVisible();
});
