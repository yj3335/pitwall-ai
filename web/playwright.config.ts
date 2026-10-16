import { defineConfig, devices } from "@playwright/test";

// The UI tests mock the control plane with recorded payloads, so they run
// without Ollama or Kafka. The live test (live.spec.ts) talks to a real
// stack and only runs when PITWALL_E2E_LIVE=1.
export default defineConfig({
  testDir: "./e2e",
  timeout: 60_000,
  fullyParallel: true,
  reporter: [["list"]],
  use: { baseURL: "http://127.0.0.1:3100", trace: "retain-on-failure" },
  // Full Chromium in new headless mode: the default headless shell delivers no
  // animation frames on macOS 27 / Apple M5, so actionability checks never settle.
  projects: [{ name: "chromium", use: { ...devices["Desktop Chrome"], channel: "chromium", viewport: { width: 1440, height: 1000 } } }],
  webServer: {
    // `make test-web` builds first and sets PW_NO_BUILD; standalone runs build here
    command: process.env.PW_NO_BUILD ? "npx next start -p 3100" : "npm run build && npx next start -p 3100",
    url: "http://127.0.0.1:3100",
    reuseExistingServer: !process.env.CI,
    timeout: 240_000,
    env: { NEXT_PUBLIC_API_URL: process.env.NEXT_PUBLIC_API_URL ?? "http://127.0.0.1:8080" },
  },
});
