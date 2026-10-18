import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  reactStrictMode: true,
  poweredByHeader: false,
  // the dashboard is opened as either localhost or 127.0.0.1; the dev server
  // blocks its own scripts for an origin it does not know
  allowedDevOrigins: ["127.0.0.1", "localhost"],
  // do not write tool-guidance files into the repository
  agentRules: false,
};

export default nextConfig;
