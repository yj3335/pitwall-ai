import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "PitWall AI",
  description: "Live F1 telemetry, Monte Carlo strategy and a cyclic LangGraph pit-wall agent",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body className="min-h-screen antialiased">{children}</body>
    </html>
  );
}
