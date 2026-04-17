import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Wallet and Billing Console",
  description: "Production billing and wallet control panel"
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
