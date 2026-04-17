"use client";

import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import { Activity, KeyRound, LayoutDashboard, Wallet } from "lucide-react";
import { ThemeToggle } from "@/components/theme-toggle";
import { appApi } from "@/lib/api";
import { useEffect, useState } from "react";

const navItems = [
  { href: "/dashboard", label: "Dashboard", icon: LayoutDashboard },
  { href: "/wallet", label: "Wallet", icon: Wallet },
  { href: "/keys", label: "Keys", icon: KeyRound },
  { href: "/logs", label: "Logs", icon: Activity }
];

export function Shell({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();
  const router = useRouter();
  const [balance, setBalance] = useState<number | null>(null);

  useEffect(() => {
    appApi
      .get("/proxy/api/v1/wallet/balance")
      .then((response) => setBalance(Number(response.data.balance ?? 0)))
      .catch(() => setBalance(null));
  }, [pathname]);

  const logout = async () => {
    await appApi.post("/auth/logout");
    router.push("/login");
  };

  return (
    <div className="min-h-screen p-4 md:p-8">
      <div className="mx-auto grid w-full max-w-7xl gap-4 md:grid-cols-[220px_1fr]">
        <aside className="card p-5">
          <div className="mb-8">
            <p className="text-xs uppercase tracking-[0.2em] text-muted">API Billing</p>
            <h1 className="mt-2 text-2xl font-semibold text-ink">Control Plane</h1>
          </div>

          <nav className="space-y-2">
            {navItems.map((item) => {
              const Icon = item.icon;
              const active = pathname === item.href;
              return (
                <Link
                  key={item.href}
                  href={item.href}
                  className={`flex items-center gap-2 rounded-xl px-3 py-2 text-sm transition ${
                    active ? "bg-accent/15 text-accent" : "text-ink hover:bg-black/5"
                  }`}
                >
                  <Icon size={16} />
                  {item.label}
                </Link>
              );
            })}
          </nav>
        </aside>

        <section className="space-y-4">
          <header className="card flex items-center justify-between p-4">
            <div>
              <p className="text-xs uppercase tracking-[0.2em] text-muted">Current Wallet</p>
              <p className="text-lg font-semibold text-ink">{balance === null ? "Unavailable" : `${balance} credits`}</p>
            </div>
            <div className="flex items-center gap-3">
              <ThemeToggle />
              <button
                type="button"
                onClick={logout}
                className="rounded-full border border-signal/40 px-3 py-1 text-sm text-signal transition hover:bg-signal/10"
              >
                Logout
              </button>
            </div>
          </header>

          <main className="animate-reveal">{children}</main>
        </section>
      </div>
    </div>
  );
}
