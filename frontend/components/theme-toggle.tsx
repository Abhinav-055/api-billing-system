"use client";

import { useEffect, useState } from "react";

export function ThemeToggle() {
  const [theme, setTheme] = useState<"light" | "dark">("light");

  useEffect(() => {
    const stored = window.localStorage.getItem("wallet-theme");
    const current = stored === "dark" ? "dark" : "light";
    setTheme(current);
    document.documentElement.classList.toggle("dark", current === "dark");
  }, []);

  const toggle = () => {
    const next = theme === "light" ? "dark" : "light";
    setTheme(next);
    document.documentElement.classList.toggle("dark", next === "dark");
    window.localStorage.setItem("wallet-theme", next);
  };

  return (
    <button
      type="button"
      onClick={toggle}
      className="rounded-full border border-muted/40 px-3 py-1 text-sm text-ink transition hover:border-accent hover:text-accent"
    >
      {theme === "light" ? "Dark" : "Light"} mode
    </button>
  );
}
