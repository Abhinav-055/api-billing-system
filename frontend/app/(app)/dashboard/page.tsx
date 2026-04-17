"use client";

import { useEffect, useState } from "react";
import { Area, AreaChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { appApi } from "@/lib/api";
import type { UsageSummary } from "@/lib/types";

export default function DashboardPage() {
  const [summary, setSummary] = useState<UsageSummary | null>(null);
  const [error, setError] = useState<string>("");

  useEffect(() => {
    appApi
      .get("/proxy/api/v1/usage/summary")
      .then((response) => {
        setSummary(response.data as UsageSummary);
        setError("");
      })
      .catch(() => setError("Unable to load usage summary"));
  }, []);

  return (
    <div className="space-y-4">
      <div className="grid gap-4 md:grid-cols-3">
        <article className="card p-5">
          <p className="text-sm text-muted">Today Cost</p>
          <p className="mt-2 text-3xl font-semibold text-ink">{summary?.today_cost ?? 0}</p>
        </article>
        <article className="card p-5">
          <p className="text-sm text-muted">Total Cost</p>
          <p className="mt-2 text-3xl font-semibold text-ink">{summary?.total_cost ?? 0}</p>
        </article>
        <article className="card p-5">
          <p className="text-sm text-muted">Today Requests</p>
          <p className="mt-2 text-3xl font-semibold text-ink">{summary?.today_requests ?? 0}</p>
        </article>
      </div>

      <section className="card p-5">
        <h2 className="text-lg font-semibold text-ink">7-day spend trend</h2>
        <div className="mt-4 h-80">
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={summary?.chart_data ?? []}>
              <defs>
                <linearGradient id="cost" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor="#0f766e" stopOpacity={0.7} />
                  <stop offset="100%" stopColor="#0f766e" stopOpacity={0.05} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="date" tick={{ fontSize: 11 }} />
              <YAxis tick={{ fontSize: 11 }} />
              <Tooltip />
              <Area type="monotone" dataKey="cost" stroke="#0f766e" fill="url(#cost)" strokeWidth={2} />
            </AreaChart>
          </ResponsiveContainer>
        </div>
        {error ? <p className="mt-2 text-sm text-signal">{error}</p> : null}
      </section>
    </div>
  );
}
