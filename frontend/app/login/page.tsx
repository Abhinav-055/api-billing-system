"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import { appApi } from "@/lib/api";

export default function LoginPage() {
  const router = useRouter();
  const [email, setEmail] = useState<string>("");
  const [password, setPassword] = useState<string>("");
  const [isRegister, setIsRegister] = useState<boolean>(false);
  const [error, setError] = useState<string>("");

  const submit = async (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setError("");

    try {
      const endpoint = isRegister ? "/auth/register" : "/auth/login";
      await appApi.post(endpoint, { email, password });
      router.push("/dashboard");
      router.refresh();
    } catch {
      setError("Authentication failed");
    }
  };

  return (
    <div className="flex min-h-screen items-center justify-center p-4">
      <form onSubmit={submit} className="card w-full max-w-md space-y-4 p-6">
        <div>
          <p className="text-xs uppercase tracking-[0.2em] text-muted">API Billing</p>
          <h1 className="mt-2 text-2xl font-semibold text-ink">{isRegister ? "Create account" : "Sign in"}</h1>
        </div>

        <div>
          <label className="text-sm text-muted">Email</label>
          <input
            type="email"
            value={email}
            onChange={(event) => setEmail(event.target.value)}
            className="mt-1 w-full rounded-xl border border-muted/30 bg-panel px-3 py-2"
            required
          />
        </div>

        <div>
          <label className="text-sm text-muted">Password</label>
          <input
            type="password"
            value={password}
            onChange={(event) => setPassword(event.target.value)}
            className="mt-1 w-full rounded-xl border border-muted/30 bg-panel px-3 py-2"
            required
          />
        </div>

        <button type="submit" className="w-full rounded-xl bg-accent px-4 py-2 font-semibold text-white">
          {isRegister ? "Create account" : "Sign in"}
        </button>

        <button
          type="button"
          onClick={() => setIsRegister((value) => !value)}
          className="w-full rounded-xl border border-muted/40 px-4 py-2 text-sm"
        >
          {isRegister ? "Already have an account? Sign in" : "Need an account? Register"}
        </button>

        {error ? <p className="text-sm text-signal">{error}</p> : null}
      </form>
    </div>
  );
}
