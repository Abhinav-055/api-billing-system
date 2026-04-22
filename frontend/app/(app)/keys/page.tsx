"use client";

import { useCallback, useEffect, useState } from "react";
import { appApi } from "@/lib/api";
import type { ApiKey } from "@/lib/types";

export default function KeysPage() {
  const [keys, setKeys] = useState<ApiKey[]>([]);
  const [newKey, setNewKey] = useState<string>("");
  const [copyState, setCopyState] = useState<string>("");
  const [error, setError] = useState<string>("");

  const loadKeys = useCallback(() => {
    appApi
      .get("/proxy/keys")
      .then((response) => setKeys((response.data.data as ApiKey[]) ?? []))
      .catch(() => setError("Unable to load API keys"));
  }, []);

  useEffect(() => {
    loadKeys();
  }, [loadKeys]);

  const generateKey = async () => {
    try {
      const response = await appApi.post("/proxy/keys/generate");
      setNewKey(response.data.api_key ?? "");
      loadKeys();
    } catch {
      setError("Unable to generate API key");
    }
  };

  const revokeKey = async (keyId: string) => {
    try {
      await appApi.delete(`/proxy/keys/${keyId}`);
      loadKeys();
    } catch {
      setError("Unable to revoke API key");
    }
  };

  const copyNewKey = async () => {
    if (!newKey) {
      return;
    }

    try {
      await navigator.clipboard.writeText(newKey);
      setCopyState("Copied");
    } catch {
      setCopyState("Copy failed");
    }
  };

  return (
    <div className="space-y-4">
      <section className="card p-5">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <h2 className="text-lg font-semibold text-ink">API Keys</h2>
          <button
            type="button"
            onClick={generateKey}
            className="rounded-xl bg-accent px-4 py-2 font-semibold text-white transition hover:opacity-90"
          >
            Generate New Key
          </button>
        </div>
        {newKey ? (
          <div className="mt-3 rounded-xl border border-accent/40 bg-accent/10 p-3">
            <p className="mb-2 text-xs font-semibold uppercase tracking-[0.12em] text-accent">New API Key</p>
            <div className="flex flex-col gap-2 md:flex-row md:items-center">
              <div className="min-w-0 flex-1 rounded-lg border border-muted/40 bg-panel px-3 py-2 font-mono text-sm text-ink">
                {newKey}
              </div>
              <button
                type="button"
                onClick={copyNewKey}
                className="rounded-lg border border-accent/40 bg-accent px-3 py-2 text-sm font-semibold text-white"
              >
                Copy
              </button>
            </div>
            {copyState ? <p className="mt-2 text-xs text-accent">{copyState}</p> : null}
          </div>
        ) : null}
        {error ? <p className="mt-2 text-sm text-signal">{error}</p> : null}
      </section>

      <section className="card p-5">
        <div className="overflow-x-auto">
          <table className="min-w-full text-left text-sm">
            <thead>
              <tr className="text-muted">
                <th className="pb-2">Key ID</th>
                <th className="pb-2">Prefix</th>
                <th className="pb-2">Status</th>
                <th className="pb-2">Action</th>
              </tr>
            </thead>
            <tbody>
              {keys.map((keyItem) => (
                <tr key={keyItem.key_id} className="border-t border-muted/20">
                  <td className="py-2">{keyItem.key_id}</td>
                  <td className="py-2">{keyItem.prefix}</td>
                  <td className="py-2">{keyItem.revoked ? "revoked" : "active"}</td>
                  <td className="py-2">
                    {!keyItem.revoked ? (
                      <button
                        type="button"
                        onClick={() => revokeKey(keyItem.key_id)}
                        className="rounded-lg border border-signal/40 px-2 py-1 text-xs text-signal"
                      >
                        Revoke
                      </button>
                    ) : (
                      "-"
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}
