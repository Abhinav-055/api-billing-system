"use client";

import { useEffect, useState } from "react";
import Script from "next/script";
import { appApi } from "@/lib/api";
import type { Transaction, WalletBalance } from "@/lib/types";

declare global {
  interface Window {
    Razorpay: new (options: Record<string, unknown>) => { open: () => void };
  }
}

export default function WalletPage() {
  const [balance, setBalance] = useState<number>(0);
  const [amount, setAmount] = useState<number>(100);
  const [transactions, setTransactions] = useState<Transaction[]>([]);
  const [error, setError] = useState<string>("");

  const refreshWalletState = async () => {
    const [balanceResponse, txResponse] = await Promise.all([
      appApi.get("/proxy/api/v1/wallet/balance"),
      appApi.get("/proxy/payments/transactions")
    ]);

    setBalance((balanceResponse.data as WalletBalance).balance);
    setTransactions((txResponse.data.data as Transaction[]) ?? []);
  };

  useEffect(() => {
    appApi
      .get("/proxy/api/v1/wallet/balance")
      .then((response) => setBalance((response.data as WalletBalance).balance))
      .catch(() => setError("Unable to load wallet balance"));

    appApi
      .get("/proxy/payments/transactions")
      .then((response) => setTransactions((response.data.data as Transaction[]) ?? []))
      .catch(() => setError("Unable to load transactions"));
  }, []);

  const addCredits = async () => {
    try {
      const { data } = await appApi.post("/proxy/payments/create-order", { amount });
      const order = data;

      if (!window.Razorpay) {
        setError("Razorpay SDK not loaded");
        return;
      }

      const razorpay = new window.Razorpay({
        key: order.key_id,
        amount: order.amount,
        currency: order.currency,
        name: "API Wallet",
        description: "Add wallet credits",
        order_id: order.order_id,
        handler: async (response: Record<string, string>) => {
          setError("");

          // Best-effort local webhook trigger; if provider webhook already processed,
          // this can return 400 and should not crash the UI.
          try {
            await appApi.post("/proxy/payments/confirm", {
              razorpay_order_id: response.razorpay_order_id,
              razorpay_payment_id: response.razorpay_payment_id,
              razorpay_signature: response.razorpay_signature
            });
          } catch {
            // Ignore callback sync failures and rely on provider webhook processing.
          }

          // Retry a few times because webhook processing can be asynchronous.
          for (let attempt = 0; attempt < 5; attempt += 1) {
            try {
              await refreshWalletState();
              return;
            } catch {
              await new Promise((resolve) => setTimeout(resolve, 1200));
            }
          }

          setError("Payment received. Wallet sync may take a few seconds. Please refresh.");
        }
      });

      razorpay.open();
    } catch {
      setError("Unable to create payment order");
    }
  };

  return (
    <div className="grid gap-4 lg:grid-cols-[320px_1fr]">
      <section className="card p-5">
        <h2 className="text-lg font-semibold text-ink">Add Credits</h2>
        <p className="mt-2 text-sm text-muted">Current balance: {balance} credits</p>

        <label className="mt-4 block text-sm text-muted">Amount</label>
        <input
          value={amount}
          onChange={(event) => setAmount(Number(event.target.value))}
          type="number"
          min={10}
          className="mt-1 w-full rounded-xl border border-muted/30 bg-panel px-3 py-2 text-ink"
        />

        <button
          type="button"
          onClick={addCredits}
          className="mt-4 rounded-xl bg-accent px-4 py-2 font-semibold text-white transition hover:opacity-90"
        >
          Add credits
        </button>
        {error ? <p className="mt-2 text-sm text-signal">{error}</p> : null}
      </section>

      <section className="card p-5">
        <h2 className="text-lg font-semibold text-ink">Transactions</h2>
        <div className="mt-4 overflow-x-auto">
          <table className="min-w-full text-left text-sm">
            <thead>
              <tr className="text-muted">
                <th className="pb-2">ID</th>
                <th className="pb-2">Type</th>
                <th className="pb-2">Amount</th>
                <th className="pb-2">Status</th>
              </tr>
            </thead>
            <tbody>
              {transactions.map((transaction) => (
                <tr key={transaction.tx_id} className="border-t border-muted/20">
                  <td className="py-2">{transaction.tx_id.slice(0, 12)}...</td>
                  <td className="py-2">{transaction.type}</td>
                  <td className="py-2">{transaction.amount}</td>
                  <td className="py-2">{transaction.status}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
      <Script src="https://checkout.razorpay.com/v1/checkout.js" strategy="afterInteractive" />
    </div>
  );
}
