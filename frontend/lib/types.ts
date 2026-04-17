export type UsageSummary = {
  today_cost: number;
  total_cost: number;
  today_requests: number;
  chart_data: Array<{ date: string; cost: number }>;
};

export type UsageLog = {
  user_id: string;
  endpoint: string;
  method: string;
  cost: number;
  request_id: string;
  status_code: number;
  latency_ms: number;
  timestamp: string;
};

export type WalletBalance = {
  user_id: string;
  balance: number;
  has_sufficient_funds: boolean;
};

export type ApiKey = {
  key_id: string;
  user_id: string;
  prefix: string;
  revoked: boolean;
  created_at: string;
  revoked_at?: string;
};

export type Transaction = {
  tx_id: string;
  transaction_id?: string;
  user_id: string;
  amount: number;
  type: string;
  reference?: string;
  balance_after?: number;
  status: string;
  created_at: string;
};
