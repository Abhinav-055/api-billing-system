import axios, { AxiosError } from "axios";
import { cookies } from "next/headers";
import { NextRequest, NextResponse } from "next/server";

const backend = process.env.BACKEND_URL ?? "http://localhost:8080";
const paymentBackend = process.env.PAYMENT_URL ?? "http://localhost:8083";

function resolveBackend(targetPath: string): string {
  if (targetPath.startsWith("/payments")) {
    return paymentBackend;
  }
  return backend;
}

async function forward(request: NextRequest, method: "GET" | "POST" | "PUT" | "PATCH" | "DELETE") {
  if (method !== "GET") {
    const origin = request.headers.get("origin");
    const referer = request.headers.get("referer");
    const allowedOrigin = request.nextUrl.origin;
    const source = origin ?? (referer ? new URL(referer).origin : "");
    if (request.headers.get("x-requested-with") !== "XMLHttpRequest" || source !== allowedOrigin) {
      return NextResponse.json({ error: "csrf validation failed" }, { status: 403 });
    }
  }
  const token = cookies().get("jwt")?.value;
  if (!token) {
    return NextResponse.json({ error: "unauthorized" }, { status: 401 });
  }

  const targetPath = request.nextUrl.pathname.replace("/api/proxy", "");
  const upstream = resolveBackend(targetPath);
  const target = `${upstream}${targetPath}${request.nextUrl.search}`;
  let body: unknown;
  if (method === "POST" || method === "PUT" || method === "PATCH") {
    try {
      body = await request.json();
    } catch {
      body = undefined;
    }
  }

  try {
    const response = await axios.request({
      url: target,
      method,
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json"
      },
      data: body,
      timeout: 10000,
      validateStatus: () => true
    });

    return NextResponse.json(response.data, { status: response.status });
  } catch (error) {
    const axiosError = error as AxiosError<{ error?: string }>;
    return NextResponse.json({ error: axiosError.response?.data?.error ?? "proxy error" }, { status: 502 });
  }
}

export async function GET(request: NextRequest) {
  return forward(request, "GET");
}

export async function POST(request: NextRequest) {
  return forward(request, "POST");
}

export async function DELETE(request: NextRequest) {
  return forward(request, "DELETE");
}

export async function PUT(request: NextRequest) {
  return forward(request, "PUT");
}

export async function PATCH(request: NextRequest) {
  return forward(request, "PATCH");
}
