import axios from "axios";
import { NextResponse } from "next/server";

const backend = process.env.BACKEND_URL ?? "http://localhost:8080";

export async function POST(request: Request) {
  const origin = request.headers.get("origin");
  const referer = request.headers.get("referer");
  const host = new URL(request.url).origin;
  if (request.headers.get("x-requested-with") !== "XMLHttpRequest" ||
      (origin ?? (referer ? new URL(referer).origin : "")) !== host) {
    return NextResponse.json({ error: "csrf validation failed" }, { status: 403 });
  }
  try {
    const payload = await request.json();
    const response = await axios.post(`${backend}/auth/register`, payload, { timeout: 10000 });
    const token = response.data.token;

    if (!token) {
      return NextResponse.json({ error: "missing token" }, { status: 502 });
    }

    const result = NextResponse.json({ user_id: response.data.user_id }, { status: 201 });
    result.cookies.set({
      name: "jwt",
      value: token,
      httpOnly: true,
      sameSite: "lax",
      secure: process.env.NODE_ENV === "production",
      path: "/",
      maxAge: 60 * 60 * 24
    });
    return result;
  } catch {
    return NextResponse.json({ error: "registration failed" }, { status: 400 });
  }
}
