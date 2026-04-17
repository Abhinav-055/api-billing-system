import axios from "axios";
import { NextResponse } from "next/server";

const backend = process.env.BACKEND_URL ?? "http://localhost:8080";

export async function POST(request: Request) {
  try {
    const payload = await request.json();
    const response = await axios.post(`${backend}/auth/login`, payload, { timeout: 10000 });
    const token = response.data.token;

    if (!token) {
      return NextResponse.json({ error: "missing token" }, { status: 502 });
    }

    const result = NextResponse.json({ user_id: response.data.user_id }, { status: 200 });
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
    return NextResponse.json({ error: "authentication failed" }, { status: 401 });
  }
}
