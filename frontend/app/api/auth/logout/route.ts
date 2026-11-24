import { NextResponse } from "next/server";

export async function POST() {
  const response = NextResponse.json({ status: "ok" }, { status: 200 });
  response.cookies.set({
    name: "jwt",
    value: "",
    httpOnly: true,
    sameSite: "lax",
    secure: (process.env.ENV ?? "production").toLowerCase() !== "local",
    path: "/",
    maxAge: 0
  });
  return response;
}
