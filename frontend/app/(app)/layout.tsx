import { cookies } from "next/headers";
import { redirect } from "next/navigation";
import { Shell } from "@/components/shell";

export default function AppLayout({ children }: { children: React.ReactNode }) {
  const token = cookies().get("jwt")?.value;
  if (!token) {
    redirect("/login");
  }

  return <Shell>{children}</Shell>;
}
