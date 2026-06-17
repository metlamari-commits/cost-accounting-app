import type { Metadata } from "next";
import { Inter } from "next/font/google";
import "./globals.css";
import { el } from "@/locales/el";

const inter = Inter({
  variable: "--font-sans",
  subsets: ["latin", "greek"],
});

export const metadata: Metadata = {
  title: el.app.name,
  description: el.app.tagline,
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="el" className={`${inter.variable} h-full antialiased`}>
      <body className="min-h-full">{children}</body>
    </html>
  );
}
