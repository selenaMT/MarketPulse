import type { Metadata } from "next";
import { IBM_Plex_Mono, Sora } from "next/font/google";
import "./globals.css";
import { AuthProvider } from "./contexts/AuthContext";

const sora = Sora({
  variable: "--font-sora",
  subsets: ["latin"],
});

const ibmPlexMono = IBM_Plex_Mono({
  variable: "--font-ibm-plex-mono",
  subsets: ["latin"],
  weight: ["400", "500"],
});

export const metadata: Metadata = {
  title: "MarketPulse | Macro Chat And Search",
  description: "Ask macro questions and search news articles using grounded semantic retrieval.",
};

const bodyClasses = [
  sora.variable,
  ibmPlexMono.variable,
  "antialiased",
].join(" ");

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body className={bodyClasses}>
        <AuthProvider>
          {children}
        </AuthProvider>
      </body>
    </html>
  );
}
