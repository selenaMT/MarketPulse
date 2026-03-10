import { NextRequest, NextResponse } from "next/server";

const DEFAULT_API_BASE = "http://127.0.0.1:8000";

function buildBackendUrl(req: NextRequest): string {
  const upstreamBase =
    process.env.MARKETPULSE_API_BASE_URL ??
    process.env.NEXT_PUBLIC_MARKETPULSE_API_BASE_URL ??
    DEFAULT_API_BASE;
  const query = req.nextUrl.searchParams.toString();
  return `${upstreamBase}/themes/hot${query ? `?${query}` : ""}`;
}

export async function GET(req: NextRequest) {
  const url = buildBackendUrl(req);
  try {
    const response = await fetch(url, {
      method: "GET",
      headers: { Accept: "application/json" },
      cache: "no-store",
    });

    const text = await response.text();
    const contentType = response.headers.get("content-type") ?? "application/json";
    const isJson = contentType.toLowerCase().includes("application/json");
    if (!response.ok && !isJson) {
      return NextResponse.json(
        {
          detail:
            text.trim() ||
            `Backend error (${response.status}) while loading hot themes.`,
        },
        { status: response.status },
      );
    }
    return new NextResponse(text, {
      status: response.status,
      headers: { "content-type": contentType },
    });
  } catch {
    return NextResponse.json(
      {
        detail:
          "Cannot reach backend API. Start FastAPI at http://127.0.0.1:8000 or set MARKETPULSE_API_BASE_URL.",
      },
      { status: 502 },
    );
  }
}
