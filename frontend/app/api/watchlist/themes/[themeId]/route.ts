import { NextRequest, NextResponse } from "next/server";

const DEFAULT_API_BASE = "http://127.0.0.1:8000";

function buildBackendUrl(themeId: string): string {
  const upstreamBase =
    process.env.MARKETPULSE_API_BASE_URL ??
    process.env.NEXT_PUBLIC_MARKETPULSE_API_BASE_URL ??
    DEFAULT_API_BASE;
  return `${upstreamBase}/watchlist/themes/${encodeURIComponent(themeId)}`;
}

function buildHeaders(req: NextRequest): Record<string, string> {
  const headers: Record<string, string> = { Accept: "application/json" };
  const authorization = req.headers.get("authorization");
  if (authorization) headers.Authorization = authorization;
  return headers;
}

export async function DELETE(
  req: NextRequest,
  context: { params: Promise<{ themeId: string }> },
) {
  const { themeId } = await context.params;
  const url = buildBackendUrl(themeId);

  try {
    const response = await fetch(url, {
      method: "DELETE",
      headers: buildHeaders(req),
      cache: "no-store",
    });

    const text = await response.text();
    const contentType = response.headers.get("content-type") ?? "application/json";
    if (!response.ok) {
      try {
        JSON.parse(text);
      } catch {
        return NextResponse.json(
          {
            detail: text.trim() || `Backend error (${response.status}) while deleting watchlist theme.`,
          },
          { status: response.status },
        );
      }
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
