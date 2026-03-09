"use client";

import { FormEvent, useMemo, useState } from "react";

type SearchResult = {
  article_id: string;
  canonical_url: string;
  title: string | null;
  published_at: string | null;
  source_name: string;
  similarity: number;
};

type SearchState = {
  isLoading: boolean;
  error: string | null;
  results: SearchResult[];
  lastQuery: string;
};

const DEFAULT_LIMIT = 8;
const MAX_LIMIT = 20;

export default function Home() {
  const [keywords, setKeywords] = useState("inflation cooling in the US labor market");
  const [sourceName, setSourceName] = useState("");
  const [minPublishedAt, setMinPublishedAt] = useState("");
  const [limit, setLimit] = useState(DEFAULT_LIMIT);
  const [search, setSearch] = useState<SearchState>({
    isLoading: false,
    error: null,
    results: [],
    lastQuery: "",
  });

  const topScore = useMemo(() => {
    if (search.results.length === 0) return 0;
    return Math.max(...search.results.map((result) => result.similarity));
  }, [search.results]);

  async function onSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const trimmedKeywords = keywords.trim();
    if (!trimmedKeywords) {
      setSearch((prev) => ({ ...prev, error: "Please enter at least one keyword." }));
      return;
    }

    setSearch((prev) => ({
      ...prev,
      isLoading: true,
      error: null,
      lastQuery: trimmedKeywords,
    }));

    const params = new URLSearchParams({
      keywords: trimmedKeywords,
      limit: String(limit),
    });
    if (sourceName.trim()) params.set("source_name", sourceName.trim());
    if (minPublishedAt) params.set("min_published_at", new Date(minPublishedAt).toISOString());

    try {
      const response = await fetch(`/api/semantic-search?${params.toString()}`, {
        method: "GET",
        headers: { Accept: "application/json" },
      });

      const payload = await response.json();
      if (!response.ok) {
        const detail = payload?.detail ?? "Search request failed.";
        throw new Error(String(detail));
      }

      setSearch((prev) => ({
        ...prev,
        isLoading: false,
        results: Array.isArray(payload) ? payload : [],
      }));
    } catch (error) {
      setSearch((prev) => ({
        ...prev,
        isLoading: false,
        results: [],
        error: error instanceof Error ? error.message : "Unknown error occurred.",
      }));
    }
  }

  return (
    <div className="grain min-h-screen">
      <main className="mx-auto w-full max-w-6xl px-4 py-8 sm:px-6 sm:py-10 lg:px-8">
        <section className="card fade-in-up mb-6 overflow-hidden">
          <div className="bg-[linear-gradient(120deg,#106f5e_0%,#1f8a74_100%)] px-6 py-7 text-white sm:px-8">
            <p className="font-mono text-xs uppercase tracking-[0.22em] text-white/80">
              MarketPulse
            </p>
            <h1 className="mt-2 text-3xl font-semibold tracking-tight sm:text-4xl">
              Semantic Article Search
            </h1>
            <p className="mt-3 max-w-2xl text-sm leading-relaxed text-white/85 sm:text-base">
              Search macro news by meaning, not exact wording. This is the first end-to-end
              feature wired to the backend vector retrieval API.
            </p>
          </div>

          <form onSubmit={onSubmit} className="grid gap-4 px-5 py-5 sm:grid-cols-2 sm:px-8 sm:py-7">
            <label className="sm:col-span-2">
              <span className="mb-1.5 block text-xs font-semibold uppercase tracking-[0.12em] text-[#5f6470]">
                Keyword Query
              </span>
              <textarea
                value={keywords}
                onChange={(event) => setKeywords(event.target.value)}
                rows={3}
                className="w-full rounded-xl border border-[#d8d3c8] bg-white px-3 py-2.5 text-sm text-[#1e2024] outline-none transition focus:border-[#106f5e] focus:ring-2 focus:ring-[#dcf2ed]"
                placeholder="e.g. inflation expectations, fed policy pause, disinflation"
              />
            </label>

            <label>
              <span className="mb-1.5 block text-xs font-semibold uppercase tracking-[0.12em] text-[#5f6470]">
                Source Filter
              </span>
              <input
                value={sourceName}
                onChange={(event) => setSourceName(event.target.value)}
                className="w-full rounded-xl border border-[#d8d3c8] bg-white px-3 py-2 text-sm outline-none transition focus:border-[#106f5e] focus:ring-2 focus:ring-[#dcf2ed]"
                placeholder="Reuters, Bloomberg, AP..."
              />
            </label>

            <label>
              <span className="mb-1.5 block text-xs font-semibold uppercase tracking-[0.12em] text-[#5f6470]">
                Published After
              </span>
              <input
                type="datetime-local"
                value={minPublishedAt}
                onChange={(event) => setMinPublishedAt(event.target.value)}
                className="w-full rounded-xl border border-[#d8d3c8] bg-white px-3 py-2 text-sm outline-none transition focus:border-[#106f5e] focus:ring-2 focus:ring-[#dcf2ed]"
              />
            </label>

            <label className="sm:col-span-2">
              <span className="mb-1.5 block text-xs font-semibold uppercase tracking-[0.12em] text-[#5f6470]">
                Result Limit: {limit}
              </span>
              <input
                type="range"
                min={1}
                max={MAX_LIMIT}
                value={limit}
                onChange={(event) => setLimit(Number(event.target.value))}
                className="w-full accent-[#106f5e]"
              />
            </label>

            <div className="sm:col-span-2">
              <button
                type="submit"
                disabled={search.isLoading}
                className="inline-flex items-center rounded-full border border-[#0d6555] bg-[#106f5e] px-5 py-2.5 text-sm font-semibold text-white transition hover:bg-[#0d6555] disabled:cursor-not-allowed disabled:opacity-70"
              >
                {search.isLoading ? "Searching..." : "Run Semantic Search"}
              </button>
            </div>
          </form>
        </section>

        <section className="grid gap-6 lg:grid-cols-[1.8fr_1fr]">
          <div className="card px-5 py-5 sm:px-7 sm:py-6">
            <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
              <div>
                <h2 className="text-xl font-semibold text-[#1e2024]">Ranked Results</h2>
                <p className="text-sm text-[#5f6470]">
                  {search.lastQuery
                    ? `Query: "${search.lastQuery}"`
                    : "Run a query to retrieve semantically similar articles."}
                </p>
              </div>
              <span className="rounded-full bg-[#fff2e5] px-3 py-1 font-mono text-xs text-[#af5a1c]">
                Top score: {topScore.toFixed(3)}
              </span>
            </div>

            {search.error ? (
              <div className="rounded-xl border border-[#efc3a1] bg-[#fff2e5] px-4 py-3 text-sm text-[#a2541b]">
                {search.error}
              </div>
            ) : null}

            {search.isLoading ? (
              <div className="space-y-3">
                {Array.from({ length: 4 }).map((_, index) => (
                  <div
                    key={index}
                    className="h-20 animate-pulse rounded-xl border border-[#e1dccc] bg-[#f8f5ee]"
                  />
                ))}
              </div>
            ) : null}

            {!search.isLoading && !search.error && search.results.length === 0 ? (
              <div className="rounded-xl border border-dashed border-[#d8d3c8] bg-[#fbf9f3] px-4 py-8 text-center text-sm text-[#5f6470]">
                No results yet.
              </div>
            ) : null}

            {!search.isLoading && search.results.length > 0 ? (
              <ol className="space-y-3">
                {search.results.map((result, index) => (
                  <li
                    key={result.article_id}
                    className="fade-in-up rounded-xl border border-[#ddd7cb] bg-white p-4"
                    style={{ animationDelay: `${index * 60}ms` }}
                  >
                    <div className="mb-2 flex items-center justify-between gap-3">
                      <span className="font-mono text-xs text-[#5f6470]">#{index + 1}</span>
                      <span className="rounded-full bg-[#dcf2ed] px-2.5 py-1 font-mono text-xs text-[#0d6555]">
                        score {result.similarity.toFixed(3)}
                      </span>
                    </div>
                    <h3 className="text-base font-semibold text-[#1e2024]">
                      {result.title ?? "(untitled)"}
                    </h3>
                    <div className="mt-2 flex flex-wrap items-center gap-2 text-xs text-[#5f6470]">
                      <span className="rounded-full bg-[#f4f1e9] px-2 py-1">{result.source_name}</span>
                      <span>
                        {result.published_at
                          ? new Date(result.published_at).toLocaleString()
                          : "Publish time unknown"}
                      </span>
                    </div>
                    <a
                      href={result.canonical_url}
                      target="_blank"
                      rel="noreferrer"
                      className="mt-3 inline-block text-sm font-medium text-[#106f5e] underline decoration-[#8ac8b9] decoration-2 underline-offset-3"
                    >
                      Open article
                    </a>
                  </li>
                ))}
              </ol>
            ) : null}
          </div>

          <div className="space-y-4">
            <section className="card px-5 py-5 sm:px-6">
              <h2 className="text-lg font-semibold text-[#1e2024]">System Status</h2>
              <p className="mt-2 text-sm text-[#5f6470]">
                Semantic search is wired end-to-end with vector similarity on stored article
                embeddings.
              </p>
              <div className="mt-4 rounded-xl border border-[#d5ebe5] bg-[#ebf8f4] px-3 py-2 font-mono text-xs text-[#0f6d5d]">
                Active feature: keyword semantic retrieval
              </div>
            </section>

            <section className="card px-5 py-5 sm:px-6">
              <h2 className="text-lg font-semibold text-[#1e2024]">Roadmap Panels</h2>
              <ul className="mt-3 space-y-2 text-sm text-[#4e5460]">
                <li className="rounded-lg bg-[#f8f5ee] px-3 py-2">Theme detection and cluster labels</li>
                <li className="rounded-lg bg-[#f8f5ee] px-3 py-2">Entity extraction per article</li>
                <li className="rounded-lg bg-[#f8f5ee] px-3 py-2">Narrative graph explorer</li>
                <li className="rounded-lg bg-[#f8f5ee] px-3 py-2">Macro signal timeline dashboard</li>
              </ul>
            </section>
          </div>
        </section>
      </main>
    </div>
  );
}
