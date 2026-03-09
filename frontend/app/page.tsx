"use client";

import { FormEvent, useEffect, useMemo, useRef, useState } from "react";

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

type SourceOption = {
  source_name: string;
  article_count: number;
};

const DEFAULT_LIMIT = 8;
const MAX_LIMIT = 20;

export default function Home() {
  const [keywords, setKeywords] = useState("inflation cooling in the US labor market");
  const [selectedSources, setSelectedSources] = useState<string[]>([]);
  const [sourceInput, setSourceInput] = useState("");
  const [sourceOptions, setSourceOptions] = useState<SourceOption[]>([]);
  const [isSourcesLoading, setIsSourcesLoading] = useState(false);
  const [sourcesError, setSourcesError] = useState<string | null>(null);
  const [isSourceDropdownOpen, setIsSourceDropdownOpen] = useState(false);
  const [minPublishedAt, setMinPublishedAt] = useState("");
  const [limit, setLimit] = useState(DEFAULT_LIMIT);
  const [search, setSearch] = useState<SearchState>({
    isLoading: false,
    error: null,
    results: [],
    lastQuery: "",
  });
  const sourceDropdownRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    let isActive = true;

    async function loadSourceOptions() {
      setIsSourcesLoading(true);
      setSourcesError(null);
      try {
        const response = await fetch("/api/sources", {
          method: "GET",
          headers: { Accept: "application/json" },
          cache: "no-store",
        });
        const payload = await response.json();
        if (!response.ok) {
          const detail = payload?.detail ?? "Cannot load sources.";
          throw new Error(String(detail));
        }
        if (!isActive) return;
        setSourceOptions(Array.isArray(payload) ? payload : []);
      } catch (error) {
        if (!isActive) return;
        setSourcesError(error instanceof Error ? error.message : "Cannot load sources.");
      } finally {
        if (isActive) setIsSourcesLoading(false);
      }
    }

    loadSourceOptions();
    return () => {
      isActive = false;
    };
  }, []);

  useEffect(() => {
    function handlePointerDown(event: MouseEvent) {
      if (!sourceDropdownRef.current) return;
      if (sourceDropdownRef.current.contains(event.target as Node)) return;
      setIsSourceDropdownOpen(false);
    }
    document.addEventListener("mousedown", handlePointerDown);
    return () => document.removeEventListener("mousedown", handlePointerDown);
  }, []);

  const topScore = useMemo(() => {
    if (search.results.length === 0) return 0;
    return Math.max(...search.results.map((result) => result.similarity));
  }, [search.results]);

  const filteredSourceOptions = useMemo(() => {
    const selected = new Set(selectedSources.map((source) => source.toLowerCase()));
    const searchText = sourceInput.trim().toLowerCase();
    const searchTokens = searchText ? searchText.split(/\s+/).filter(Boolean) : [];

    return sourceOptions.filter((option) => {
      if (selected.has(option.source_name.toLowerCase())) return false;
      if (!searchTokens.length) return true;
      const normalized = option.source_name.toLowerCase();
      return searchTokens.every((token) => normalized.includes(token));
    });
  }, [selectedSources, sourceInput, sourceOptions]);

  function addSource(sourceName: string) {
    const normalized = sourceName.trim();
    if (!normalized) return;
    setSelectedSources((prev) => {
      if (prev.some((source) => source.toLowerCase() === normalized.toLowerCase())) {
        return prev;
      }
      return [...prev, normalized];
    });
    setSourceInput("");
    setIsSourceDropdownOpen(true);
  }

  function removeSource(sourceName: string) {
    const normalized = sourceName.trim().toLowerCase();
    setSelectedSources((prev) =>
      prev.filter((source) => source.trim().toLowerCase() !== normalized)
    );
    setIsSourceDropdownOpen(true);
  }

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
    for (const sourceName of selectedSources) {
      params.append("source_names", sourceName);
    }
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
        <section className="card fade-in-up relative z-30 mb-6">
          <div className="bg-[linear-gradient(120deg,#123c46_0%,#1f9d85_52%,#35c5aa_100%)] px-6 py-7 text-white sm:px-8">
            <p className="font-mono text-xs uppercase tracking-[0.22em] text-white/72">
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
              <span className="mb-1.5 block text-xs font-semibold uppercase tracking-[0.12em] text-[var(--muted)]">
                Keyword Query
              </span>
              <textarea
                value={keywords}
                onChange={(event) => setKeywords(event.target.value)}
                rows={3}
                className="w-full rounded-xl border border-[var(--edge)] bg-[var(--paper-strong)] px-3 py-2.5 text-sm text-[var(--ink)] outline-none transition placeholder:text-[color:rgba(155,177,185,0.7)] focus:border-[var(--accent)] focus:ring-2 focus:ring-[var(--accent-soft)]"
                placeholder="e.g. inflation expectations, fed policy pause, disinflation"
              />
            </label>

            <div ref={sourceDropdownRef} className="relative">
              <span className="mb-1.5 block text-xs font-semibold uppercase tracking-[0.12em] text-[var(--muted)]">
                Source Filter
              </span>
              <div className="min-h-[44px] w-full rounded-xl border border-[var(--edge)] bg-[var(--paper-strong)] px-3 py-2 text-sm text-[var(--ink)] focus-within:border-[var(--accent)] focus-within:ring-2 focus-within:ring-[var(--accent-soft)]">
                <div className="flex flex-wrap items-center gap-1.5">
                  {selectedSources.map((source, index) => (
                    <span
                      key={source}
                      className="inline-flex items-center gap-1 rounded-full border border-[var(--edge)] bg-[var(--paper-soft)] px-2 py-0.5 text-xs text-[var(--ink)]"
                    >
                      <span>{index < selectedSources.length - 1 ? `${source},` : source}</span>
                      <button
                        type="button"
                        aria-label={`Remove ${source}`}
                        onClick={() => removeSource(source)}
                        className="font-semibold leading-none text-[var(--muted)] hover:text-[var(--ink)]"
                      >
                        x
                      </button>
                    </span>
                  ))}
                  <input
                    value={sourceInput}
                    onChange={(event) => {
                      setSourceInput(event.target.value);
                      setIsSourceDropdownOpen(true);
                    }}
                    onFocus={() => setIsSourceDropdownOpen(true)}
                    onKeyDown={(event) => {
                      if (event.key === "Enter") {
                        if (filteredSourceOptions.length > 0) {
                          event.preventDefault();
                          addSource(filteredSourceOptions[0].source_name);
                        }
                        return;
                      }
                      if (event.key === "Backspace" && !sourceInput && selectedSources.length > 0) {
                        event.preventDefault();
                        removeSource(selectedSources[selectedSources.length - 1]);
                      }
                    }}
                    className="min-w-[180px] flex-1 bg-transparent py-0.5 text-sm text-[var(--ink)] outline-none placeholder:text-[color:rgba(155,177,185,0.7)]"
                    placeholder={
                      selectedSources.length > 0
                        ? "Type to add more sources..."
                        : "Type to search sources..."
                    }
                  />
                </div>
              </div>

              {isSourceDropdownOpen ? (
                <div className="mt-1.5 max-h-48 w-full overflow-y-auto rounded-xl border border-[var(--edge)] bg-[var(--paper-strong)] p-1.5 shadow-lg">
                  {isSourcesLoading ? (
                    <p className="px-2 py-2 text-sm text-[var(--muted)]">Loading sources...</p>
                  ) : null}
                  {!isSourcesLoading && sourcesError ? (
                    <p className="px-2 py-2 text-sm text-[var(--danger)]">{sourcesError}</p>
                  ) : null}
                  {!isSourcesLoading && !sourcesError && filteredSourceOptions.length === 0 ? (
                    <p className="px-2 py-2 text-sm text-[var(--muted)]">
                      No sources match your search.
                    </p>
                  ) : null}
                  {!isSourcesLoading && !sourcesError && filteredSourceOptions.length > 0 ? (
                    <ul className="space-y-1">
                      {filteredSourceOptions.map((option) => (
                        <li key={option.source_name}>
                          <button
                            type="button"
                            onClick={() => addSource(option.source_name)}
                            className="flex w-full items-center justify-between rounded-lg px-2 py-1.5 text-left text-sm text-[var(--ink)] hover:bg-[var(--paper-soft)]"
                          >
                            <span>{option.source_name}</span>
                            <span className="font-mono text-xs text-[var(--muted)]">
                              {option.article_count}
                            </span>
                          </button>
                        </li>
                      ))}
                    </ul>
                  ) : null}
                </div>
              ) : null}
            </div>

            <label>
              <span className="mb-1.5 block text-xs font-semibold uppercase tracking-[0.12em] text-[var(--muted)]">
                Published After
              </span>
              <input
                type="datetime-local"
                value={minPublishedAt}
                onChange={(event) => setMinPublishedAt(event.target.value)}
                className="w-full rounded-xl border border-[var(--edge)] bg-[var(--paper-strong)] px-3 py-2 text-sm text-[var(--ink)] outline-none transition focus:border-[var(--accent)] focus:ring-2 focus:ring-[var(--accent-soft)]"
              />
            </label>

            <label className="sm:col-span-2">
              <span className="mb-1.5 block text-xs font-semibold uppercase tracking-[0.12em] text-[var(--muted)]">
                Result Limit: {limit}
              </span>
              <input type="range" min={1} max={MAX_LIMIT} value={limit} onChange={(event) => setLimit(Number(event.target.value))} className="w-full accent-[var(--accent)]" />
            </label>

            <div className="sm:col-span-2">
              <button
                type="submit"
                disabled={search.isLoading}
                className="inline-flex items-center rounded-full border border-[var(--accent-strong)] bg-[var(--accent)] px-5 py-2.5 text-sm font-semibold text-[#052018] transition hover:bg-[var(--accent-strong)] hover:text-white disabled:cursor-not-allowed disabled:opacity-70"
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
                <h2 className="text-xl font-semibold text-[var(--ink)]">Ranked Results</h2>
                <p className="text-sm text-[var(--muted)]">
                  {search.lastQuery
                    ? `Query: "${search.lastQuery}"`
                    : "Run a query to retrieve semantically similar articles."}
                </p>
              </div>
              <span className="rounded-full bg-[var(--warm-soft)] px-3 py-1 font-mono text-xs text-[var(--warm)]">
                Top score: {topScore.toFixed(3)}
              </span>
            </div>

            {search.error ? (
              <div className="rounded-xl border border-[color:rgba(255,180,138,0.28)] bg-[var(--danger-soft)] px-4 py-3 text-sm text-[var(--danger)]">
                {search.error}
              </div>
            ) : null}

            {search.isLoading ? (
              <div className="space-y-3">
                {Array.from({ length: 4 }).map((_, index) => (
                  <div
                    key={index}
                    className="h-20 animate-pulse rounded-xl border border-[var(--edge)] bg-[var(--paper-soft)]"
                  />
                ))}
              </div>
            ) : null}

            {!search.isLoading && !search.error && search.results.length === 0 ? (
              <div className="rounded-xl border border-dashed border-[var(--edge)] bg-[var(--paper-soft)] px-4 py-8 text-center text-sm text-[var(--muted)]">
                No results yet.
              </div>
            ) : null}

            {!search.isLoading && search.results.length > 0 ? (
              <ol className="space-y-3">
                {search.results.map((result, index) => (
                  <li
                    key={result.article_id}
                    className="fade-in-up rounded-xl border border-[var(--edge)] bg-[var(--paper-strong)] p-4"
                    style={{ animationDelay: `${index * 60}ms` }}
                  >
                    <div className="mb-2 flex items-center justify-between gap-3">
                      <span className="font-mono text-xs text-[var(--muted)]">#{index + 1}</span>
                      <span className="rounded-full bg-[var(--accent-soft)] px-2.5 py-1 font-mono text-xs text-[var(--accent)]">
                        score {result.similarity.toFixed(3)}
                      </span>
                    </div>
                    <h3 className="text-base font-semibold text-[var(--ink)]">
                      {result.title ?? "(untitled)"}
                    </h3>
                    <div className="mt-2 flex flex-wrap items-center gap-2 text-xs text-[var(--muted)]">
                      <span className="rounded-full bg-[var(--paper-soft)] px-2 py-1">{result.source_name}</span>
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
                      className="mt-3 inline-block text-sm font-medium text-[var(--accent)] underline decoration-[color:rgba(60,199,170,0.5)] decoration-2 underline-offset-3"
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
              <h2 className="text-lg font-semibold text-[var(--ink)]">System Status</h2>
              <p className="mt-2 text-sm text-[var(--muted)]">
                Semantic search is wired end-to-end with vector similarity on stored article
                embeddings.
              </p>
              <div className="mt-4 rounded-xl border border-[color:rgba(60,199,170,0.24)] bg-[var(--accent-soft)] px-3 py-2 font-mono text-xs text-[var(--accent)]">
                Active feature: keyword semantic retrieval
              </div>
            </section>

            <section className="card px-5 py-5 sm:px-6">
              <h2 className="text-lg font-semibold text-[var(--ink)]">Roadmap Panels</h2>
              <ul className="mt-3 space-y-2 text-sm text-[var(--muted)]">
                <li className="rounded-lg bg-[var(--paper-soft)] px-3 py-2">Theme detection and cluster labels</li>
                <li className="rounded-lg bg-[var(--paper-soft)] px-3 py-2">Entity extraction per article</li>
                <li className="rounded-lg bg-[var(--paper-soft)] px-3 py-2">Narrative graph explorer</li>
                <li className="rounded-lg bg-[var(--paper-soft)] px-3 py-2">Macro signal timeline dashboard</li>
              </ul>
            </section>
          </div>
        </section>
      </main>
    </div>
  );
}
