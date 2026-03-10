"use client";

import { FormEvent, useEffect, useMemo, useRef, useState } from "react";

type ChatSource = {
  index: number;
  article_id: string;
  canonical_url: string;
  title: string | null;
  published_at: string | null;
  source_name: string;
  similarity: number;
};

type ChatState = {
  isLoading: boolean;
  error: string | null;
  answer: string;
  sources: ChatSource[];
  lastQuery: string;
};

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
  const [isChatOpen, setIsChatOpen] = useState(false);
  const [chatQuery, setChatQuery] = useState(
    "What are the main macro drivers in markets this week?",
  );
  const [keywords, setKeywords] = useState("inflation cooling in the US labor market");
  const [selectedSources, setSelectedSources] = useState<string[]>([]);
  const [sourceInput, setSourceInput] = useState("");
  const [sourceOptions, setSourceOptions] = useState<SourceOption[]>([]);
  const [isSourcesLoading, setIsSourcesLoading] = useState(false);
  const [sourcesError, setSourcesError] = useState<string | null>(null);
  const [isSourceDropdownOpen, setIsSourceDropdownOpen] = useState(false);
  const [minPublishedAt, setMinPublishedAt] = useState("");
  const [limit, setLimit] = useState(DEFAULT_LIMIT);
  const [chat, setChat] = useState<ChatState>({
    isLoading: false,
    error: null,
    answer: "",
    sources: [],
    lastQuery: "",
  });
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
          throw new Error(String(payload?.detail ?? "Cannot load sources."));
        }
        if (isActive) setSourceOptions(Array.isArray(payload) ? payload : []);
      } catch (error) {
        if (isActive) setSourcesError(error instanceof Error ? error.message : "Cannot load sources.");
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

  const filteredSourceOptions = useMemo(() => {
    const selected = new Set(selectedSources.map((source) => source.toLowerCase()));
    const searchText = sourceInput.trim().toLowerCase();
    const searchTokens = searchText ? searchText.split(/\s+/).filter(Boolean) : [];
    return sourceOptions.filter((option) => {
      if (selected.has(option.source_name.toLowerCase())) return false;
      if (!searchTokens.length) return true;
      return searchTokens.every((token) => option.source_name.toLowerCase().includes(token));
    });
  }, [selectedSources, sourceInput, sourceOptions]);

  const topScore = useMemo(() => {
    if (search.results.length === 0) return 0;
    return Math.max(...search.results.map((result) => result.similarity));
  }, [search.results]);

  function addSource(sourceName: string) {
    const normalized = sourceName.trim();
    if (!normalized) return;
    setSelectedSources((prev) =>
      prev.some((source) => source.toLowerCase() === normalized.toLowerCase()) ? prev : [...prev, normalized],
    );
    setSourceInput("");
    setIsSourceDropdownOpen(true);
  }

  function removeSource(sourceName: string) {
    const normalized = sourceName.trim().toLowerCase();
    setSelectedSources((prev) => prev.filter((source) => source.trim().toLowerCase() !== normalized));
    setIsSourceDropdownOpen(true);
  }

  async function onChatSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const trimmedQuery = chatQuery.trim();
    if (!trimmedQuery) {
      setChat((prev) => ({ ...prev, error: "Please enter a macro question." }));
      return;
    }

    setChat((prev) => ({
      ...prev,
      isLoading: true,
      error: null,
      lastQuery: trimmedQuery,
    }));

    const payload: {
      query: string;
      retrieval_limit: number;
      min_published_at?: string;
      source_names?: string[];
    } = {
      query: trimmedQuery,
      retrieval_limit: Math.min(limit, 5),
    };

    if (selectedSources.length > 0) payload.source_names = selectedSources;
    if (minPublishedAt) payload.min_published_at = new Date(minPublishedAt).toISOString();

    try {
      const response = await fetch("/api/chat/answer", {
        method: "POST",
        headers: { Accept: "application/json", "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });

      const result = await response.json();
      if (!response.ok) {
        const detail = result?.detail ?? "Chat request failed.";
        throw new Error(String(detail));
      }

      setChat((prev) => ({
        ...prev,
        isLoading: false,
        answer: typeof result?.answer === "string" ? result.answer : "",
        sources: Array.isArray(result?.sources) ? result.sources : [],
      }));
    } catch (error) {
      setChat((prev) => ({
        ...prev,
        isLoading: false,
        answer: "",
        sources: [],
        error: error instanceof Error ? error.message : "Unknown error occurred.",
      }));
    }
  }

  async function onSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const trimmedKeywords = keywords.trim();
    if (!trimmedKeywords) {
      setSearch((prev) => ({ ...prev, error: "Please enter at least one keyword." }));
      return;
    }
    setSearch((prev) => ({ ...prev, isLoading: true, error: null, lastQuery: trimmedKeywords }));
    const params = new URLSearchParams({ keywords: trimmedKeywords, limit: String(limit) });
    for (const sourceName of selectedSources) params.append("source_names", sourceName);
    if (minPublishedAt) params.set("min_published_at", new Date(minPublishedAt).toISOString());
    try {
      const response = await fetch(`/api/semantic-search?${params.toString()}`, {
        method: "GET",
        headers: { Accept: "application/json" },
      });
      const payload = await response.json();
      if (!response.ok) throw new Error(String(payload?.detail ?? "Search request failed."));
      setSearch((prev) => ({ ...prev, isLoading: false, results: Array.isArray(payload) ? payload : [] }));
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
        <section className="card mb-6">
          <div className="bg-[linear-gradient(120deg,#123c46_0%,#1f9d85_52%,#35c5aa_100%)] px-6 py-7 text-white sm:px-8">
            <h1 className="text-3xl font-semibold sm:text-4xl">Semantic Search</h1>
            <p className="mt-3 max-w-2xl text-sm text-white/85 sm:text-base">
              Theme features are intentionally disabled while a new theme solution is being rebuilt.
            </p>
          </div>
          <form onSubmit={onSubmit} className="grid gap-4 px-5 py-5 sm:grid-cols-2 sm:px-8 sm:py-7">
            <label className="sm:col-span-2">
              <span className="mb-1.5 block text-xs font-semibold uppercase tracking-[0.12em] text-[var(--muted)]">Keyword Query</span>
              <textarea
                value={keywords}
                onChange={(event) => setKeywords(event.target.value)}
                rows={3}
                className="w-full rounded-xl border border-[var(--edge)] bg-[var(--paper-strong)] px-3 py-2.5 text-sm text-[var(--ink)] outline-none focus:border-[var(--accent)] focus:ring-2 focus:ring-[var(--accent-soft)]"
              />
            </label>
            <div ref={sourceDropdownRef} className="relative">
              <span className="mb-1.5 block text-xs font-semibold uppercase tracking-[0.12em] text-[var(--muted)]">Source Filter</span>
              <div className="min-h-[44px] w-full rounded-xl border border-[var(--edge)] bg-[var(--paper-strong)] px-3 py-2 text-sm text-[var(--ink)]">
                <div className="flex flex-wrap items-center gap-1.5">
                  {selectedSources.map((source) => (
                    <span key={source} className="inline-flex items-center gap-1 rounded-full border border-[var(--edge)] bg-[var(--paper-soft)] px-2 py-0.5 text-xs text-[var(--ink)]">
                      <span>{source}</span>
                      <button type="button" onClick={() => removeSource(source)} className="font-semibold leading-none text-[var(--muted)]">x</button>
                    </span>
                  ))}
                  <input
                    value={sourceInput}
                    onChange={(event) => {
                      setSourceInput(event.target.value);
                      setIsSourceDropdownOpen(true);
                    }}
                    onFocus={() => setIsSourceDropdownOpen(true)}
                    className="min-w-[180px] flex-1 bg-transparent py-0.5 text-sm text-[var(--ink)] outline-none"
                    placeholder="Type to search sources..."
                  />
                </div>
              </div>
              {isSourceDropdownOpen ? (
                <div className="mt-1.5 max-h-48 w-full overflow-y-auto rounded-xl border border-[var(--edge)] bg-[var(--paper-strong)] p-1.5 shadow-lg">
                  {isSourcesLoading ? <p className="px-2 py-2 text-sm text-[var(--muted)]">Loading sources...</p> : null}
                  {!isSourcesLoading && sourcesError ? <p className="px-2 py-2 text-sm text-[var(--danger)]">{sourcesError}</p> : null}
                  {!isSourcesLoading && !sourcesError && filteredSourceOptions.map((option) => (
                    <button key={option.source_name} type="button" onClick={() => addSource(option.source_name)} className="mt-1 flex w-full items-center justify-between rounded-lg px-2 py-1.5 text-left text-sm text-[var(--ink)] hover:bg-[var(--paper-soft)]">
                      <span>{option.source_name}</span>
                      <span className="font-mono text-xs text-[var(--muted)]">{option.article_count}</span>
                    </button>
                  ))}
                </div>
              ) : null}
            </div>
            <label>
              <span className="mb-1.5 block text-xs font-semibold uppercase tracking-[0.12em] text-[var(--muted)]">Published After</span>
              <input type="datetime-local" value={minPublishedAt} onChange={(event) => setMinPublishedAt(event.target.value)} className="w-full rounded-xl border border-[var(--edge)] bg-[var(--paper-strong)] px-3 py-2 text-sm text-[var(--ink)] outline-none focus:border-[var(--accent)] focus:ring-2 focus:ring-[var(--accent-soft)]" />
            </label>
            <label className="sm:col-span-2">
              <span className="mb-1.5 block text-xs font-semibold uppercase tracking-[0.12em] text-[var(--muted)]">Result Limit: {limit}</span>
              <input type="range" min={1} max={MAX_LIMIT} value={limit} onChange={(event) => setLimit(Number(event.target.value))} className="w-full accent-[var(--accent)]" />
            </label>
            <div className="sm:col-span-2">
              <button type="submit" disabled={search.isLoading} className="inline-flex items-center rounded-full border border-[var(--accent-strong)] bg-[var(--accent)] px-5 py-2.5 text-sm font-semibold text-[#052018]">
                {search.isLoading ? "Searching..." : "Run Semantic Search"}
              </button>
            </div>
          </form>
        </section>

        <section className="card px-5 py-5 sm:px-7 sm:py-6">
          <div className="mb-4 flex items-center justify-between gap-3">
            <h2 className="text-xl font-semibold text-[var(--ink)]">Ranked Results</h2>
            <span className="rounded-full bg-[var(--warm-soft)] px-3 py-1 font-mono text-xs text-[var(--warm)]">Top score: {topScore.toFixed(3)}</span>
          </div>
          {search.error ? <div className="rounded-xl border border-[color:rgba(255,180,138,0.28)] bg-[var(--danger-soft)] px-4 py-3 text-sm text-[var(--danger)]">{search.error}</div> : null}
          {!search.isLoading && search.results.length === 0 ? <div className="rounded-xl border border-dashed border-[var(--edge)] bg-[var(--paper-soft)] px-4 py-8 text-center text-sm text-[var(--muted)]">No results yet.</div> : null}
          {!search.isLoading && search.results.length > 0 ? (
            <ol className="space-y-3">
              {search.results.map((result, index) => (
                <li key={result.article_id} className="rounded-xl border border-[var(--edge)] bg-[var(--paper-strong)] p-4">
                  <div className="mb-2 flex items-center justify-between gap-3">
                    <span className="font-mono text-xs text-[var(--muted)]">#{index + 1}</span>
                    <span className="rounded-full bg-[var(--accent-soft)] px-2.5 py-1 font-mono text-xs text-[var(--accent)]">score {result.similarity.toFixed(3)}</span>
                  </div>
                  <h3 className="text-base font-semibold text-[var(--ink)]">{result.title ?? "(untitled)"}</h3>
                  <p className="mt-1 text-xs text-[var(--muted)]">{result.source_name} | {result.published_at ? new Date(result.published_at).toLocaleString() : "Publish time unknown"}</p>
                  <a href={result.canonical_url} target="_blank" rel="noreferrer" className="mt-3 inline-block text-sm font-medium text-[var(--accent)] underline">Open article</a>
                </li>
              ))}
            </ol>
          ) : null}
        </section>
      </main>

      <div className="fixed right-4 bottom-4 z-50 sm:right-6 sm:bottom-6">
        {isChatOpen ? (
          <section className="card fade-in-up mb-4 w-[calc(100vw-2rem)] max-w-[390px] overflow-hidden">
            <div className="bg-[linear-gradient(135deg,#3d2512_0%,#88542a_44%,#ff9b54_100%)] px-5 py-4 text-white">
              <div className="flex items-start justify-between gap-3">
                <div>
                  <p className="font-mono text-xs uppercase tracking-[0.22em] text-white/70">
                    Analyst Copilot
                  </p>
                  <h2 className="mt-1 text-lg font-semibold">Ask MarketPulse</h2>
                </div>
                <button
                  type="button"
                  onClick={() => setIsChatOpen(false)}
                  className="rounded-full border border-white/20 bg-white/10 px-2.5 py-1 text-sm font-semibold text-white/86 transition hover:bg-white/16"
                  aria-label="Close chatbot"
                >
                  x
                </button>
              </div>
            </div>

            <div className="max-h-[70vh] overflow-y-auto px-4 py-4">
              <form onSubmit={onChatSubmit} className="grid gap-3">
                <label>
                  <span className="mb-1.5 block text-xs font-semibold uppercase tracking-[0.12em] text-[var(--muted)]">
                    What Do You Have In Mind Today?
                  </span>
                  <textarea
                    value={chatQuery}
                    onChange={(event) => setChatQuery(event.target.value)}
                    rows={4}
                    className="w-full rounded-xl border border-[var(--edge)] bg-[var(--paper-strong)] px-3 py-2.5 text-sm text-[var(--ink)] outline-none transition placeholder:text-[color:rgba(155,177,185,0.7)] focus:border-[var(--warm)] focus:ring-2 focus:ring-[var(--warm-soft)]"
                    placeholder="e.g. Why are yields rising and what is driving the move?"
                  />
                </label>

                <div>
                  <button
                    type="submit"
                    disabled={chat.isLoading}
                    className="inline-flex items-center rounded-full border border-[#b66d34] bg-[var(--warm)] px-4 py-2.5 text-sm font-semibold text-[#2e1304] transition hover:bg-[#ffac70] disabled:cursor-not-allowed disabled:opacity-70"
                  >
                    {chat.isLoading ? "Generating..." : "Ask Chatbot"}
                  </button>
                </div>
              </form>

              <div className="mt-4 space-y-4">
                {chat.error ? (
                  <div className="rounded-xl border border-[color:rgba(255,180,138,0.28)] bg-[var(--danger-soft)] px-4 py-3 text-sm text-[var(--danger)]">
                    {chat.error}
                  </div>
                ) : null}

                {!chat.isLoading && !chat.error && !chat.answer ? (
                  <div className="rounded-xl border border-dashed border-[var(--edge)] bg-[var(--paper-soft)] px-4 py-6 text-center text-sm text-[var(--muted)]">
                    Ask a question to open a grounded macro answer.
                  </div>
                ) : null}

                {!chat.isLoading && chat.answer ? (
                  <div className="space-y-4">
                    <div className="flex items-center justify-between gap-3">
                      <p className="text-xs font-semibold uppercase tracking-[0.12em] text-[var(--muted)]">
                        {chat.lastQuery ? `Question: "${chat.lastQuery}"` : "Latest answer"}
                      </p>
                    </div>

                    <article className="rounded-2xl border border-[color:rgba(255,155,84,0.18)] bg-[linear-gradient(180deg,rgba(255,155,84,0.10),rgba(255,155,84,0.03))] px-4 py-4 text-sm leading-7 whitespace-pre-wrap text-[var(--ink)]">
                      {chat.answer}
                    </article>
                  </div>
                ) : null}
              </div>
            </div>
          </section>
        ) : null}

        <button
          type="button"
          onClick={() => setIsChatOpen((current) => !current)}
          className="flex h-16 w-16 items-center justify-center rounded-full border border-[#b66d34] bg-[linear-gradient(135deg,#88542a_0%,#ff9b54_100%)] text-[#2e1304] shadow-[0_18px_40px_rgba(0,0,0,0.38)] transition hover:scale-[1.03] hover:shadow-[0_22px_50px_rgba(0,0,0,0.42)]"
          aria-label={isChatOpen ? "Hide chatbot" : "Open chatbot"}
        >
          <span className="font-mono text-2xl font-semibold">?</span>
        </button>
      </div>
    </div>
  );
}
