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

type HotTheme = {
  theme_id: string;
  slug: string;
  canonical_label: string;
  trend: string;
  status: string;
  article_count_7d: number;
  hot_score: number;
};

type ThemeOverview = {
  canonical_label: string;
  status: string;
  trend: string;
  total_articles: number;
  source_diversity: number;
  avg_assignment_score: number;
  aliases: string[];
};

type TimelinePoint = {
  bucket_start: string;
  article_count: number;
  source_count: number;
};

type RelatedPayload = {
  related_themes: Array<{
    theme_id: string;
    canonical_label: string;
    relation_score: number;
    evidence_count: number;
  }>;
  developments: Array<{
    article_id: string;
    canonical_url: string;
    title: string | null;
    source_name: string;
    published_at: string | null;
    assignment_method: string;
    assignment_version: string;
    alias_score: number;
    semantic_score: number;
    entity_overlap_score: number;
    asset_overlap_score: number;
    relationship_overlap_score: number;
    margin_score: number;
    assignment_rationale: Record<string, unknown>;
    event: string | null;
  }>;
};

type ThemeRecommendation = {
  recommendation_id: string;
  recommendation_type: "merge" | "split";
  source_label: string;
  target_label: string | null;
  confidence_score: number;
  rationale: string | null;
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

  const [hotThemes, setHotThemes] = useState<HotTheme[]>([]);
  const [themesError, setThemesError] = useState<string | null>(null);
  const [selectedThemeRef, setSelectedThemeRef] = useState<string | null>(null);
  const [themeOverview, setThemeOverview] = useState<ThemeOverview | null>(null);
  const [timeline, setTimeline] = useState<TimelinePoint[]>([]);
  const [related, setRelated] = useState<RelatedPayload | null>(null);
  const [themeDetailsError, setThemeDetailsError] = useState<string | null>(null);
  const [recommendations, setRecommendations] = useState<ThemeRecommendation[]>([]);
  const [recommendationsError, setRecommendationsError] = useState<string | null>(null);

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
    let isActive = true;
    async function loadRecommendations() {
      setRecommendationsError(null);
      try {
        const response = await fetch("/api/themes/recommendations?limit=5&status=suggested", {
          method: "GET",
          headers: { Accept: "application/json" },
          cache: "no-store",
        });
        const payload = await response.json();
        if (!response.ok) throw new Error(String(payload?.detail ?? "Cannot load recommendations."));
        if (!isActive) return;
        setRecommendations(Array.isArray(payload) ? (payload as ThemeRecommendation[]) : []);
      } catch (error) {
        if (!isActive) return;
        setRecommendationsError(error instanceof Error ? error.message : "Cannot load recommendations.");
      }
    }
    loadRecommendations();
    return () => {
      isActive = false;
    };
  }, []);

  useEffect(() => {
    let isActive = true;
    async function loadHotThemes() {
      setThemesError(null);
      try {
        const response = await fetch("/api/themes/hot?limit=8&lookback_days=30", {
          method: "GET",
          headers: { Accept: "application/json" },
          cache: "no-store",
        });
        const payload = await response.json();
        if (!response.ok) {
          throw new Error(String(payload?.detail ?? "Cannot load hot themes."));
        }
        if (!isActive) return;
        const themes = Array.isArray(payload) ? (payload as HotTheme[]) : [];
        setHotThemes(themes);
        if (themes.length > 0) {
          setSelectedThemeRef(themes[0].slug);
        }
      } catch (error) {
        if (isActive) setThemesError(error instanceof Error ? error.message : "Cannot load hot themes.");
      }
    }
    loadHotThemes();
    return () => {
      isActive = false;
    };
  }, []);

  useEffect(() => {
    if (!selectedThemeRef) return;
    const themeRef: string = selectedThemeRef;
    let isActive = true;
    async function loadThemeDetails() {
      setThemeDetailsError(null);
      try {
        const [overviewResponse, timelineResponse, relatedResponse] = await Promise.all([
          fetch(`/api/themes/${encodeURIComponent(themeRef)}`, { cache: "no-store" }),
          fetch(`/api/themes/${encodeURIComponent(themeRef)}/timeline?days=60`, { cache: "no-store" }),
          fetch(`/api/themes/${encodeURIComponent(themeRef)}/related?related_limit=8&development_limit=8`, {
            cache: "no-store",
          }),
        ]);
        const [overviewPayload, timelinePayload, relatedPayload] = await Promise.all([
          overviewResponse.json(),
          timelineResponse.json(),
          relatedResponse.json(),
        ]);
        if (!overviewResponse.ok) throw new Error(String(overviewPayload?.detail ?? "Theme overview failed."));
        if (!timelineResponse.ok) throw new Error(String(timelinePayload?.detail ?? "Theme timeline failed."));
        if (!relatedResponse.ok) throw new Error(String(relatedPayload?.detail ?? "Theme related failed."));
        if (!isActive) return;
        setThemeOverview(overviewPayload as ThemeOverview);
        setTimeline(Array.isArray(timelinePayload) ? (timelinePayload as TimelinePoint[]) : []);
        setRelated(relatedPayload as RelatedPayload);
      } catch (error) {
        if (!isActive) return;
        setThemeDetailsError(error instanceof Error ? error.message : "Cannot load selected theme.");
        setThemeOverview(null);
        setTimeline([]);
        setRelated(null);
      }
    }
    loadThemeDetails();
    return () => {
      isActive = false;
    };
  }, [selectedThemeRef]);

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
            <h1 className="text-3xl font-semibold sm:text-4xl">Theme Intelligence + Semantic Search</h1>
            <p className="mt-3 max-w-2xl text-sm text-white/85 sm:text-base">
              Track hot themes, monitor theme evolution, and inspect supporting articles.
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

        <section className="grid gap-6 lg:grid-cols-[1.7fr_1fr]">
          <div className="card px-5 py-5 sm:px-7 sm:py-6">
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
          </div>

          <div className="space-y-4">
            <section className="card px-5 py-5 sm:px-6">
              <h2 className="text-lg font-semibold text-[var(--ink)]">Hot Themes</h2>
              {themesError ? <p className="mt-2 text-sm text-[var(--danger)]">{themesError}</p> : null}
              {!themesError && hotThemes.length === 0 ? <p className="mt-2 text-sm text-[var(--muted)]">No theme data yet. Run `python backend/scripts/update_themes.py`.</p> : null}
              {hotThemes.length > 0 ? (
                <ul className="mt-3 space-y-2">
                  {hotThemes.map((theme) => (
                    <li key={theme.theme_id}>
                      <button type="button" onClick={() => setSelectedThemeRef(theme.slug)} className={`w-full rounded-lg border px-3 py-2 text-left ${selectedThemeRef === theme.slug ? "border-[var(--accent)] bg-[var(--accent-soft)]" : "border-[var(--edge)] bg-[var(--paper-soft)]"}`}>
                        <div className="flex items-center justify-between gap-2">
                          <span className="text-sm font-semibold text-[var(--ink)]">{theme.canonical_label}</span>
                          <span className="font-mono text-xs text-[var(--muted)]">{theme.hot_score.toFixed(2)}</span>
                        </div>
                        <p className="mt-1 text-xs text-[var(--muted)]">{theme.trend} | {theme.article_count_7d} articles / 7d</p>
                      </button>
                    </li>
                  ))}
                </ul>
              ) : null}
            </section>

            <section className="card px-5 py-5 sm:px-6">
              <h2 className="text-lg font-semibold text-[var(--ink)]">Theme Evolution</h2>
              {themeDetailsError ? <p className="mt-2 text-sm text-[var(--danger)]">{themeDetailsError}</p> : null}
              {themeOverview ? (
                <div className="mt-3 space-y-2">
                  <p className="text-sm font-semibold text-[var(--ink)]">{themeOverview.canonical_label}</p>
                  <p className="text-xs text-[var(--muted)]">{themeOverview.status} | trend: {themeOverview.trend}</p>
                  <p className="text-xs text-[var(--muted)]">{themeOverview.total_articles} linked articles | {themeOverview.source_diversity} sources | avg {themeOverview.avg_assignment_score.toFixed(2)}</p>
                  {themeOverview.aliases.length > 0 ? <p className="text-xs text-[var(--muted)]">aliases: {themeOverview.aliases.slice(0, 5).join(", ")}</p> : null}
                  {timeline.length > 0 ? (
                    <div className="rounded-lg border border-[var(--edge)] bg-[var(--paper-soft)] p-2.5">
                      {timeline.slice(Math.max(0, timeline.length - 8)).map((point) => (
                        <div key={point.bucket_start} className="flex items-center justify-between text-xs">
                          <span className="font-mono text-[var(--muted)]">{new Date(point.bucket_start).toLocaleDateString()}</span>
                          <span className="text-[var(--ink)]">{point.article_count} articles, {point.source_count} sources</span>
                        </div>
                      ))}
                    </div>
                  ) : null}
                </div>
              ) : <p className="mt-2 text-sm text-[var(--muted)]">Select a hot theme to inspect its timeline.</p>}
            </section>

            <section className="card px-5 py-5 sm:px-6">
              <h2 className="text-lg font-semibold text-[var(--ink)]">Related Developments</h2>
              {!related ? <p className="mt-2 text-sm text-[var(--muted)]">No linked developments yet.</p> : (
                <div className="mt-3 space-y-3">
                  {related.related_themes.slice(0, 4).map((theme) => (
                    <div key={theme.theme_id} className="rounded-lg bg-[var(--paper-soft)] px-3 py-2 text-xs">
                      <div className="flex items-center justify-between gap-2 text-[var(--ink)]">
                        <span>{theme.canonical_label}</span>
                        <span className="font-mono text-[var(--muted)]">{theme.relation_score.toFixed(1)}</span>
                      </div>
                      <p className="text-[var(--muted)]">evidence {theme.evidence_count}</p>
                    </div>
                  ))}
                  {related.developments.slice(0, 4).map((item) => (
                    <div key={item.article_id} className="rounded-lg border border-[var(--edge)] bg-[var(--paper-soft)] p-2.5">
                      <a href={item.canonical_url} target="_blank" rel="noreferrer" className="text-sm font-medium text-[var(--ink)] underline">{item.title ?? "(untitled)"}</a>
                      <p className="mt-1 text-xs text-[var(--muted)]">{item.source_name} | {item.published_at ? new Date(item.published_at).toLocaleString() : "Unknown time"}</p>
                      {item.event ? <p className="mt-1 text-xs text-[var(--muted)]">{item.event}</p> : null}
                      <p className="mt-1 text-[11px] text-[var(--muted)]">
                        why: {item.assignment_method} | alias {item.alias_score.toFixed(2)} | semantic {item.semantic_score.toFixed(2)} | entity {item.entity_overlap_score.toFixed(2)} | margin {item.margin_score.toFixed(2)}
                      </p>
                    </div>
                  ))}
                </div>
              )}
            </section>

            <section className="card px-5 py-5 sm:px-6">
              <h2 className="text-lg font-semibold text-[var(--ink)]">Theme Maintenance</h2>
              {recommendationsError ? <p className="mt-2 text-sm text-[var(--danger)]">{recommendationsError}</p> : null}
              {!recommendationsError && recommendations.length === 0 ? <p className="mt-2 text-sm text-[var(--muted)]">No maintenance suggestions right now.</p> : null}
              {recommendations.length > 0 ? (
                <div className="mt-3 space-y-2">
                  {recommendations.map((item) => (
                    <div key={item.recommendation_id} className="rounded-lg border border-[var(--edge)] bg-[var(--paper-soft)] p-2.5 text-xs">
                      <p className="font-semibold text-[var(--ink)]">
                        {item.recommendation_type === "merge" ? "Merge" : "Split"}: {item.source_label}
                        {item.target_label ? ` -> ${item.target_label}` : ""}
                      </p>
                      <p className="mt-0.5 text-[var(--muted)]">confidence {item.confidence_score.toFixed(2)}</p>
                      {item.rationale ? <p className="mt-1 text-[var(--muted)]">{item.rationale}</p> : null}
                    </div>
                  ))}
                </div>
              ) : null}
            </section>
          </div>
        </section>
      </main>
    </div>
  );
}
