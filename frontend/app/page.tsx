"use client";

import { FormEvent, KeyboardEvent, useCallback, useEffect, useMemo, useRef, useState } from "react";
import Link from "next/link";
import AuthModal from "./components/AuthModal";
import { useAuth } from "./contexts/AuthContext";

type ChatSource = {
  index: number;
  article_id: string;
  canonical_url: string;
  title: string | null;
  published_at: string | null;
  source_name: string;
  similarity: number;
};

type ChatRole = "user" | "assistant";

type ChatMessage = {
  id: string;
  role: ChatRole;
  content: string;
  sources: ChatSource[];
  status: "loading" | "complete" | "error";
};

type ChatState = {
  isLoading: boolean;
  error: string | null;
  messages: ChatMessage[];
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

type HotTheme = {
  id: string;
  slug: string;
  canonical_label: string;
  status: string;
  article_count: number;
  last_seen_at: string | null;
  updated_at: string | null;
};

type HotThemeState = {
  isLoading: boolean;
  error: string | null;
  items: HotTheme[];
};

type WatchlistTheme = {
  id: string;
  slug: string;
  canonical_label: string;
  summary: string | null;
  status: string;
  discovery_method: string;
  scope: string;
  owner_user_id: string | null;
  article_count: number;
  first_seen_at: string | null;
  last_seen_at: string | null;
  created_at: string | null;
  updated_at: string | null;
  alerts_enabled: boolean | null;
  watchlisted_at: string | null;
};

type WatchlistThemeArticle = {
  article_id: string;
  canonical_url: string;
  title: string | null;
  description: string | null;
  published_at: string | null;
  source_name: string;
  similarity_score: number;
  assignment_score: number;
  assignment_method: string;
  matched_at: string | null;
};

type WatchlistThemeArticlesState = {
  isLoading: boolean;
  isOpen: boolean;
  hasLoaded: boolean;
  error: string | null;
  items: WatchlistThemeArticle[];
};

type WatchlistState = {
  isLoading: boolean;
  error: string | null;
  items: WatchlistTheme[];
};

const DEFAULT_LIMIT = 8;
const MAX_LIMIT = 20;

function createMessageId(prefix: string) {
  return `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

export default function Home() {
  const [isChatOpen, setIsChatOpen] = useState(false);
  const [chatInput, setChatInput] = useState("");
  const { user, logout, isLoading: authLoading } = useAuth();
  const [isAuthModalOpen, setIsAuthModalOpen] = useState(false);
  const [keywords, setKeywords] = useState("");
  const [selectedSources, setSelectedSources] = useState<string[]>([]);
  const [sourceInput, setSourceInput] = useState("");
  const [sourceOptions, setSourceOptions] = useState<SourceOption[]>([]);
  const [isSourcesLoading, setIsSourcesLoading] = useState(false);
  const [sourcesError, setSourcesError] = useState<string | null>(null);
  const [isSourceDropdownOpen, setIsSourceDropdownOpen] = useState(false);
  const [minPublishedAt, setMinPublishedAt] = useState("");
  const [maxPublishedAt, setMaxPublishedAt] = useState("");
  const [limit, setLimit] = useState(DEFAULT_LIMIT);
  const [chat, setChat] = useState<ChatState>({
    isLoading: false,
    error: null,
    messages: [],
  });
  const [search, setSearch] = useState<SearchState>({
    isLoading: false,
    error: null,
    results: [],
    lastQuery: "",
  });
  const [hotThemes, setHotThemes] = useState<HotThemeState>({
    isLoading: false,
    error: null,
    items: [],
  });
  const [watchlist, setWatchlist] = useState<WatchlistState>({
    isLoading: false,
    error: null,
    items: [],
  });
  const [watchlistArticles, setWatchlistArticles] = useState<Record<string, WatchlistThemeArticlesState>>({});
  const [watchlistActionThemeId, setWatchlistActionThemeId] = useState<string | null>(null);
  const [isCreatingCustomWatchlistTheme, setIsCreatingCustomWatchlistTheme] = useState(false);
  const [watchlistFormError, setWatchlistFormError] = useState<string | null>(null);
  const [customWatchlistLabel, setCustomWatchlistLabel] = useState("");
  const [customWatchlistDescription, setCustomWatchlistDescription] = useState("");
  const [isFiltersOpen, setIsFiltersOpen] = useState(false);
  const [isResultsCardHidden, setIsResultsCardHidden] = useState(false);
  const sourceDropdownRef = useRef<HTMLDivElement | null>(null);
  const chatTextareaRef = useRef<HTMLTextAreaElement | null>(null);
  const latestAssistantMessageRef = useRef<HTMLDivElement | null>(null);

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
    async function loadHotThemes() {
      setHotThemes({ isLoading: true, error: null, items: [] });
      try {
        const response = await fetch("/api/themes/hot?limit=10", {
          method: "GET",
          headers: { Accept: "application/json" },
          cache: "no-store",
        });
        const payload = await response.json();
        if (!response.ok) {
          throw new Error(String(payload?.detail ?? "Cannot load hot themes."));
        }
        if (isActive) {
          setHotThemes({
            isLoading: false,
            error: null,
            items: Array.isArray(payload) ? payload : [],
          });
        }
      } catch (error) {
        if (isActive) {
          setHotThemes({
            isLoading: false,
            error: error instanceof Error ? error.message : "Cannot load hot themes.",
            items: [],
          });
        }
      }
    }
    loadHotThemes();
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

  useEffect(() => {
    if (!isChatOpen) return;
    chatTextareaRef.current?.focus();
  }, [isChatOpen]);

  useEffect(() => {
    if (!isChatOpen) return;
    latestAssistantMessageRef.current?.scrollIntoView({
      block: "start",
      behavior: "smooth",
    });
  }, [chat.messages, chat.isLoading, isChatOpen]);

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

  const filteredSearchResults = useMemo(() => {
    if (!maxPublishedAt) return search.results;
    const maxTimestamp = new Date(maxPublishedAt).getTime();
    if (Number.isNaN(maxTimestamp)) return search.results;
    return search.results.filter((result) => {
      if (!result.published_at) return false;
      const publishedTimestamp = new Date(result.published_at).getTime();
      return !Number.isNaN(publishedTimestamp) && publishedTimestamp <= maxTimestamp;
    });
  }, [maxPublishedAt, search.results]);

  const topScore = useMemo(() => {
    if (filteredSearchResults.length === 0) return 0;
    return Math.max(...filteredSearchResults.map((result) => result.similarity));
  }, [filteredSearchResults]);
  const hasSearched = search.lastQuery.trim().length > 0;

  const watchlistThemeIds = useMemo(() => new Set(watchlist.items.map((theme) => theme.id)), [watchlist.items]);

  const buildAuthHeaders = useCallback((includeContentType: boolean): Record<string, string> | null => {
    const token = typeof window === "undefined" ? null : localStorage.getItem("token");
    if (!token) return null;
    const headers: Record<string, string> = {
      Accept: "application/json",
      Authorization: `Bearer ${token}`,
    };
    if (includeContentType) headers["Content-Type"] = "application/json";
    return headers;
  }, []);

  const loadWatchlistThemes = useCallback(async () => {
    const headers = buildAuthHeaders(false);
    if (!headers) {
      setWatchlist({ isLoading: false, error: "Please sign in to use watchlist.", items: [] });
      return;
    }
    setWatchlist((prev) => ({ ...prev, isLoading: true, error: null }));
    try {
      const response = await fetch("/api/watchlist/themes?limit=50", {
        method: "GET",
        headers,
        cache: "no-store",
      });
      const payload = await response.json();
      if (!response.ok) {
        throw new Error(String(payload?.detail ?? "Cannot load watchlist themes."));
      }
      setWatchlist({
        isLoading: false,
        error: null,
        items: Array.isArray(payload) ? payload : [],
      });
    } catch (error) {
      setWatchlist({
        isLoading: false,
        error: error instanceof Error ? error.message : "Cannot load watchlist themes.",
        items: [],
      });
    }
  }, [buildAuthHeaders]);

  useEffect(() => {
    if (!user) {
      setWatchlist({ isLoading: false, error: null, items: [] });
      setWatchlistArticles({});
      return;
    }
    void loadWatchlistThemes();
  }, [user, loadWatchlistThemes]);

  async function addThemeToWatchlist(themeId: string) {
    if (!user) {
      setIsAuthModalOpen(true);
      return;
    }
    const headers = buildAuthHeaders(true);
    if (!headers) {
      setIsAuthModalOpen(true);
      return;
    }
    setWatchlistActionThemeId(themeId);
    setWatchlistFormError(null);
    try {
      const response = await fetch("/api/watchlist/themes", {
        method: "POST",
        headers,
        body: JSON.stringify({ theme_id: themeId, alerts_enabled: true }),
      });
      const payload = await response.json();
      if (!response.ok) {
        throw new Error(String(payload?.detail ?? "Cannot add theme to watchlist."));
      }
      await loadWatchlistThemes();
    } catch (error) {
      setWatchlistFormError(error instanceof Error ? error.message : "Cannot add theme to watchlist.");
    } finally {
      setWatchlistActionThemeId(null);
    }
  }

  async function createCustomWatchlistTheme(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!user) {
      setIsAuthModalOpen(true);
      return;
    }
    const headers = buildAuthHeaders(true);
    if (!headers) {
      setIsAuthModalOpen(true);
      return;
    }
    const canonicalLabel = customWatchlistLabel.trim();
    if (!canonicalLabel) {
      setWatchlistFormError("Custom theme label is required.");
      return;
    }

    const payload: {
      canonical_label: string;
      description?: string;
      alerts_enabled: boolean;
    } = {
      canonical_label: canonicalLabel,
      alerts_enabled: true,
    };

    const normalizedDescription = customWatchlistDescription.trim();
    if (normalizedDescription) payload.description = normalizedDescription;

    setIsCreatingCustomWatchlistTheme(true);
    setWatchlistFormError(null);
    try {
      const response = await fetch("/api/watchlist/themes", {
        method: "POST",
        headers,
        body: JSON.stringify(payload),
      });
      const data = await response.json();
      if (!response.ok) {
        throw new Error(String(data?.detail ?? "Cannot create custom watchlist theme."));
      }
      setCustomWatchlistLabel("");
      setCustomWatchlistDescription("");
      await loadWatchlistThemes();
    } catch (error) {
      setWatchlistFormError(error instanceof Error ? error.message : "Cannot create custom watchlist theme.");
    } finally {
      setIsCreatingCustomWatchlistTheme(false);
    }
  }

  async function removeWatchlistTheme(themeId: string) {
    const headers = buildAuthHeaders(false);
    if (!headers) {
      setIsAuthModalOpen(true);
      return;
    }
    setWatchlistActionThemeId(themeId);
    setWatchlistFormError(null);
    try {
      const response = await fetch(`/api/watchlist/themes/${encodeURIComponent(themeId)}`, {
        method: "DELETE",
        headers,
      });
      const payload = await response.json();
      if (!response.ok) {
        throw new Error(String(payload?.detail ?? "Cannot remove watchlist theme."));
      }
      setWatchlist((prev) => ({
        ...prev,
        items: prev.items.filter((item) => item.id !== themeId),
      }));
      setWatchlistArticles((prev) => {
        const next = { ...prev };
        delete next[themeId];
        return next;
      });
    } catch (error) {
      setWatchlistFormError(error instanceof Error ? error.message : "Cannot remove watchlist theme.");
    } finally {
      setWatchlistActionThemeId(null);
    }
  }

  async function toggleWatchlistThemeArticles(themeId: string) {
    const current = watchlistArticles[themeId];
    if (current?.isOpen) {
      setWatchlistArticles((prev) => ({
        ...prev,
        [themeId]: { ...prev[themeId], isOpen: false },
      }));
      return;
    }

    if (current?.hasLoaded) {
      setWatchlistArticles((prev) => ({
        ...prev,
        [themeId]: { ...prev[themeId], isOpen: true },
      }));
      return;
    }

    const headers = buildAuthHeaders(false);
    if (!headers) {
      setIsAuthModalOpen(true);
      return;
    }
    setWatchlistArticles((prev) => ({
      ...prev,
      [themeId]: {
        isLoading: true,
        isOpen: true,
        hasLoaded: false,
        error: null,
        items: [],
      },
    }));
    try {
      const response = await fetch(`/api/watchlist/themes/${encodeURIComponent(themeId)}/articles?limit=30`, {
        method: "GET",
        headers,
        cache: "no-store",
      });
      const payload = await response.json();
      if (!response.ok) {
        throw new Error(String(payload?.detail ?? "Cannot load watchlist articles."));
      }
      setWatchlistArticles((prev) => ({
        ...prev,
        [themeId]: {
          isLoading: false,
          isOpen: true,
          hasLoaded: true,
          error: null,
          items: Array.isArray(payload) ? payload : [],
        },
      }));
    } catch (error) {
      setWatchlistArticles((prev) => ({
        ...prev,
        [themeId]: {
          isLoading: false,
          isOpen: true,
          hasLoaded: true,
          error: error instanceof Error ? error.message : "Cannot load watchlist articles.",
          items: [],
        },
      }));
    }
  }

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

  function clearChat() {
    setChat({
      isLoading: false,
      error: null,
      messages: [],
    });
    setChatInput("");
    chatTextareaRef.current?.focus();
  }

  async function submitChatMessage() {
    const trimmedQuery = chatInput.trim();
    if (!trimmedQuery) {
      setChat((prev) => ({ ...prev, error: "Please enter a macro question." }));
      return;
    }

    const userMessage: ChatMessage = {
      id: createMessageId("user"),
      role: "user",
      content: trimmedQuery,
      sources: [],
      status: "complete",
    };
    const assistantMessageId = createMessageId("assistant");
    const assistantPlaceholder: ChatMessage = {
      id: assistantMessageId,
      role: "assistant",
      content: "",
      sources: [],
      status: "loading",
    };
    const conversationHistory = chat.messages
      .filter((message) => message.status === "complete")
      .map((message) => ({
        role: message.role,
        content: message.content,
      }));

    setChatInput("");
    setChat((prev) => ({
      ...prev,
      isLoading: true,
      error: null,
      messages: [...prev.messages, userMessage, assistantPlaceholder],
    }));

    const payload: {
      query: string;
      retrieval_limit: number;
      min_published_at?: string;
      source_names?: string[];
      conversation_history?: Array<{ role: ChatRole; content: string }>;
    } = {
      query: trimmedQuery,
      retrieval_limit: Math.min(limit, 5),
      conversation_history: conversationHistory,
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
        messages: prev.messages.map((message) =>
          message.id === assistantMessageId
            ? {
                ...message,
                content: typeof result?.answer === "string" ? result.answer : "",
                sources: Array.isArray(result?.sources) ? result.sources : [],
                status: "complete",
              }
            : message,
        ),
      }));
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unknown error occurred.";
      setChat((prev) => ({
        ...prev,
        isLoading: false,
        error: message,
        messages: prev.messages.map((entry) =>
          entry.id === assistantMessageId
            ? {
                ...entry,
                content: message,
                status: "error",
              }
            : entry,
        ),
      }));
    }
  }

  async function onChatSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    await submitChatMessage();
  }

  function onChatInputKeyDown(event: KeyboardEvent<HTMLTextAreaElement>) {
    if (event.key !== "Enter" || event.shiftKey) return;
    event.preventDefault();
    if (chat.isLoading) return;
    void submitChatMessage();
  }

  async function onSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const trimmedKeywords = keywords.trim();
    if (!trimmedKeywords) {
      setSearch((prev) => ({ ...prev, error: "Please enter at least one keyword." }));
      return;
    }
    setIsResultsCardHidden(false);
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
      <main id="home" className="mx-auto w-full max-w-6xl px-4 py-8 sm:px-6 sm:py-10 lg:px-8">
        <section id="semantic-search" className="card mb-6">
          <div className="bg-[linear-gradient(120deg,#123c46_0%,#1f9d85_52%,#35c5aa_100%)] px-6 py-7 text-white sm:px-8">
            <div className="mb-4 flex flex-wrap items-start justify-between gap-3">
              <div>
                <h1 className="mt-1 text-3xl font-semibold sm:text-4xl">MarketPulse</h1>
                <div className="mt-3 flex flex-wrap items-center gap-2">
                  <a
                    href="#home"
                    className="rounded-full border border-white/28 bg-white/10 px-3 py-1 text-xs font-semibold uppercase tracking-[0.08em] text-white/92"
                  >
                    Home
                  </a>
                  <Link
                    href="/chatbot"
                    className="rounded-full border border-white/28 bg-white/10 px-3 py-1 text-xs font-semibold uppercase tracking-[0.08em] text-white/92"
                  >
                    Chatbot
                  </Link>
                </div>
              </div>
              <div className="flex items-center gap-2">
                {authLoading ? (
                  <div className="text-sm text-white/72">Loading...</div>
                ) : user ? (
                  <div className="flex items-center gap-2">
                    <span className="text-sm text-white/85">Welcome, {user.email}</span>
                    <button
                      onClick={logout}
                      className="rounded px-3 py-1 text-sm transition hover:bg-white/30 bg-white/20"
                    >
                      Sign Out
                    </button>
                  </div>
                ) : (
                  <button
                    onClick={() => setIsAuthModalOpen(true)}
                    className="rounded px-3 py-1 text-sm transition hover:bg-white/30 bg-white/20"
                  >
                    Sign In
                  </button>
                )}
              </div>
            </div>
            <p className="max-w-2xl text-sm leading-relaxed text-white/85 sm:text-base">
              Search themes semantically with a date/time range, monitor your profile watchlist, and track hot market themes.
            </p>
          </div>
        </section>

        <section className="card mb-6">
          <form onSubmit={onSubmit} className="grid gap-4 px-5 py-5 sm:grid-cols-2 sm:px-8 sm:py-7">
            <div className="sm:col-span-2">
              <div className="flex flex-col gap-2 sm:flex-row">
                <input
                  value={keywords}
                  onChange={(event) => setKeywords(event.target.value)}
                  placeholder="Search articles semantically..."
                  className="flex-1 rounded-xl border border-[var(--edge)] bg-[var(--paper-strong)] px-3 py-2.5 text-sm text-[var(--ink)] outline-none focus:border-[var(--accent)] focus:ring-2 focus:ring-[var(--accent-soft)]"
                />
                <button
                  type="button"
                  onClick={() =>
                    setIsFiltersOpen((current) => {
                      const next = !current;
                      if (!next) setIsSourceDropdownOpen(false);
                      return next;
                    })
                  }
                  className="inline-flex items-center justify-center rounded-full border border-[var(--edge)] bg-[var(--paper-soft)] px-4 py-2 text-xs font-semibold uppercase tracking-[0.08em] text-[var(--ink)]"
                >
                  {isFiltersOpen ? "Hide Filters" : "Filters"}
                </button>
                <button
                  type="submit"
                  disabled={search.isLoading}
                  className="inline-flex items-center justify-center rounded-full border border-[var(--accent-strong)] bg-[var(--accent)] px-5 py-2.5 text-sm font-semibold text-[#052018]"
                >
                  {search.isLoading ? "Searching..." : "Search"}
                </button>
              </div>
            </div>
            {isFiltersOpen ? (
              <>
                <div ref={sourceDropdownRef} className="relative">
                  <span className="mb-1.5 block text-xs font-semibold uppercase tracking-[0.12em] text-[var(--muted)]">
                    Source Filter
                  </span>
                  <div className="min-h-[44px] w-full rounded-xl border border-[var(--edge)] bg-[var(--paper-strong)] px-3 py-2 text-sm text-[var(--ink)]">
                    <div className="flex flex-wrap items-center gap-1.5">
                      {selectedSources.map((source) => (
                        <span
                          key={source}
                          className="inline-flex items-center gap-1 rounded-full border border-[var(--edge)] bg-[var(--paper-soft)] px-2 py-0.5 text-xs text-[var(--ink)]"
                        >
                          <span>{source}</span>
                          <button
                            type="button"
                            onClick={() => removeSource(source)}
                            className="font-semibold leading-none text-[var(--muted)]"
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
                        className="min-w-[180px] flex-1 bg-transparent py-0.5 text-sm text-[var(--ink)] outline-none"
                        placeholder="Type to search sources..."
                      />
                    </div>
                  </div>
                  {isSourceDropdownOpen ? (
                    <div className="mt-1.5 max-h-48 w-full overflow-y-auto rounded-xl border border-[var(--edge)] bg-[var(--paper-strong)] p-1.5 shadow-lg">
                      {isSourcesLoading ? <p className="px-2 py-2 text-sm text-[var(--muted)]">Loading sources...</p> : null}
                      {!isSourcesLoading && sourcesError ? (
                        <p className="px-2 py-2 text-sm text-[var(--danger)]">{sourcesError}</p>
                      ) : null}
                      {!isSourcesLoading && !sourcesError && filteredSourceOptions.map((option) => (
                        <button
                          key={option.source_name}
                          type="button"
                          onClick={() => addSource(option.source_name)}
                          className="mt-1 flex w-full items-center justify-between rounded-lg px-2 py-1.5 text-left text-sm text-[var(--ink)] hover:bg-[var(--paper-soft)]"
                        >
                          <span>{option.source_name}</span>
                          <span className="font-mono text-xs text-[var(--muted)]">{option.article_count}</span>
                        </button>
                      ))}
                    </div>
                  ) : null}
                </div>
                <div className="space-y-3">
                  <label className="block">
                    <span className="mb-1.5 block text-xs font-semibold uppercase tracking-[0.12em] text-[var(--muted)]">
                      Start Date/Time
                    </span>
                    <input
                      type="datetime-local"
                      value={minPublishedAt}
                      onChange={(event) => setMinPublishedAt(event.target.value)}
                      className="w-full rounded-xl border border-[var(--edge)] bg-[var(--paper-strong)] px-3 py-2 text-sm text-[var(--ink)] outline-none focus:border-[var(--accent)] focus:ring-2 focus:ring-[var(--accent-soft)]"
                    />
                  </label>
                  <label className="block">
                    <span className="mb-1.5 block text-xs font-semibold uppercase tracking-[0.12em] text-[var(--muted)]">
                      End Date/Time
                    </span>
                    <input
                      type="datetime-local"
                      value={maxPublishedAt}
                      onChange={(event) => setMaxPublishedAt(event.target.value)}
                      className="w-full rounded-xl border border-[var(--edge)] bg-[var(--paper-strong)] px-3 py-2 text-sm text-[var(--ink)] outline-none focus:border-[var(--accent)] focus:ring-2 focus:ring-[var(--accent-soft)]"
                    />
                  </label>
                </div>
                <label className="sm:col-span-2">
                  <span className="mb-1.5 block text-xs font-semibold uppercase tracking-[0.12em] text-[var(--muted)]">
                    Result Limit: {limit}
                  </span>
                  <input
                    type="range"
                    min={1}
                    max={MAX_LIMIT}
                    value={limit}
                    onChange={(event) => setLimit(Number(event.target.value))}
                    className="w-full accent-[var(--accent)]"
                  />
                </label>
              </>
            ) : null}
          </form>
        </section>

        {hasSearched && !isResultsCardHidden ? (
          <section className="card mb-6 px-5 py-5 sm:px-7 sm:py-6">
            <div className="mb-4 flex items-center justify-between gap-3">
              <h2 className="text-xl font-semibold text-[var(--ink)]">Ranked Results</h2>
              <div className="flex items-center gap-2">
                <span className="rounded-full bg-[var(--warm-soft)] px-3 py-1 font-mono text-xs text-[var(--warm)]">
                  Top score: {topScore.toFixed(3)}
                </span>
                <button
                  type="button"
                  onClick={() => setIsResultsCardHidden(true)}
                  className="inline-flex h-7 w-7 items-center justify-center rounded-full border border-[var(--edge)] bg-[var(--paper-soft)] text-sm font-semibold text-[var(--muted)]"
                  aria-label="Hide ranked results"
                >
                  x
                </button>
              </div>
            </div>
            {search.error ? (
              <div className="rounded-xl border border-[color:rgba(255,180,138,0.28)] bg-[var(--danger-soft)] px-4 py-3 text-sm text-[var(--danger)]">
                {search.error}
              </div>
            ) : null}
            {!search.isLoading && filteredSearchResults.length === 0 ? (
              <div className="rounded-xl border border-dashed border-[var(--edge)] bg-[var(--paper-soft)] px-4 py-8 text-center text-sm text-[var(--muted)]">
                No results yet.
              </div>
            ) : null}
            {!search.isLoading && filteredSearchResults.length > 0 ? (
              <ol className="space-y-3">
                {filteredSearchResults.map((result, index) => (
                  <li key={result.article_id} className="rounded-xl border border-[var(--edge)] bg-[var(--paper-strong)] p-4">
                    <div className="mb-2 flex items-center justify-between gap-3">
                      <span className="font-mono text-xs text-[var(--muted)]">#{index + 1}</span>
                      <span className="rounded-full bg-[var(--accent-soft)] px-2.5 py-1 font-mono text-xs text-[var(--accent)]">
                        score {result.similarity.toFixed(3)}
                      </span>
                    </div>
                    <h3 className="text-base font-semibold text-[var(--ink)]">{result.title ?? "(untitled)"}</h3>
                    <p className="mt-1 text-xs text-[var(--muted)]">
                      {result.source_name} |{" "}
                      {result.published_at ? new Date(result.published_at).toLocaleString() : "Publish time unknown"}
                    </p>
                    <a
                      href={result.canonical_url}
                      target="_blank"
                      rel="noreferrer"
                      className="mt-3 inline-block text-sm font-medium text-[var(--accent)] underline"
                    >
                      Open article
                    </a>
                  </li>
                ))}
              </ol>
            ) : null}
          </section>
        ) : null}

        <section className="mb-6 grid gap-6 lg:grid-cols-3">
          <article className="card px-4 py-4 sm:px-5 sm:py-5 lg:order-2 lg:col-span-1">
            <div className="mb-4 flex items-center justify-between gap-3">
              <h2 className="text-xl font-semibold text-[var(--ink)]">Watchlist (Profile)</h2>
              {user ? (
                <button
                  type="button"
                  onClick={() => void loadWatchlistThemes()}
                  disabled={watchlist.isLoading}
                  className="rounded-full border border-[var(--edge)] bg-[var(--paper-soft)] px-3 py-1.5 text-xs font-semibold uppercase tracking-[0.08em] text-[var(--ink)] disabled:cursor-not-allowed disabled:opacity-60"
                >
                  {watchlist.isLoading ? "Refreshing..." : "Refresh"}
                </button>
              ) : null}
            </div>

            {!user ? (
              <div className="rounded-xl border border-dashed border-[var(--edge)] bg-[var(--paper-soft)] px-4 py-6">
                <p className="text-sm text-[var(--muted)]">Sign in to use your profile watchlist.</p>
                <button
                  type="button"
                  onClick={() => setIsAuthModalOpen(true)}
                  className="mt-3 inline-flex items-center rounded-full border border-[var(--accent-strong)] bg-[var(--accent)] px-4 py-2 text-xs font-semibold uppercase tracking-[0.08em] text-[#052018]"
                >
                  Sign In
                </button>
              </div>
            ) : (
              <>
                <form onSubmit={createCustomWatchlistTheme} className="mb-4 grid gap-3 rounded-xl border border-[var(--edge)] bg-[var(--paper-soft)] p-4">
                  <label>
                    <span className="mb-1.5 block text-xs font-semibold uppercase tracking-[0.12em] text-[var(--muted)]">
                      Add Custom Theme
                    </span>
                    <input
                      value={customWatchlistLabel}
                      onChange={(event) => setCustomWatchlistLabel(event.target.value)}
                      placeholder="Theme name"
                      className="w-full rounded-xl border border-[var(--edge)] bg-[var(--paper-strong)] px-3 py-2 text-sm text-[var(--ink)] outline-none focus:border-[var(--accent)] focus:ring-2 focus:ring-[var(--accent-soft)]"
                    />
                  </label>
                  <input
                    value={customWatchlistDescription}
                    onChange={(event) => setCustomWatchlistDescription(event.target.value)}
                    placeholder="Description (optional)"
                    className="rounded-xl border border-[var(--edge)] bg-[var(--paper-strong)] px-3 py-2 text-sm text-[var(--ink)] outline-none focus:border-[var(--accent)] focus:ring-2 focus:ring-[var(--accent-soft)]"
                  />
                  <button
                    type="submit"
                    disabled={isCreatingCustomWatchlistTheme}
                    className="inline-flex items-center rounded-full border border-[var(--accent-strong)] bg-[var(--accent)] px-4 py-2 text-xs font-semibold uppercase tracking-[0.08em] text-[#052018] disabled:cursor-not-allowed disabled:opacity-60"
                  >
                    {isCreatingCustomWatchlistTheme ? "Creating..." : "Create + Watch"}
                  </button>
                </form>

                {watchlistFormError ? (
                  <div className="mb-4 rounded-xl border border-[color:rgba(255,180,138,0.28)] bg-[var(--danger-soft)] px-4 py-3 text-sm text-[var(--danger)]">
                    {watchlistFormError}
                  </div>
                ) : null}
                {watchlist.error ? (
                  <div className="mb-4 rounded-xl border border-[color:rgba(255,180,138,0.28)] bg-[var(--danger-soft)] px-4 py-3 text-sm text-[var(--danger)]">
                    {watchlist.error}
                  </div>
                ) : null}
                {watchlist.isLoading ? (
                  <div className="rounded-xl border border-dashed border-[var(--edge)] bg-[var(--paper-soft)] px-4 py-8 text-center text-sm text-[var(--muted)]">
                    Loading watchlist...
                  </div>
                ) : null}
                {!watchlist.isLoading && watchlist.items.length === 0 ? (
                  <div className="rounded-xl border border-dashed border-[var(--edge)] bg-[var(--paper-soft)] px-4 py-8 text-center text-sm text-[var(--muted)]">
                    Your watchlist is empty.
                  </div>
                ) : null}
                {!watchlist.isLoading && watchlist.items.length > 0 ? (
                  <ol className="space-y-3">
                    {watchlist.items.map((theme) => {
                      const articleState = watchlistArticles[theme.id];
                      return (
                        <li key={theme.id} className="rounded-xl border border-[var(--edge)] bg-[var(--paper-strong)] p-4">
                          <div className="mb-1 flex items-center justify-between gap-3">
                            <h3 className="text-base font-semibold text-[var(--ink)]">{theme.canonical_label}</h3>
                            <span className="rounded-full bg-[var(--warm-soft)] px-2.5 py-1 font-mono text-xs text-[var(--warm)]">
                              {theme.article_count}
                            </span>
                          </div>
                          <p className="text-xs text-[var(--muted)]">{theme.discovery_method} | {theme.status}</p>
                          <div className="mt-3 flex flex-wrap items-center gap-2">
                            <button
                              type="button"
                              onClick={() => void toggleWatchlistThemeArticles(theme.id)}
                              className="inline-flex items-center rounded-full border border-[var(--edge)] bg-[var(--paper-soft)] px-3 py-1.5 text-xs font-semibold uppercase tracking-[0.08em] text-[var(--ink)]"
                            >
                              {articleState?.isOpen ? "Hide Articles" : "View Articles"}
                            </button>
                            <button
                              type="button"
                              onClick={() => void removeWatchlistTheme(theme.id)}
                              disabled={watchlistActionThemeId === theme.id}
                              className="inline-flex items-center rounded-full border border-[color:rgba(255,180,138,0.28)] bg-[var(--danger-soft)] px-3 py-1.5 text-xs font-semibold uppercase tracking-[0.08em] text-[var(--danger)] disabled:cursor-not-allowed disabled:opacity-60"
                            >
                              {watchlistActionThemeId === theme.id ? "Removing..." : "Remove"}
                            </button>
                          </div>
                          {articleState?.isOpen ? (
                            <div className="mt-3 rounded-lg border border-[var(--edge)] bg-[var(--paper-soft)] p-3">
                              {articleState.isLoading ? (
                                <p className="text-sm text-[var(--muted)]">Loading watchlist articles...</p>
                              ) : null}
                              {articleState.error ? (
                                <p className="text-sm text-[var(--danger)]">{articleState.error}</p>
                              ) : null}
                              {!articleState.isLoading && !articleState.error && articleState.items.length === 0 ? (
                                <p className="text-sm text-[var(--muted)]">No linked articles yet.</p>
                              ) : null}
                              {!articleState.isLoading && !articleState.error && articleState.items.length > 0 ? (
                                <ul className="space-y-2">
                                  {articleState.items.slice(0, 5).map((article) => (
                                    <li key={article.article_id}>
                                      <a
                                        href={article.canonical_url}
                                        target="_blank"
                                        rel="noreferrer"
                                        className="text-sm font-medium text-[var(--accent)] underline"
                                      >
                                        {article.title ?? "(untitled)"}
                                      </a>
                                      <p className="mt-0.5 text-xs text-[var(--muted)]">
                                        {article.source_name}
                                        {article.published_at ? ` | ${new Date(article.published_at).toLocaleString()}` : ""}
                                      </p>
                                    </li>
                                  ))}
                                </ul>
                              ) : null}
                            </div>
                          ) : null}
                        </li>
                      );
                    })}
                  </ol>
                ) : null}
              </>
            )}
          </article>

          <article className="card px-5 py-5 sm:px-7 sm:py-6 lg:order-1 lg:col-span-2">
            <div className="mb-4 flex items-center justify-between gap-3">
              <h2 className="text-xl font-semibold text-[var(--ink)]">Hot Themes</h2>
              <span className="rounded-full bg-[var(--accent-soft)] px-3 py-1 font-mono text-xs text-[var(--accent)]">
                now
              </span>
            </div>
            {hotThemes.error ? (
              <div className="rounded-xl border border-[color:rgba(255,180,138,0.28)] bg-[var(--danger-soft)] px-4 py-3 text-sm text-[var(--danger)]">
                {hotThemes.error}
              </div>
            ) : null}
            {hotThemes.isLoading ? (
              <div className="rounded-xl border border-dashed border-[var(--edge)] bg-[var(--paper-soft)] px-4 py-8 text-center text-sm text-[var(--muted)]">
                Loading hot themes...
              </div>
            ) : null}
            {!hotThemes.isLoading && !hotThemes.error && hotThemes.items.length > 0 ? (
              <ol className="space-y-3">
                {hotThemes.items.map((theme, index) => (
                  <li key={theme.id} className="rounded-xl border border-[var(--edge)] bg-[var(--paper-strong)] p-4">
                    <div className="mb-1 flex items-center justify-between gap-3">
                      <span className="font-mono text-xs text-[var(--muted)]">#{index + 1}</span>
                      <span className="rounded-full bg-[var(--warm-soft)] px-2.5 py-1 font-mono text-xs text-[var(--warm)]">
                        {theme.article_count} articles
                      </span>
                    </div>
                    <h3 className="text-base font-semibold text-[var(--ink)]">{theme.canonical_label}</h3>
                    <div className="mt-3 flex flex-wrap items-center gap-2">
                      <Link
                        href={`/themes/${encodeURIComponent(theme.id)}`}
                        className="inline-flex items-center rounded-full border border-[var(--edge)] bg-[var(--paper-soft)] px-3 py-1.5 text-xs font-semibold uppercase tracking-[0.08em] text-[var(--ink)] hover:border-[var(--accent)]"
                      >
                        Timeline
                      </Link>
                      {user ? (
                        <button
                          type="button"
                          onClick={() => void addThemeToWatchlist(theme.id)}
                          disabled={watchlistActionThemeId === theme.id || watchlistThemeIds.has(theme.id)}
                          className="inline-flex items-center rounded-full border border-[var(--accent-strong)] bg-[var(--accent-soft)] px-3 py-1.5 text-xs font-semibold uppercase tracking-[0.08em] text-[var(--accent)] disabled:cursor-not-allowed disabled:opacity-60"
                        >
                          {watchlistThemeIds.has(theme.id) ? "Watching" : "Add Watchlist"}
                        </button>
                      ) : null}
                    </div>
                  </li>
                ))}
              </ol>
            ) : null}
          </article>
        </section>

      </main>
      <AuthModal isOpen={isAuthModalOpen} onClose={() => setIsAuthModalOpen(false)} />
      <div className="fixed bottom-4 right-4 z-50 sm:bottom-6 sm:right-6">
        {isChatOpen ? (
          <section className="card fade-in-up mb-4 flex h-[min(78vh,680px)] w-[calc(100vw-2rem)] max-w-[420px] flex-col overflow-hidden">
            <div className="bg-[linear-gradient(135deg,#3d2512_0%,#88542a_44%,#ff9b54_100%)] px-5 py-4 text-white">
              <div className="flex items-start justify-between gap-3">
                <div>
                  <p className="font-mono text-xs uppercase tracking-[0.22em] text-white/70">Analyst Copilot</p>
                  <h2 className="mt-1 text-lg font-semibold">Ask MarketPulse</h2>
                  <p className="mt-1 text-sm text-white/75">Grounded replies with linked source articles.</p>
                </div>
                <div className="flex items-center gap-2">
                  <button
                    type="button"
                    onClick={clearChat}
                    disabled={chat.messages.length === 0 && !chat.error && !chatInput}
                    className="rounded-full border border-white/20 bg-white/10 px-3 py-1 text-xs font-semibold uppercase tracking-[0.12em] text-white/86 transition hover:bg-white/16 disabled:cursor-not-allowed disabled:opacity-45"
                  >
                    Clear
                  </button>
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
            </div>

            <div className="flex-1 space-y-4 overflow-y-auto px-4 py-4">
              {chat.messages.length === 0 ? (
                <div className="flex min-h-full items-center justify-center">
                  <p className="text-sm text-[var(--muted)]">Ask a question here</p>
                </div>
              ) : null}

              {chat.messages.map((message, index) => {
                const isUser = message.role === "user";
                const isError = message.status === "error";
                const isLatestAssistantMessage =
                  !isUser &&
                  index ===
                    [...chat.messages]
                      .map((entry) => entry.role)
                      .lastIndexOf("assistant");
                const bubbleClassName = isUser
                  ? "border-[#0a6b5a] bg-[linear-gradient(180deg,rgba(60,199,170,0.18),rgba(60,199,170,0.08))] text-[var(--ink)]"
                  : isError
                    ? "border-[color:rgba(255,180,138,0.28)] bg-[var(--danger-soft)] text-[var(--danger)]"
                    : "border-[color:rgba(255,155,84,0.18)] bg-[linear-gradient(180deg,rgba(255,155,84,0.10),rgba(255,155,84,0.03))] text-[var(--ink)]";

                return (
                  <div
                    key={message.id}
                    ref={isLatestAssistantMessage ? latestAssistantMessageRef : null}
                    className={`flex ${isUser ? "justify-end" : "justify-start"}`}
                  >
                    <article className={`max-w-[88%] rounded-2xl border px-4 py-3 ${bubbleClassName}`}>
                      <p className="mb-2 text-[11px] font-semibold uppercase tracking-[0.14em] text-[var(--muted)]">
                        {isUser ? "You" : "MarketPulse"}
                      </p>
                      <div className="whitespace-pre-wrap text-sm leading-7">
                        {message.status === "loading" ? "Thinking..." : message.content}
                      </div>
                      {!isUser && message.status === "complete" && message.sources.length > 0 ? (
                        <div className="mt-4 space-y-2 border-t border-white/8 pt-3">
                          {message.sources.map((source) => (
                            <a
                              key={`${message.id}-${source.article_id}-${source.index}`}
                              href={source.canonical_url}
                              target="_blank"
                              rel="noreferrer"
                              className="block rounded-xl border border-[var(--edge)] bg-black/10 px-3 py-2 transition hover:bg-black/16"
                            >
                              <div className="flex items-center justify-between gap-2">
                                <span className="font-mono text-[11px] text-[var(--muted)]">[{source.index}]</span>
                                <span className="font-mono text-[11px] text-[var(--muted)]">
                                  {source.similarity.toFixed(3)}
                                </span>
                              </div>
                              <p className="mt-1 text-sm font-medium text-[var(--ink)]">{source.title ?? "(untitled)"}</p>
                              <p className="mt-1 text-[11px] text-[var(--muted)]">
                                {source.source_name}
                                {source.published_at ? ` | ${new Date(source.published_at).toLocaleString()}` : ""}
                              </p>
                            </a>
                          ))}
                        </div>
                      ) : null}
                    </article>
                  </div>
                );
              })}
            </div>

            <form onSubmit={onChatSubmit} className="border-t border-[var(--edge)] bg-[color:rgba(4,10,14,0.32)] px-4 py-4">
              <div className="rounded-2xl border border-[var(--edge)] bg-[var(--paper-strong)] px-3 py-3">
                <div className="flex items-end gap-3">
                  <textarea
                    ref={chatTextareaRef}
                    value={chatInput}
                    onChange={(event) => setChatInput(event.target.value)}
                    onKeyDown={onChatInputKeyDown}
                    rows={2}
                    className="min-h-[54px] flex-1 resize-none bg-transparent text-sm text-[var(--ink)] outline-none placeholder:text-[color:rgba(155,177,185,0.7)]"
                    placeholder="Type a message about the market..."
                  />
                  <button
                    type="submit"
                    disabled={chat.isLoading}
                    className="flex h-11 w-11 items-center justify-center rounded-full border border-[#b66d34] bg-[var(--warm)] text-[#2e1304] transition hover:bg-[#ffac70] disabled:cursor-not-allowed disabled:opacity-70"
                    aria-label="Send message"
                  >
                    <svg viewBox="0 0 24 24" className="h-5 w-5 fill-none stroke-current stroke-2" aria-hidden="true">
                      <path d="M4 12h12" />
                      <path d="m11 5 7 7-7 7" />
                    </svg>
                  </button>
                </div>
                <div className="mt-2 flex items-center justify-between gap-3 text-[11px] text-[var(--muted)]">
                  <span>Enter sends. Shift+Enter adds a new line.</span>
                  {chat.error ? <span className="text-[var(--danger)]">{chat.error}</span> : null}
                </div>
              </div>
            </form>
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
