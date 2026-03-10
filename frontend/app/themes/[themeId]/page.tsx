"use client";

import Link from "next/link";
import { use, useEffect, useMemo, useState } from "react";

type TimelineTheme = {
  id: string;
  slug: string;
  canonical_label: string;
  status: string;
  article_count: number;
  current_snapshot_version: number;
  last_snapshot_at: string | null;
  first_seen_at: string | null;
  last_seen_at: string | null;
};

type TimelineArticleReference = {
  article_id: string;
  title: string;
  canonical_url: string;
  source_name: string;
  published_at: string | null;
};

type TimelineNode = {
  snapshot_version: number;
  event_at: string | null;
  period_label: string;
  article_count: number;
  event_description: string;
  related_articles: TimelineArticleReference[];
};

type ThemeTimelinePayload = {
  theme: TimelineTheme;
  theme_overview: string;
  nodes: TimelineNode[];
};

type TimelineState = {
  isLoading: boolean;
  error: string | null;
  payload: ThemeTimelinePayload | null;
};

type ThemeTimelinePageProps = {
  params: Promise<{
    themeId: string;
  }>;
};

export default function ThemeTimelinePage({ params }: ThemeTimelinePageProps) {
  const { themeId } = use(params);
  const [timeline, setTimeline] = useState<TimelineState>({
    isLoading: true,
    error: null,
    payload: null,
  });

  useEffect(() => {
    let isActive = true;

    async function loadTimeline() {
      setTimeline({ isLoading: true, error: null, payload: null });
      try {
        const response = await fetch(
          `/api/themes/${encodeURIComponent(themeId)}/timeline?limit=80`,
          {
            method: "GET",
            headers: { Accept: "application/json" },
            cache: "no-store",
          },
        );
        const payload = await response.json();
        if (!response.ok) {
          throw new Error(String(payload?.detail ?? "Cannot load theme timeline."));
        }
        if (isActive) {
          setTimeline({
            isLoading: false,
            error: null,
            payload: payload as ThemeTimelinePayload,
          });
        }
      } catch (error) {
        if (isActive) {
          setTimeline({
            isLoading: false,
            error: error instanceof Error ? error.message : "Cannot load theme timeline.",
            payload: null,
          });
        }
      }
    }

    loadTimeline();
    return () => {
      isActive = false;
    };
  }, [themeId]);

  const nodeCount = useMemo(() => timeline.payload?.nodes.length ?? 0, [timeline.payload]);
  const theme = timeline.payload?.theme ?? null;
  const themeOverview = timeline.payload?.theme_overview ?? null;
  const nodes = timeline.payload?.nodes ?? [];

  return (
    <div className="grain min-h-screen">
      <main className="mx-auto w-full max-w-5xl px-4 py-8 sm:px-6 sm:py-10 lg:px-8">
        <section className="card mb-6 px-5 py-5 sm:px-7 sm:py-6">
          <div className="mb-4 flex items-center justify-between gap-3">
            <div>
              <p className="text-xs uppercase tracking-[0.16em] text-[var(--muted)]">Theme Timeline</p>
              <h1 className="mt-1 text-2xl font-semibold text-[var(--ink)] sm:text-3xl">
                {theme?.canonical_label ?? "Loading theme..."}
              </h1>
            </div>
            <Link
              href="/"
              className="inline-flex items-center rounded-full border border-[var(--edge)] bg-[var(--paper-soft)] px-3 py-1.5 text-xs font-semibold uppercase tracking-[0.08em] text-[var(--ink)]"
            >
              Back
            </Link>
          </div>

          <div className="grid gap-3 sm:grid-cols-3">
            <div className="rounded-xl border border-[var(--edge)] bg-[var(--paper-strong)] px-4 py-3">
              <p className="text-xs uppercase tracking-[0.12em] text-[var(--muted)]">Status</p>
              <p className="mt-1 text-sm font-semibold text-[var(--ink)]">{theme?.status ?? "-"}</p>
            </div>
            <div className="rounded-xl border border-[var(--edge)] bg-[var(--paper-strong)] px-4 py-3">
              <p className="text-xs uppercase tracking-[0.12em] text-[var(--muted)]">Articles</p>
              <p className="mt-1 text-sm font-semibold text-[var(--ink)]">{theme?.article_count ?? 0}</p>
            </div>
            <div className="rounded-xl border border-[var(--edge)] bg-[var(--paper-strong)] px-4 py-3">
              <p className="text-xs uppercase tracking-[0.12em] text-[var(--muted)]">Snapshots</p>
              <p className="mt-1 text-sm font-semibold text-[var(--ink)]">{nodeCount}</p>
            </div>
          </div>
          {themeOverview ? (
            <p className="mt-4 rounded-xl border border-[var(--edge)] bg-[var(--paper-soft)] px-4 py-3 text-sm text-[var(--muted)]">
              {themeOverview}
            </p>
          ) : null}
        </section>

        <section className="card px-5 py-5 sm:px-7 sm:py-6">
          <div className="mb-4 flex items-center justify-between gap-3">
            <h2 className="text-xl font-semibold text-[var(--ink)]">Evolution Nodes</h2>
            <span className="rounded-full bg-[var(--accent-soft)] px-3 py-1 font-mono text-xs text-[var(--accent)]">
              snapshot timeline
            </span>
          </div>

          {timeline.error ? (
            <div className="rounded-xl border border-[color:rgba(255,180,138,0.28)] bg-[var(--danger-soft)] px-4 py-3 text-sm text-[var(--danger)]">
              {timeline.error}
            </div>
          ) : null}

          {timeline.isLoading ? (
            <div className="rounded-xl border border-dashed border-[var(--edge)] bg-[var(--paper-soft)] px-4 py-8 text-center text-sm text-[var(--muted)]">
              Loading timeline...
            </div>
          ) : null}

          {!timeline.isLoading && !timeline.error && nodes.length === 0 ? (
            <div className="rounded-xl border border-dashed border-[var(--edge)] bg-[var(--paper-soft)] px-4 py-8 text-center text-sm text-[var(--muted)]">
              No snapshots available yet for this theme.
            </div>
          ) : null}

          {!timeline.isLoading && !timeline.error && nodes.length > 0 ? (
            <ol className="relative ml-2 border-l border-[var(--edge)] pl-6 sm:pl-8">
              {nodes.map((node, index) => (
                <li key={`${node.snapshot_version}-${index}`} className="relative pb-6 last:pb-0">
                  <span className="absolute -left-[30px] top-4 h-4 w-4 rounded-full border border-[var(--accent-strong)] bg-[var(--accent)] sm:-left-[34px]" />
                  <article className="rounded-xl border border-[var(--edge)] bg-[var(--paper-strong)] p-4">
                    <div className="mb-2 flex flex-wrap items-center justify-between gap-2">
                      <span className="rounded-full bg-[var(--accent-soft)] px-2.5 py-1 font-mono text-xs text-[var(--accent)]">
                        v{node.snapshot_version}
                      </span>
                      <span className="text-xs text-[var(--muted)]">
                        {node.event_at
                          ? new Date(node.event_at).toLocaleString()
                          : "unknown time"}
                      </span>
                    </div>
                    <p className="mb-2 text-xs uppercase tracking-[0.11em] text-[var(--muted)]">
                      {node.period_label} | {node.article_count} linked articles
                    </p>
                    <p className="text-sm leading-6 text-[var(--ink)]">{node.event_description}</p>
                    {node.related_articles.length > 0 ? (
                      <div className="mt-3 rounded-lg border border-[var(--edge)] bg-[var(--paper-soft)] p-3">
                        <p className="mb-2 text-xs font-semibold uppercase tracking-[0.1em] text-[var(--muted)]">
                          Related Articles
                        </p>
                        <ul className="space-y-2">
                          {node.related_articles.map((article) => (
                            <li key={`${node.snapshot_version}-${article.article_id || article.canonical_url}`}>
                              <a
                                href={article.canonical_url}
                                target="_blank"
                                rel="noreferrer"
                                className="text-sm font-medium text-[var(--accent)] underline"
                              >
                                {article.title}
                              </a>
                              <p className="mt-0.5 text-xs text-[var(--muted)]">
                                {article.source_name}
                                {article.published_at
                                  ? ` | ${new Date(article.published_at).toLocaleString()}`
                                  : ""}
                              </p>
                            </li>
                          ))}
                        </ul>
                      </div>
                    ) : null}
                  </article>
                </li>
              ))}
            </ol>
          ) : null}
        </section>
      </main>
    </div>
  );
}
