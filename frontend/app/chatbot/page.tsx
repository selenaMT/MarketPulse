"use client";

import Link from "next/link";
import { FormEvent, KeyboardEvent, useEffect, useRef, useState } from "react";

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

function createMessageId(prefix: string) {
  return `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

export default function ChatbotPage() {
  const [chatInput, setChatInput] = useState("");
  const [chat, setChat] = useState<ChatState>({
    isLoading: false,
    error: null,
    messages: [],
  });
  const chatTextareaRef = useRef<HTMLTextAreaElement | null>(null);
  const latestAssistantMessageRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    chatTextareaRef.current?.focus();
  }, []);

  useEffect(() => {
    latestAssistantMessageRef.current?.scrollIntoView({
      block: "start",
      behavior: "smooth",
    });
  }, [chat.messages, chat.isLoading]);

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

    try {
      const response = await fetch("/api/chat/answer", {
        method: "POST",
        headers: { Accept: "application/json", "Content-Type": "application/json" },
        body: JSON.stringify({
          query: trimmedQuery,
          retrieval_limit: 5,
          conversation_history: conversationHistory,
        }),
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

  return (
    <div className="grain min-h-screen">
      <main className="mx-auto flex min-h-screen w-full max-w-5xl flex-col px-4 py-8 sm:px-6 sm:py-10 lg:px-8">
        <section className="card flex min-h-[80vh] flex-col overflow-hidden">
          <div className="bg-[linear-gradient(135deg,#3d2512_0%,#88542a_44%,#ff9b54_100%)] px-5 py-4 text-white sm:px-6 sm:py-5">
            <div className="flex items-start justify-between gap-3">
              <div>
                <p className="font-mono text-xs uppercase tracking-[0.22em] text-white/70">Analyst Copilot</p>
                <h1 className="mt-1 text-xl font-semibold sm:text-2xl">MarketPulse Chatbot</h1>
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
                <Link
                  href="/"
                  className="rounded-full border border-white/20 bg-white/10 px-3 py-1 text-xs font-semibold uppercase tracking-[0.12em] text-white/86 transition hover:bg-white/16"
                >
                  Home
                </Link>
              </div>
            </div>
          </div>

          <div className="flex-1 space-y-4 overflow-y-auto px-4 py-4 sm:px-6">
            {chat.messages.length === 0 ? (
              <div className="flex min-h-full items-center justify-center">
                <p className="text-sm text-[var(--muted)]">Ask a question about macro and markets</p>
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
                              <span className="font-mono text-[11px] text-[var(--muted)]">{source.similarity.toFixed(3)}</span>
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

          <form onSubmit={onChatSubmit} className="border-t border-[var(--edge)] bg-[color:rgba(4,10,14,0.32)] px-4 py-4 sm:px-6">
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
      </main>
    </div>
  );
}
