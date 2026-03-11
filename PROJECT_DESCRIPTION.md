# Project Description

MarketPulse is an AI-powered macroeconomic intelligence platform designed to reduce information overload for investors and research teams. Instead of relying on manual keyword alerts and fragmented notes, it transforms large volumes of financial news into structured, trackable insights.

The system ingests articles from external sources, processes text with embeddings, and links content to emerging macro themes using semantic similarity. Themes are ranked by momentum ("hot themes"), tracked over time through snapshots, and exposed through a timeline view so users can monitor narrative evolution.

Users can run semantic search for articles with optional filters, create personal watchlists, and follow both global and custom themes. A retrieval-augmented chatbot provides grounded answers with linked source articles, helping users move from raw headlines to actionable context.

Technically, MarketPulse uses a FastAPI backend with layered services/repositories, PostgreSQL + pgvector for vector search, and a Next.js + TypeScript frontend with API proxy routes and token-based authentication.

The result is a practical decision-support tool that helps asset managers, hedge funds, and macro research teams identify what matters now, understand how it is changing, and react faster with confidence.
