create extension if not exists vector;
create extension if not exists pgcrypto;

create table if not exists articles (
  id uuid primary key default gen_random_uuid(),
  source_name text not null,
  source_article_id text null,
  url text not null,
  canonical_url text not null unique,
  title text null,
  description text null,
  content text null,
  author text null,
  language varchar(8) null,
  region text null,
  published_at timestamptz null,
  embedding vector(1536) null,
  embedding_model text null default 'text-embedding-3-small',
  embedded_at timestamptz null,
  metadata jsonb not null default '{}'::jsonb,
  raw_payload jsonb null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists idx_articles_published_at on articles (published_at desc);
create index if not exists idx_articles_source_published_at on articles (source_name, published_at desc);
create index if not exists idx_articles_created_at on articles (created_at desc);
create index if not exists idx_articles_metadata_gin on articles using gin (metadata);
create index if not exists idx_articles_embedding_hnsw
on articles using hnsw (embedding vector_cosine_ops)
where embedding is not null;

create or replace function match_articles(
  query_embedding vector(1536),
  match_count int default 20,
  min_published_at timestamptz default null,
  filter_source text default null
)
returns table (
  article_id uuid,
  canonical_url text,
  title text,
  published_at timestamptz,
  source_name text,
  similarity double precision
)
language sql
stable
as $$
  select
    a.id as article_id,
    a.canonical_url,
    a.title,
    a.published_at,
    a.source_name,
    (1 - (a.embedding <=> query_embedding)) as similarity
  from articles a
  where a.embedding is not null
    and (min_published_at is null or a.published_at >= min_published_at)
    and (filter_source is null or a.source_name = filter_source)
  order by a.embedding <=> query_embedding
  limit greatest(match_count, 1);
$$;

create table if not exists themes (
  id uuid primary key default gen_random_uuid(),
  slug text not null unique,
  canonical_label text not null,
  summary text null,
  status text not null default 'active',
  discovery_method text not null default 'signal',
  first_seen_at timestamptz null,
  last_seen_at timestamptz null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint chk_themes_status
    check (status in ('candidate', 'emerging', 'active', 'cooling', 'dormant', 'retired'))
);

create unique index if not exists idx_themes_canonical_label_ci
on themes (lower(canonical_label));
create index if not exists idx_themes_status on themes (status);
create index if not exists idx_themes_last_seen_at on themes (last_seen_at desc);

create table if not exists theme_aliases (
  id uuid primary key default gen_random_uuid(),
  theme_id uuid not null references themes(id) on delete cascade,
  alias text not null,
  normalized_alias text not null,
  is_primary boolean not null default false,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (theme_id, normalized_alias),
  unique (normalized_alias)
);

create index if not exists idx_theme_aliases_theme_id on theme_aliases (theme_id);

create table if not exists theme_candidates (
  id uuid primary key default gen_random_uuid(),
  display_label text not null,
  normalized_label text not null unique,
  article_count integer not null default 0,
  status text not null default 'candidate',
  promoted_theme_id uuid null references themes(id) on delete set null,
  first_seen_at timestamptz null,
  last_seen_at timestamptz null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint chk_theme_candidates_status
    check (status in ('candidate', 'promoted', 'discarded'))
);

create index if not exists idx_theme_candidates_status on theme_candidates (status);
create index if not exists idx_theme_candidates_article_count
on theme_candidates (article_count desc, last_seen_at desc);

create table if not exists theme_article_links (
  id uuid primary key default gen_random_uuid(),
  theme_id uuid not null references themes(id) on delete cascade,
  article_id uuid not null references articles(id) on delete cascade,
  assignment_score double precision not null default 0.0,
  assignment_method text not null default 'signal_alias',
  is_primary boolean not null default false,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (theme_id, article_id)
);

create index if not exists idx_theme_article_links_theme_id on theme_article_links (theme_id);
create index if not exists idx_theme_article_links_article_id on theme_article_links (article_id);
create index if not exists idx_theme_article_links_is_primary on theme_article_links (is_primary);

create table if not exists theme_snapshots (
  id uuid primary key default gen_random_uuid(),
  theme_id uuid not null references themes(id) on delete cascade,
  bucket_start timestamptz not null,
  bucket_granularity text not null default 'day',
  article_count integer not null default 0,
  source_count integer not null default 0,
  momentum_score double precision not null default 0.0,
  avg_assignment_score double precision not null default 0.0,
  avg_market_tone_score double precision not null default 0.0,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (theme_id, bucket_start, bucket_granularity)
);

create index if not exists idx_theme_snapshots_theme_bucket
on theme_snapshots (theme_id, bucket_start desc);
create index if not exists idx_theme_snapshots_bucket
on theme_snapshots (bucket_start desc);

create table if not exists theme_relations (
  id uuid primary key default gen_random_uuid(),
  source_theme_id uuid not null references themes(id) on delete cascade,
  target_theme_id uuid not null references themes(id) on delete cascade,
  relation_type text not null default 'co_occurrence',
  relation_score double precision not null default 0.0,
  evidence_count integer not null default 0,
  last_observed_at timestamptz null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint chk_theme_relations_distinct check (source_theme_id <> target_theme_id),
  unique (source_theme_id, target_theme_id, relation_type)
);

create index if not exists idx_theme_relations_source on theme_relations (source_theme_id);
create index if not exists idx_theme_relations_target on theme_relations (target_theme_id);
create index if not exists idx_theme_relations_score
on theme_relations (relation_type, relation_score desc);

create table if not exists theme_lineage (
  id uuid primary key default gen_random_uuid(),
  parent_theme_id uuid not null references themes(id) on delete cascade,
  child_theme_id uuid not null references themes(id) on delete cascade,
  relation_type text not null,
  note text null,
  created_at timestamptz not null default now(),
  constraint chk_theme_lineage_distinct check (parent_theme_id <> child_theme_id),
  constraint chk_theme_lineage_type
    check (relation_type in ('split_from', 'merged_into', 'renamed_from', 'evolved_from')),
  unique (parent_theme_id, child_theme_id, relation_type)
);

create index if not exists idx_theme_lineage_parent on theme_lineage (parent_theme_id);
create index if not exists idx_theme_lineage_child on theme_lineage (child_theme_id);

alter table if exists themes
  add column if not exists centroid_embedding vector(1536),
  add column if not exists centroid_count integer not null default 0,
  add column if not exists centroid_updated_at timestamptz null,
  add column if not exists entity_profile jsonb not null default '[]'::jsonb,
  add column if not exists asset_profile jsonb not null default '[]'::jsonb,
  add column if not exists relationship_profile jsonb not null default '[]'::jsonb,
  add column if not exists profile_updated_at timestamptz null;

create index if not exists idx_themes_centroid_count
on themes (centroid_count desc);

alter table if exists theme_candidates
  add column if not exists centroid_embedding vector(1536),
  add column if not exists centroid_count integer not null default 0,
  add column if not exists cohesion_sum double precision not null default 0.0,
  add column if not exists cohesion_count integer not null default 0,
  add column if not exists entity_profile jsonb not null default '[]'::jsonb;

create index if not exists idx_theme_candidates_centroid_count
on theme_candidates (centroid_count desc);

create table if not exists theme_candidate_aliases (
  id uuid primary key default gen_random_uuid(),
  candidate_id uuid not null references theme_candidates(id) on delete cascade,
  alias text not null,
  normalized_alias text not null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (candidate_id, normalized_alias),
  unique (normalized_alias)
);

create index if not exists idx_theme_candidate_aliases_candidate_id
on theme_candidate_aliases (candidate_id);

create table if not exists theme_candidate_observations (
  id uuid primary key default gen_random_uuid(),
  candidate_id uuid not null references theme_candidates(id) on delete cascade,
  article_id uuid not null references articles(id) on delete cascade,
  source_name text null,
  observed_at timestamptz not null,
  signal_text text null,
  normalized_signal text not null,
  entity_names jsonb not null default '[]'::jsonb,
  created_at timestamptz not null default now(),
  unique (candidate_id, article_id)
);

create index if not exists idx_theme_candidate_observations_candidate_id
on theme_candidate_observations (candidate_id);
create index if not exists idx_theme_candidate_observations_observed_at
on theme_candidate_observations (observed_at desc);

alter table if exists theme_article_links
  add column if not exists assignment_version text not null default 'hybrid_v1',
  add column if not exists assignment_rationale jsonb not null default '{}'::jsonb,
  add column if not exists alias_score double precision not null default 0.0,
  add column if not exists semantic_score double precision not null default 0.0,
  add column if not exists entity_overlap_score double precision not null default 0.0,
  add column if not exists asset_overlap_score double precision not null default 0.0,
  add column if not exists relationship_overlap_score double precision not null default 0.0,
  add column if not exists margin_score double precision not null default 0.0;

create index if not exists idx_theme_article_links_assignment_version
on theme_article_links (assignment_version);

create table if not exists theme_assignment_logs (
  id uuid primary key default gen_random_uuid(),
  theme_id uuid not null references themes(id) on delete cascade,
  article_id uuid not null references articles(id) on delete cascade,
  assignment_version text not null,
  assignment_method text not null,
  assignment_score double precision not null,
  alias_score double precision not null default 0.0,
  semantic_score double precision not null default 0.0,
  entity_overlap_score double precision not null default 0.0,
  asset_overlap_score double precision not null default 0.0,
  relationship_overlap_score double precision not null default 0.0,
  margin_score double precision not null default 0.0,
  rationale jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create index if not exists idx_theme_assignment_logs_theme_created
on theme_assignment_logs (theme_id, created_at desc);
create index if not exists idx_theme_assignment_logs_article_created
on theme_assignment_logs (article_id, created_at desc);

create table if not exists theme_maintenance_recommendations (
  id uuid primary key default gen_random_uuid(),
  recommendation_type text not null,
  source_theme_id uuid not null references themes(id) on delete cascade,
  target_theme_id uuid null references themes(id) on delete cascade,
  confidence_score double precision not null default 0.0,
  status text not null default 'suggested',
  rationale text null,
  payload jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint chk_theme_maintenance_recommendation_type
    check (recommendation_type in ('merge', 'split')),
  constraint chk_theme_maintenance_recommendation_status
    check (status in ('suggested', 'applied', 'dismissed'))
);

create index if not exists idx_theme_maintenance_recommendations_status
on theme_maintenance_recommendations (status, created_at desc);
create index if not exists idx_theme_maintenance_recommendations_source
on theme_maintenance_recommendations (source_theme_id);
create index if not exists idx_theme_maintenance_recommendations_target
on theme_maintenance_recommendations (target_theme_id);

create table if not exists theme_sync_runs (
  id uuid primary key default gen_random_uuid(),
  assignment_input_count integer not null default 0,
  assigned_articles integer not null default 0,
  theme_links_upserted integer not null default 0,
  created_themes integer not null default 0,
  promoted_candidates integer not null default 0,
  snapshots_upserted integer not null default 0,
  relations_upserted integer not null default 0,
  status_updates integer not null default 0,
  abstained_articles integer not null default 0,
  abstained_signals integer not null default 0,
  assignment_rate double precision not null default 0.0,
  abstain_rate double precision not null default 0.0,
  recommendation_count integer not null default 0,
  recommendation_applied_count integer not null default 0,
  config jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create index if not exists idx_theme_sync_runs_created_at
on theme_sync_runs (created_at desc);

create table if not exists users (
  id uuid primary key default gen_random_uuid(),
  email text not null unique,
  hashed_password text not null,
  is_active boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

do $$
begin
  if exists (
    select 1
    from information_schema.tables
    where table_schema = 'public'
      and table_name = 'theme_lineage'
  ) then
    alter table theme_lineage drop constraint if exists chk_theme_lineage_type;
    alter table theme_lineage add constraint chk_theme_lineage_type
      check (
        relation_type in (
          'split_from',
          'merged_into',
          'renamed_from',
          'evolved_from',
          'merge_recommended',
          'split_recommended'
        )
      );
  end if;
exception
  when duplicate_object then
    null;
end $$;
