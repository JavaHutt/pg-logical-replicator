CREATE TABLE snapshotters (
    id UUID primary key,
    started_at timestamp with time zone not null default now(),
    table_name varchar not null,
    key varchar not null default '',
    version timestamp with time zone not null default timestamp 'epoch',
    is_done boolean not null default false
);
