CREATE TABLE entries (
    id bigserial primary key,
    created_at timestamp with time zone not null default now(),
    updated_at timestamp with time zone not null,
    description varchar(256) not null
);