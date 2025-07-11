create table if not exists download_info (
    id integer primary key autoincrement,
    url text not null,
    success integer not null
);
