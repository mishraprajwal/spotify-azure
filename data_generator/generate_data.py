"""Generate synthetic streaming data as T-SQL scripts.

Produces two scripts in the output directory:
  * seed_initial.sql     - full historical load
  * seed_incremental.sql - new rows plus updates to existing dimension rows

Uses only the standard library and a fixed seed so output is reproducible.
"""
import argparse
import random
from datetime import date, datetime, timedelta
from pathlib import Path

GENRES = ["Pop", "Rock", "Hip-Hop", "Jazz", "Electronic", "Classical", "Country", "Indie"]
COUNTRIES = ["US", "UK", "IN", "DE", "BR", "FR", "JP", "CA", "AU", "MX"]
TIERS = ["free", "premium", "family", "student"]
DEVICES = ["mobile", "desktop", "tablet", "speaker", "tv"]
SYLLABLES = ["lu", "na", "ro", "ki", "ve", "mo", "sa", "tre", "bel", "dor", "fin", "ax", "ora", "ix"]
WORDS = ["Echo", "Midnight", "Neon", "Drift", "Velvet", "Static", "Horizon", "Ember", "Orbit", "Hollow", "Pulse", "Mirage"]

BATCH = 500


def make_name(rng, parts=2):
    return " ".join("".join(rng.choices(SYLLABLES, k=rng.randint(2, 3))).capitalize() for _ in range(parts))


def title(rng):
    return f"{rng.choice(WORDS)} {rng.choice(WORDS)}"


def sql_str(value):
    return "N'" + str(value).replace("'", "''") + "'"


def fmt(value):
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, datetime):
        return f"'{value:%Y-%m-%d %H:%M:%S}'"
    if isinstance(value, date):
        return f"'{value:%Y-%m-%d}'"
    return sql_str(value)


def inserts(table, columns, rows):
    out = []
    cols = ", ".join(columns)
    for i in range(0, len(rows), BATCH):
        chunk = rows[i:i + BATCH]
        values = ",\n  ".join("(" + ", ".join(fmt(v) for v in r) + ")" for r in chunk)
        out.append(f"INSERT INTO dbo.{table} ({cols}) VALUES\n  {values};\nGO\n")
    return out


def build_artists(rng, start_id, count, ts):
    return [(start_id + i, make_name(rng), rng.choice(GENRES), rng.choice(COUNTRIES), ts) for i in range(count)]


def build_tracks(rng, start_id, count, artist_ids, ts):
    rows = []
    for i in range(count):
        released = date(2015, 1, 1) + timedelta(days=rng.randint(0, 3650))
        rows.append((start_id + i, title(rng), rng.choice(artist_ids), title(rng) + " (Album)",
                     rng.randint(90, 420), released, ts))
    return rows


def build_users(rng, start_id, count, ts):
    return [(start_id + i, make_name(rng, 2), rng.choice(COUNTRIES), rng.choices(TIERS, [5, 5, 2, 1])[0], ts)
            for i in range(count)]


def build_dates(start, end):
    rows, d = [], start
    while d <= end:
        rows.append((d, d.year, d.month, d.day, d.strftime("%A"), d.weekday() >= 5))
        d += timedelta(days=1)
    return rows


def build_streams(rng, start_id, count, user_ids, track_ids, window_start, window_end):
    span = int((window_end - window_start).total_seconds())
    rows = []
    for i in range(count):
        ts = window_start + timedelta(seconds=rng.randint(0, span))
        rows.append((start_id + i, rng.choice(user_ids), rng.choice(track_ids), ts,
                     rng.randint(5_000, 420_000), rng.choice(DEVICES)))
    return rows


def write_script(path, parts):
    Path(path).write_text("\n".join(parts), encoding="utf-8")


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--out", default="sql")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--artists", type=int, default=50)
    p.add_argument("--tracks", type=int, default=300)
    p.add_argument("--users", type=int, default=200)
    p.add_argument("--streams", type=int, default=5000)
    args = p.parse_args()

    rng = random.Random(args.seed)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    t0 = datetime(2025, 9, 1)
    t1 = datetime(2025, 9, 30, 23, 59, 59)
    t2 = datetime(2025, 10, 7)

    artists = build_artists(rng, 1, args.artists, t0)
    tracks = build_tracks(rng, 1, args.tracks, [a[0] for a in artists], t0)
    users = build_users(rng, 1, args.users, t0)
    dates = build_dates(date(2025, 9, 1), date(2025, 10, 31))
    streams = build_streams(rng, 1, args.streams, [u[0] for u in users], [t[0] for t in tracks], t0, t1)

    initial = ["-- Initial load\n"]
    initial += inserts("dim_artist", ["artist_id", "artist_name", "genre", "country", "updated_at"], artists)
    initial += inserts("dim_track", ["track_id", "track_name", "artist_id", "album_name", "duration_sec",
                                     "release_date", "updated_at"], tracks)
    initial += inserts("dim_user", ["user_id", "user_name", "country", "subscription_tier", "updated_at"], users)
    initial += inserts("dim_date", ["date_value", "year_num", "month_num", "day_num", "weekday_name", "is_weekend"], dates)
    initial += inserts("fact_stream", ["stream_id", "user_id", "track_id", "stream_timestamp", "ms_played",
                                       "device_type"], streams)
    write_script(out / "seed_initial.sql", initial)

    # Incremental batch: new dimension rows, tier changes for some users, and new streams.
    new_users = build_users(rng, len(users) + 1, 20, t2)
    all_users = users + new_users
    new_tracks = build_tracks(rng, len(tracks) + 1, 15, [a[0] for a in artists], t2)
    all_tracks = tracks + new_tracks
    changed = rng.sample(users, min(15, len(users)))
    new_streams = build_streams(rng, len(streams) + 1, 800, [u[0] for u in all_users],
                                [t[0] for t in all_tracks], datetime(2025, 10, 1), datetime(2025, 10, 7, 23, 59, 59))

    inc = ["-- Incremental load\n"]
    inc += inserts("dim_user", ["user_id", "user_name", "country", "subscription_tier", "updated_at"], new_users)
    inc += inserts("dim_track", ["track_id", "track_name", "artist_id", "album_name", "duration_sec",
                                 "release_date", "updated_at"], new_tracks)
    for u in changed:
        tier = rng.choice([t for t in TIERS if t != u[3]])
        inc.append(f"UPDATE dbo.dim_user SET subscription_tier = {sql_str(tier)}, "
                   f"updated_at = {fmt(t2)} WHERE user_id = {u[0]};\nGO\n")
    inc += inserts("fact_stream", ["stream_id", "user_id", "track_id", "stream_timestamp", "ms_played",
                                   "device_type"], new_streams)
    write_script(out / "seed_incremental.sql", inc)

    print(f"Wrote {out / 'seed_initial.sql'} and {out / 'seed_incremental.sql'}")


if __name__ == "__main__":
    main()
