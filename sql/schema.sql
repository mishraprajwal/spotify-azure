-- Source schema for Azure SQL Database (T-SQL)

CREATE TABLE dbo.dim_artist (
    artist_id   INT           NOT NULL PRIMARY KEY,
    artist_name NVARCHAR(150) NOT NULL,
    genre       NVARCHAR(50)  NOT NULL,
    country     NVARCHAR(50)  NOT NULL,
    updated_at  DATETIME2(0)  NOT NULL
);

CREATE TABLE dbo.dim_track (
    track_id     INT           NOT NULL PRIMARY KEY,
    track_name   NVARCHAR(200) NOT NULL,
    artist_id    INT           NOT NULL REFERENCES dbo.dim_artist (artist_id),
    album_name   NVARCHAR(200) NOT NULL,
    duration_sec INT           NOT NULL,
    release_date DATE          NOT NULL,
    updated_at   DATETIME2(0)  NOT NULL
);

CREATE TABLE dbo.dim_user (
    user_id           INT           NOT NULL PRIMARY KEY,
    user_name         NVARCHAR(100) NOT NULL,
    country           NVARCHAR(50)  NOT NULL,
    subscription_tier NVARCHAR(20)  NOT NULL,
    updated_at        DATETIME2(0)  NOT NULL
);

CREATE TABLE dbo.dim_date (
    date_value   DATE        NOT NULL PRIMARY KEY,
    year_num     INT         NOT NULL,
    month_num    INT         NOT NULL,
    day_num      INT         NOT NULL,
    weekday_name NVARCHAR(10) NOT NULL,
    is_weekend   BIT         NOT NULL
);

CREATE TABLE dbo.fact_stream (
    stream_id        BIGINT       NOT NULL PRIMARY KEY,
    user_id          INT          NOT NULL REFERENCES dbo.dim_user (user_id),
    track_id         INT          NOT NULL REFERENCES dbo.dim_track (track_id),
    stream_timestamp DATETIME2(0) NOT NULL,
    ms_played        INT          NOT NULL,
    device_type      NVARCHAR(20) NOT NULL
);
