DROP TABLE IF EXISTS spotify_data;

CREATE TABLE IF NOT EXISTS spotify_data (
    song_name character varying,
    artist_name character varying,
    played_at character varying,
    timestamp character varying
);