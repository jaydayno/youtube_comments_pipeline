DROP TABLE IF EXISTS youtube_jubilee_data;
CREATE TABLE IF NOT EXISTS youtube_jubilee_data (
    id character varying PRIMARY KEY,
    author_channel_id character varying,
    author character varying,
    published_at character varying,
    updated_at character varying,
    like_count character varying,
    display_text character varying,
    key_phrase character varying,
    text_polarities character varying,
    classifications character varying
);
