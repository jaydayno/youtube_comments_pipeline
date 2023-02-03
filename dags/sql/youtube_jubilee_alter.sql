ALTER TABLE youtube_jubilee_data 
ALTER column published_at TYPE timestamp USING published_at::timestamp without time zone,
ALTER column updated_at TYPE timestamp USING updated_at::timestamp without time zone,
ALTER column like_count TYPE integer USING (like_count::integer),
ALTER column text_polarities TYPE real USING (text_polarities::real);
