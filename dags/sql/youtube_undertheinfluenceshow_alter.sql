ALTER TABLE youtube_undertheinfluenceshow_data 
            ALTER column published_at TYPE timestamp USING published_at::timestamp without time zone,
            ALTER column updated_at TYPE timestamp USING updated_at::timestamp without time zone,
            ALTER column text_polarities TYPE numeric(3,2) USING text_polarities::numeric(3,2);
