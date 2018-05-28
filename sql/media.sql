DROP VIEW IF EXISTS media_consumption;
CREATE VIEW media_consumption AS
SELECT term,
	SUM(runtime) as hours_watched,
	SUM(CASE WHEN movie = 1 then 1 END) as movie_count,
    SUM(CASE WHEN movie = 0 then 1 END) as shows_count,
    AVG(runtime) as average_runtime
FROM media
LEFT JOIN important_dates ON media.date BETWEEN important_dates.start AND important_dates.end
GROUP BY term;
