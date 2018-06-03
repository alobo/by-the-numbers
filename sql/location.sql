-- Attendance view - check GPS coordinates during calendar events (classes)
DROP VIEW IF EXISTS location_event_attendance;
CREATE VIEW location_event_attendance AS
SELECT start,
	   MIN(course) as course,
	   MIN(type) as type,
	   MAX(present) as present
FROM
	-- my location during each lecture
    (SELECT course,
			type,
			start,
			end,
			loc,
			accuracy,
			datetime,
			st_distance_sphere(loc, POINT(-80.544076, 43.470612)) as distanceToCampus,
			st_distance_sphere(loc, POINT(-80.544076, 43.470612)) < 440 as present
	FROM `location`
	LEFT JOIN `schedule` ON location.datetime BETWEEN `schedule`.start AND `schedule`.end
	WHERE course IS NOT NULL) as location_during_event
GROUP BY start;

-- Calculate academic event attendance
DROP VIEW IF EXISTS location_aggregate_event_attendance;
CREATE VIEW location_aggregate_event_attendance AS
SELECT term, AVG(present) as attendance FROM location_event_attendance
LEFT JOIN important_dates ON location_event_attendance.start BETWEEN important_dates.start AND important_dates.end
GROUP BY term;

-- Calculate the number of times I visited home
DROP VIEW IF EXISTS location_home_visits;
CREATE VIEW location_home_visits AS
SELECT term,
	-- Subtract 1 to account for travel on the last day
	SUM(CASE WHEN name LIKE "home%" THEN 1 END) - 1 as days_at_home
FROM
	-- Subquery required because each day has many GPS coordinates
	(SELECT DATE(datetime) as date, name FROM location
	WHERE name IS NOT NULL
	GROUP BY DATE(datetime), name) as daily_location
LEFT JOIN important_dates ON DATE(daily_location.date) BETWEEN important_dates.start AND important_dates.end
GROUP BY term;
