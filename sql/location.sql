-- Attendance view - check GPS coordinates during calendar events (classes)
DROP VIEW IF EXISTS event_attendance;
CREATE VIEW event_attendance AS
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
