-- Aggregate GPA data
DROP VIEW IF EXISTS academics_transcript_summary;
CREATE VIEW academics_transcript_summary AS
SELECT term, AVG(grade) as gpa from transcript
WHERE in_gpa = 1
GROUP BY term;

-- Aggregate all lecture, lab and tutorial hours
DROP VIEW IF EXISTS academics_class_hours;
CREATE VIEW academics_class_hours AS
SELECT term,
    SUM(CASE WHEN type = "LEC" or type = "PRJ" or type = "SEM" then duration END) / 60 as class_hours,
	AVG(CASE WHEN type = "LEC" or type = "PRJ" or type = "SEM" then duration END) / 60 as class_hours_avg,
    COUNT(CASE WHEN type = "LEC" or type = "PRJ" or type = "SEM" then 1 END) as class_events,
    SUM(CASE WHEN type = "TUT" then duration END) / 60 as tut_hours,
    COUNT(CASE WHEN type = "TUT" then 1 END) as tut_events,
    SUM(CASE WHEN type = "LAB" then duration END) / 60 as lab_hours,
    COUNT(CASE WHEN type = "LAB" then 1 END) as lab_events
FROM schedule
LEFT JOIN important_dates ON schedule.start BETWEEN important_dates.start AND important_dates.end
GROUP BY term;
