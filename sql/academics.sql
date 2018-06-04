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
    SUM(CASE WHEN type = "LEC" or type = "PRJ" or type = "SEM" or type = "TUT" or type = "LAB" then duration END) / 60 as academic_hours,
    SUM(CASE WHEN type = "LEC" or type = "PRJ" or type = "SEM" then duration END) / 60 as class_hours,
	AVG(CASE WHEN type = "LEC" or type = "PRJ" or type = "SEM" then duration END) / 60 as avg_lec_len,
    SUM(CASE WHEN type = "TUT" then duration END) / 60 as tut_hours,
    SUM(CASE WHEN type = "LAB" then duration END) / 60 as lab_hours
FROM schedule
LEFT JOIN important_dates ON schedule.start BETWEEN important_dates.start AND important_dates.end
GROUP BY term;

-- Aggregate all interviews received from CECA
DROP VIEW IF EXISTS academics_coop_interviews;
CREATE VIEW academics_coop_interviews AS
SELECT term, COUNT(*) FROM email
LEFT JOIN important_dates ON email.date BETWEEN important_dates.start AND important_dates.end
WHERE email.subject = "uW Pick Interview Slot"
GROUP BY term;
