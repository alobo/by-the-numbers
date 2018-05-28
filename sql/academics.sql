-- Aggregate GPA data
DROP VIEW IF EXISTS academics_transcript_summary;
CREATE VIEW academics_transcript_summary AS
SELECT term, AVG(grade) as gpa from transcript
WHERE in_gpa = 1
GROUP BY term;
