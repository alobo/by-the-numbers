-- Aggregate all transactions related to coffeeshops
DROP VIEW IF EXISTS finance_coffee_spend;
CREATE VIEW finance_coffee_spend AS
SELECT term, SUM(amount) AS finance_coffee_spend
FROM finances
LEFT JOIN important_dates ON finances.date BETWEEN important_dates.start AND important_dates.end
WHERE amount < 0 AND (
	LOWER(description) LIKE "%hortons%" OR
    LOWER(description) LIKE "%starbucks%" OR
    LOWER(description) LIKE "%coffee%"
)
GROUP BY term;
