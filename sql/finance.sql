-- Categorize coffee purchases
UPDATE finances
SET category = 'Personal:Dining:Coffee'
WHERE (
	LOWER(description) LIKE "%hortons%" OR
    LOWER(description) LIKE "%starbucks%" OR
    LOWER(description) LIKE "%settlement%" OR
    LOWER(description) LIKE "%coffee%"
);

-- Aggregate all transactions related to coffeeshops
DROP VIEW IF EXISTS finance_coffee_spend;
CREATE VIEW finance_coffee_spend AS
SELECT term, semester, SUM(amount) AS coffee_spend
FROM finances
LEFT JOIN important_dates ON finances.date BETWEEN important_dates.start AND important_dates.end
WHERE amount < 0 AND LOWER(category) LIKE "%coffee%"
GROUP BY term, semester;
