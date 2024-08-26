# DATA SOURCE: https://www.kaggle.com/datasets/swaptr/layoffs-2022

-- SQL Project -DATA CLEANING 



-- DATA BACK UP ( to not mess with raw data )

CREATE TABLE discharge_2 LIKE discharge;
INSERT discharge_2 SELECT * FROM discharge;

-- REMOVE DUPLICATES ( duplicate data may lead to inaccuracies )
-- could be easier if the table contains UNIQUE IDs

WITH duplicate_cte AS 
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry
,total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM discharge_2
)
SELECT * FROM duplicate_cte
WHERE row_num > 1;

DELETE FROM duplicate_cte
WHERE row_num > 1;

-- could not delete duplicated rows due to MySQL Error 1288 ( lack of PRIMARY KEYS )
-- Delete duplicated rows with another method

CREATE TABLE `discharge_dup` 
(
`company` text,
`location` text,
`industry` text,
`total_laid_off` int DEFAULT NULL,
`percentage_laid_off` text,
`date` text,
`stage` text,
`country` text,
`funds_raised_millions` int DEFAULT NULL,
`row_num` int
);

INSERT INTO discharge_dup
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry
,total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM discharge_2;

SELECT * FROM discharge_dup
WHERE row_num > 1;

DELETE FROM discharge_dup
WHERE row_num > 1;


-- STANDARDIZING DATA 

SELECT company, TRIM(company) 
FROM discharge_dup;

UPDATE discharge_dup 
SET company = TRIM(company);

SELECT DISTINCT industry
FROM discharge_dup;

SELECT * FROM discharge_dup
WHERE industry LIKE 'Crypto%';

UPDATE discharge_dup
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT country
FROM discharge_dup;

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM discharge_dup;

UPDATE discharge_dup
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

SELECT `date`, str_to_date(`date`, '%m/%d/%Y')
FROM discharge_dup;

UPDATE discharge_dup 
SET `date` = str_to_date(`date`, '%m/%d/%Y');

-- set data type of date column to 'date' instead of 'text'

ALTER TABLE discharge_dup
MODIFY COLUMN `date` DATE;

-- Fill NULL industry values with a JOIN

SELECT *
FROM discharge_dup t1
JOIN discharge_dup t2
ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE discharge_dup
SET industry = NULL
WHERE industry = '';

UPDATE discharge_dup t1
JOIN discharge_dup t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- rows where percentage & total laid are NULL, we get rid of them

DELETE FROM discharge_dup
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;





-- EXPLORATORY DATA ANALYSIS


-- What companies laid off their whole workforce?

SELECT * FROM discharge_dup
WHERE percentage_laid_off = 1;

-- What top 3 companies laid of the most employees?

SELECT company, SUM(total_laid_off) 
FROM discharge_dup
GROUP BY company
ORDER BY 2 DESC
LIMIT 3;

-- When these layoffs happened?

SELECT MIN(`date`), MAX(`date`)
FROM discharge_dup;

-- Which country has the most employees laid off?

SELECT country, SUM(total_laid_off)
FROM discharge_dup
GROUP BY country
ORDER BY 2 DESC
LIMIT 1;






SELECT * FROM discharge_dup
