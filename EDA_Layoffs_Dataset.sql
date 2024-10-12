-- Exploratory data analysis

-- Looking at Percentage to see how big these layoffs were

SELECT MAX(percentage_laid_off),  MIN(percentage_laid_off)
FROM layoffs_stagging2
WHERE  percentage_laid_off IS NOT NULL;

-- Which companies had 1 which is basically 100 percent of they company laid off

SELECT *
FROM layoffs_stagging2
WHERE  percentage_laid_off = 1;

-- these are mostly startups it looks like who all went out of business during this time

-- if we order by funcs_raised_millions we can see how big some of these companies were

SELECT *
FROM layoffs_stagging2
WHERE  percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

-- BritishVolt looks like an EV company, Quibi! I recognize that company
-- wow raised like 2 billion dollars and went under - ouch

-- SOMEWHAT TOUGHER AND MOSTLY USING GROUP BY--------------------------------------------------------------------------------------------------

-- Companies with the biggest single Layoff

SELECT company, total_laid_off
FROM layoffs_stagging
ORDER BY 2 DESC
LIMIT 5;
-- now that's just on a single day

-- Companies with the most Total Layoffs
SELECT company, SUM(total_laid_off)
FROM layoffs_stagging2
GROUP BY company
ORDER BY 2 DESC
LIMIT 10;


-- by location
SELECT location, SUM(total_laid_off)
FROM layoffs_stagging2
GROUP BY location
ORDER BY 2 DESC
LIMIT 10;

-- this it total in the past 3 years or in the dataset

SELECT country, SUM(total_laid_off)
FROM layoffs_stagging2
GROUP BY country
ORDER BY 2 DESC;

SELECT YEAR(date), SUM(total_laid_off)
FROM layoffs_stagging2
GROUP BY YEAR(date)
ORDER BY 1 ASC;


SELECT industry, SUM(total_laid_off)
FROM layoffs_stagging2
GROUP BY industry
ORDER BY 2 DESC;


SELECT stage, SUM(total_laid_off)
FROM layoffs_stagging2
GROUP BY stage
ORDER BY 2 DESC;

-- Earlier we looked at Companies with the most Layoffs. Now let's look at that per year. It's a little more difficult.
-- I want to look at
 
WITH company_year (`company`, `years`, `total_laid_off`) AS 
(
SELECT company, year(`date`), SUM(total_laid_off) 
FROM layoffs_stagging2
GROUP BY 1, 2
),
	company_rank AS (
    SELECT company, years, total_laid_off, DENSE_RANK() OVER(PARTITION BY `years` ORDER BY `total_laid_off` DESC) AS Rangking
    FROM company_year
    )
		 SELECT company, years, total_laid_off, Rangking
         FROM company_rank
         WHERE Rangking <= 5
         AND years IS NOT NULL
         ORDER BY years ASC, total_laid_off DESC;


-- Rolling Total of Layoffs Per Month

SELECT SUBSTR(`date`, 1, 7) AS 'Month', SUM(total_laid_off)
FROM layoffs_stagging2
WHERE SUBSTR(`date`, 1, 7) IS NOT NULL
GROUP BY SUBSTR(`date`, 1, 7)
ORDER BY SUBSTR(`date`, 1, 7) ASC;

-- now use it in a CTE so we can query off of it

WITH rolling_total AS (
	SELECT SUBSTR(`date`, 1, 7) AS 'Month', SUM(total_laid_off) AS total_off
		FROM layoffs_stagging2
		WHERE SUBSTR(`date`, 1, 7) IS NOT NULL
		GROUP BY SUBSTR(`date`, 1, 7)
		ORDER BY SUBSTR(`date`, 1, 7) ASC
        )
	SELECT `month`, total_off, SUM(total_off) OVER(ORDER BY `month`) AS rolling_total_Month
    FROM rolling_total
    ORDER BY `month` ASC;
    
    