-- SQL Project - Data Cleaning

-- https://www.kaggle.com/datasets/swaptr/layoffs-2022

USE world_layoffs;

-- now when we are data cleaning we usually follow a few steps
-- 1. check for duplicates and remove any
-- 2. standardize data and fix errors
-- 3. Look at null values and see what 
-- 4. remove any columns and rows that are not necessary - few ways



-- 1. Remove Duplicates

# First let's check for duplicates

SELECT *
FROM layoffs_stagging
;

-- these are the ones we want to delete where the row number is > 1 or 2or greater essentially

SELECT * 
FROM (
SELECT company, industry, total_laid_off,`date`,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, 
            stage, country, funds_raised_millions
            ) AS row_num
	FROM layoffs_stagging1
    ) AS duplicates
WHERE row_num > 1;

-- let's just look at oda to confirm
SELECT *
FROM layoffs_stagging
WHERE company = 'oda';

-- create a new column and add those row numbers in. Then delete where row numbers are over 2, then delete that column
-- so let's do it!!

SELECT *
FROM world_layoffs.layoffs_staging
;

CREATE TABLE `world_layoffs`.`layoffs_staging2` (
`company` text,
`location`text,
`industry`text,
`total_laid_off` INT,
`percentage_laid_off` text,
`date` text,
`stage`text,
`country` text,
`funds_raised_millions` int,
row_num INT
);

INSERT INTO `layoffs_stagging2`
(`company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
`row_num`)
SELECT `company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_stagging;
	
-- now that we have this we can delete rows were row_num is greater than 2

DELETE FROM world_layoffs.layoffs_staging2
WHERE row_num >= 2;

-- 2. Standardize Data

SELECT company, TRIM(company)
FROM layoffs_stagging2;


-- Turn off safe mode first so that the table can be updated using the following this code:
SET SQL_SAFE_UPDATES = 0;

-- then this query can be executed
UPDATE layoffs_stagging2
SET company = TRIM(company);

-- now we want to change the industry column

SELECT DISTINCT industry
FROM layoffs_stagging2
WHERE industry LIKE 'Crypto%';

-- change words containing crypto to just crypto only


UPDATE layoffs_stagging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_stagging2
ORDER BY 1;

UPDATE layoffs_stagging2 
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'united states%';

SELECT DISTINCT country
FROM layoffs_stagging2
ORDER BY 1;

-- change the date column which was originally in text format to date format

SELECT `date`
FROM layoffs_stagging2;

UPDATE layoffs_stagging2 
SET `date` = STR_TO_DATE(`DATE`, '%m/%d/%Y'); 

ALTER TABLE layoffs_stagging2
MODIFY COLUMN `date` DATE;

-- it looks like airbnb is a travel, but this one just isn't populated.
-- I'm sure it's the same for the others. What we can do is
-- write a query that if there is another row with the same company name, it will update it to the non-null industry values
-- makes it easy so if there were thousands we wouldn't have to manually check them all


-- we should set the blanks to nulls since those are typically easier to work with

UPDATE layoffs_stagging2
SET industry = NULL
WHERE industry = '';

-- now if we check those are all null

SELECT *
FROM layoffs_stagging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- now we need to populate those nulls 

UPDATE layoffs_stagging2 t1
JOIN layoffs_stagging2 T2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- and if we check it looks like Bally's was the only one without a populated row to populate this null values

SELECT *
FROM layoffs_stagging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- 3. Look at Null Values

-- the null values in total_laid_off, percentage_laid_off, and funds_raised_millions all look normal. I don't think I want to change that
-- I like having them null because it makes it easier for calculations during the EDA phase

-- so there isn't anything I want to change with the null values

-- 4. remove any columns and rows we need to


ALTER TABLE layoffs_staging2
DROP COLUMN row_num;


SELECT * 
FROM layoffs_stagging2;

DELETE 
FROM layoffs_stagging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM layoffs_stagging2;