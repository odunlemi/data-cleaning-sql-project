-- Data Cleaning Project on World Company Layoffs from [input link here]

SELECT *
FROM layoffs;

-- To-do
-- 1. Remove duplicates
-- 2. Standardize the data
-- 3. Null values and blank values
-- 4. Remove any unnecessary columns or rows


-- Create a staging table to avoid modifying the raw dataset
CREATE TABLE layoffs_staging
LIKE layoffs;

-- Insert data into the staging table
INSERT layoffs_staging
SELECT *
FROM layoffs; -- Copy of data from the original table

-- Create a stored procedure to avoid manually calling it
CREATE PROCEDURE staging_table()
SELECT *
FROM layoffs_staging;

CALL staging_table(); -- Calls the staging table

-- Using a row number column to filter and identify duplicates
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

WITH duplicate_cte AS
(SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;	-- Returns 5 rows of duplicates

-- Using a second staging table to filter on the row numbers
CREATE TABLE `layoffs_staging2` (
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- Create a stored procedure to avoid manually calling it
CREATE PROCEDURE staging_table2()
SELECT *
FROM layoffs_staging2;

CALL staging_table2(); -- Calls the staging table

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;


-- 2. Standardizing the data

-- Updates the company names to be properly formatted
UPDATE layoffs_staging2
SET company = TRIM(company);

-- Check for duplicates or wrong data in the 'industry' column
SELECT *
FROM layoffs_staging2
WHERE industry LIKE '';

-- Updates similar companies as all 'Crypto'
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- In the 'country' column, 'United States' updated to be the same for all matching rows
SELECT DISTINCT country
FROM layoffs_staging2
WHERE country LIKE 'United States%';
UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- Modifying the 'date' column to be of datatype DATE, format YYYY-MM-DD
SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

-- Update the date column
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- Now as datatype DATE
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


-- 3. Null values and blank values

SELECT DISTINCT industry
FROM layoffs_staging2
WHERE industry is NULL
OR
industry = '';

-- Changes blank values to null
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Confirms there are no blank values
SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

-- Populates data in the 'industry' column
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- Bally's Interactive, only company with NULL value as industry
SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%';

CALL staging_table2();


-- 4. Remove any unnecessary columns or rows

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Drops 'row_num' column
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;
