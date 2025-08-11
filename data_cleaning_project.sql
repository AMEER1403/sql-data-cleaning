-- =========================================================
-- SQL DATA CLEANING PROJECT
-- Dataset: Layoffs
-- Author: [Your Name]
-- Description: Cleaning and standardizing layoffs dataset
-- =========================================================

-- STEP 1: VIEW RAW DATA
SELECT * FROM layoffs;

-- =========================================================
-- STEP 2: REMOVE DUPLICATES
-- =========================================================

-- Create a staging table for cleaning
CREATE TABLE layoff_staging LIKE layoffs;

INSERT INTO layoff_staging
SELECT * FROM layoffs;

-- Check for duplicates
SELECT *,
       ROW_NUMBER() OVER(
           PARTITION BY company, location, industry, total_laid_off,
                        percentage_laid_off, `date`, stage, country, funds_raised_millions
       ) AS row_num
FROM layoff_staging;

-- Remove duplicates using CTE
SET SQL_SAFE_UPDATES = 0;

WITH duplicate_cte AS (
    SELECT *,
           ROW_NUMBER() OVER(
               PARTITION BY company, location, industry, total_laid_off,
                            percentage_laid_off, `date`, stage, country, funds_raised_millions
           ) AS row_num
    FROM layoff_staging
)
DELETE FROM duplicate_cte
WHERE row_num > 1;

-- =========================================================
-- STEP 3: CREATE FINAL CLEANING TABLE
-- =========================================================

CREATE TABLE layoff_staging3 (
    company TEXT,
    location TEXT,
    industry TEXT,
    total_laid_off INT DEFAULT NULL,
    percentage_laid_off TEXT,
    `date` TEXT,
    stage TEXT,
    country TEXT,
    funds_raised_millions INT DEFAULT NULL,
    row_num INT
);

-- Insert cleaned data with row numbers
INSERT INTO layoff_staging3
SELECT *,
       ROW_NUMBER() OVER(
           PARTITION BY company, location, industry, total_laid_off,
                        percentage_laid_off, `date`, stage, country, funds_raised_millions
       ) AS row_num
FROM layoff_staging;

-- Remove duplicates from final table
DELETE FROM layoff_staging3
WHERE row_num > 1;

-- =========================================================
-- STEP 4: STANDARDIZE TEXT DATA
-- =========================================================

-- Trim extra spaces in company names
UPDATE layoff_staging3
SET company = TRIM(company);

-- Standardize 'Crypto' industry naming
UPDATE layoff_staging3
SET industry = 'Crypto'
WHERE industry LIKE 'crypto%';

-- Remove trailing dots in country names
UPDATE layoff_staging3
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- =========================================================
-- STEP 5: FIX DATE FORMAT
-- =========================================================

-- Convert date strings to DATE type
UPDATE layoff_staging3
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoff_staging3
MODIFY COLUMN `date` DATE;

-- =========================================================
-- STEP 6: HANDLE NULL & BLANK VALUES
-- =========================================================

-- Replace empty industry values with NULL
UPDATE layoff_staging3
SET industry = NULL
WHERE industry = '';

-- Populate missing industry from other rows with the same company
UPDATE layoff_staging3 t1
JOIN layoff_staging3 t2
     ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
  AND t2.industry IS NOT NULL;

-- Remove rows where both total_laid_off and percentage_laid_off are NULL
DELETE FROM layoff_staging3
WHERE total_laid_off IS NULL
  AND percentage_laid_off IS NULL;

-- =========================================================
-- STEP 7: REMOVE TEMPORARY COLUMNS
-- =========================================================
ALTER TABLE layoff_staging3
DROP COLUMN row_num;

-- =========================================================
-- CLEANING COMPLETE
-- =========================================================
SELECT * FROM layoff_staging3;
