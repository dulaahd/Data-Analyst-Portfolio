-- Data Cleaning

-- https://www.kaggle.com/datasets/swaptr/layoffs-2022

-- 1.Remove Duplicates
-- 2.Standardize the data and fix errors
-- 3.Check and remove Null values or blank values if needed
-- 4.Remove any unnecessary rows and columns 


-- First, we copy all the data to a staging table called 'layoffs_staging'
-- Why? Because we are going to change and clean this data. In case we make a mistake we need to have the raw data table available.

CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT layoffs_staging
SELECT * FROM layoffs;

SELECT * FROM layoffs_staging;


-- 1.Remove Duplicates

-- To remove duplicates there's no row number. Therefore we can add a row number by partioning the data.

SELECT*,
ROW_NUMBER() OVER(
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,'date',stage,country,funds_raised_millions) AS row_num
FROM layoffs_staging;

-- To filter where the row number is greater than 2, we can use a CTE or a Sub query. Here we can use a CTE since it's easy.

WITH duplicate_cte AS
(
SELECT*,
ROW_NUMBER() OVER(
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,'date',stage,country,funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT*
FROM duplicate_cte
WHERE row_num > 1;

-- To confrim whether the remaining are th duplicates, we can check it with a selected company.

SELECT *
FROM layoffs_staging
WHERE company = "Casper";

-- To delete the duplicates, we can create a new table named 'Layoffs_staging2' and add a new column for the row number named 'row_num' and then filter the rows that are greater than 1 and then delete them. Later we can delete the row_num column.

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
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


SELECT *
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT*,
ROW_NUMBER() OVER(
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,'date',stage,country,funds_raised_millions) AS row_num
FROM layoffs_staging;

-- Duplicates in the dataset

SELECT *
FROM layoffs_staging2
WHERE row_num >1;

-- Delete the duplicates

DELETE
FROM layoffs_staging2
WHERE row_num >1;





-- 2.Standardize the data

-- to remove white spaces in company column

SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company=TRIM(company);


-- check the distinct industries and see whether there's anything to change or whether there's any null and empty rows.

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

SELECT *
FROM layoffs_staging2
WHERE industry LIKE "Crypto%";

UPDATE layoffs_staging2
SET industry = "Crypto"
WHERE industry LIKE "Crypto%";

-- check the distinct locations and see whether there's anything to change.

SELECT DISTINCT location
FROM layoffs_staging2
ORDER BY 1;

-- check the distinct countries and see whether there's anything to change.

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

-- There are some "United States" and some "United States." with a period at the end. Those need to be standardized.
SELECT DISTINCT country, TRIM(TRAILING "." FROM country)
FROM layoffs_staging2
ORDER BY 1;


UPDATE layoffs_staging2
SET country = TRIM(TRAILING "." FROM country)
WHERE country LIKE "United States%";

-- if we do EDA or Time series vizulaisations later, we need to change the form of date from text to date.

SELECT `date`,
STR_TO_DATE(`date`,'%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET  `date`= STR_TO_DATE(`date`,'%m/%d/%Y');

-- to change the format
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT*
FROM layoffs_staging2;





-- 3.Null values or blank values

SELECT*
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Converting blank into NULL

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = "";


SELECT*
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = "";

-- In some rows we can find out the industry by checking another row which have similar company name. So we can write a query and then it will update it to the non-null industry values.

SELECT*
FROM layoffs_staging2
WHERE company = "Airbnb";

-- populating the null values.

SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
  ON t1.company=t2.company
  AND t1.location = t2.location
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
  ON t1.company=t2.company
SET t1.industry= t2.industry 
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;

SELECT*
FROM layoffs_staging2;





-- 4.Remove any rows and columns

-- checking whether these rows are useful.

SELECT*
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- total_laid_off and percentage_laid_off are both null in these rows. Therefore in my opinion we can't get any useful information from these because we don't know exactly whether they have laid_off employees or not. Since I can't trust those data, I'm going to delete those rows.

DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT*
FROM layoffs_staging2;

-- Now we do not need the row_num column. Therefore, I'm going to delete the row_num column
ALTER TABLE layoffs_staging2

DROP COLUMN row_num;

SELECT*
FROM layoffs_staging2;
