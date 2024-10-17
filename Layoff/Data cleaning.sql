-- Data Cleaning

USE layoff;

SELECT * 
FROM layoffs;

-- 1. Remove duplicates
-- 2. Standardize data
-- 3. Remove null values or blank values
-- 4. Remove any columns

-- MAKE COPY OF OG DATASET TO MAKE MODIFICATION

CREATE TABLE layoff_copy 
LIKE layoffs;

SELECT * 
FROM layoff_copy;

INSERT layoff_copy 
SELECT *
FROM layoffs;

-- LOOK FOR DUPLICATE ROWS BY PARTITIONING 

SELECT *,
ROW_NUMBER()OVER(
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off)AS row_num
from layoff_copy;

WITH Duplicate_cte AS
(
SELECT *,
ROW_NUMBER()OVER(
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,country,funds_raised_millions)AS row_num
from layoff_copy
)
SELECT * 
FROM Duplicate_cte
WHERE row_num>1; 

SELECT * 
FROM layoff_copy
WHERE company = 'Casper';

-- CREATE A NEW TABLE TO ADD THE CALCULATED COLUMN USING CTE

CREATE TABLE `layoff_copy1` (
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

SELECT * FROM 
layoff_copy1;

INSERT INTO layoff_copy1
SELECT *,
ROW_NUMBER()OVER(
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,country,funds_raised_millions)AS row_num
from layoff_copy;

-- DELETE DUPLICATE ENTRIES
 
DELETE FROM layoff_copy1
WHERE row_num>1;

SELECT * FROM 
layoff_copy1
WHERE row_num>1;

SELECT * 
FROM layoff_copy1
WHERE company = 'Casper';

-- STANDARDIZATION

SELECT company,TRIM(COMPANY)
FROM layoff_copy1;

UPDATE layoff_copy1
SET company = TRIM(COMPANY);

SELECT DISTINCT(industry)
FROM layoff_copy1
ORDER BY 1;

SELECT distinct(industry)
FROM layoff_copy1
WHERE industry like 'crypto%';

UPDATE layoff_copy1
SET industry= 'Crypto'
WHERE industry like 'Crypto%';

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
from layoff_copy1
ORDER BY 1;

UPDATE layoff_copy1
SET country = TRIM(TRAILING '.' FROM country)
WHERE COUNTRY LIKE 'United States%';

SELECT DISTINCT country
from layoff_copy1
ORDER BY 1;

SELECT `date`, 
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoff_copy1;

UPDATE layoff_copy1
SET `date`= STR_TO_DATE(`date`, '%m/%d/%Y');

SELECT * FROM 
layoff_copy1;

ALTER TABLE layoff_copy1
MODIFY COLUMN `date` DATE;

-- REMOVE NULL VALUES OR BLANK VALUES 

SELECT * FROM
layoff_copy1 WHERE
total_laid_off IS NULL AND
percentage_laid_off IS NULL;

SELECT * FROM
layoff_copy1
WHERE industry IS NULL 
OR industry = '';

-- CHECK FOR WE CAN POPULATE VALUES FROM OTHER ENTRIES OF SAME COMPANY

select * from
layoff_copy1 where
company = 'Airbnb';

SELECT L1.industry, L2.industry FROM
layoff_copy1 L1
JOIN layoff_copy1 L2
	ON L1.company=L2.company AND
    L1.location = L2.location
WHERE (L1.industry IS NULL OR L1.industry ='' AND
	  L2.industry IS NOT NULL);

-- SET ALL THE BLANK VALUES TO NULL VALUES TO EASILY WORK WITH IT

UPDATE layoff_copy1
SET industry = null
WHERE industry='';

UPDATE layoff_copy1 L1
JOIN layoff_copy1 L2
	ON L1.company=L2.company AND
    L1.location = L2.location
SET L1.industry=L2.industry
WHERE (L1.industry IS NULL OR L1.industry ='') AND
	  L2.industry IS NOT NULL;
      
      
SELECT * FROM
layoff_copy1 WHERE
total_laid_off IS NULL AND
percentage_laid_off IS NULL;

-- DELETE ENTRIES NOT HAVING NECESSARY INFORMATION

DELETE
FROM layoff_copy1
WHERE total_laid_off IS NULL AND
percentage_laid_off IS NULL;

SELECT * FROM layoff_copy1;

-- DROP UNNECESSARY COLUMN

ALTER TABLE layoff_copy1
DROP COLUMN row_num;

SELECT * FROM layoff_copy1;
-- FINAL CLEANED DATASET



