select * 
from layoffs;
-- staging
CREATE TABLE Layoff_staging
like layoffs;
-- inserting values
INSERT Layoff_staging
SELECT * FROM layoffs;
-- duplicate table
SELECT * FROM layoff_staging;
-- step 1 removing duplicates
SELECT * FROM layoff_staging;
WITH duplicate_cte AS
(
SELECT*,
ROW_NUMBER()OVER(
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off, 'data')AS row_num
FROM layoff_staging
)
SELECT * 
FROM duplicate_cte
WHERE row_num > 1
order by 1;

SELECT * 
FROM layoff_staging
WHERE company='oda';

WITH duplicate_cte AS
(
SELECT*,
ROW_NUMBER()OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country,funds_raised_millions) AS row_num
FROM layoff_staging
)
DELETE
FROM duplicate_cte
WHERE row_num > 1;

CREATE TABLE `layoff_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  row_num INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * FROM layoff_staging2;
INSERT INTO layoff_staging2
SELECT *,
ROW_NUMBER()OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country,funds_raised_millions) AS row_num
FROM layoff_staging
;
DELETE
FROM layoff_staging2
WHERE row_num >1;

SELECT *
FROM layoff_staging2
WHERE row_num >1;

SELECT *
FROM layoff_staging2;

-- STANDARDIZING DATA
SELECT company,trim(company)
FROM layoff_staging2;

UPDATE layoff_staging2
SET company=trim(company);

SELECT distinct industry
FROM layoff_staging2
order by 1;
SELECT industry 
FROM layoff_staging2
where industry like 'crypto%';

UPDATE layoff_staging2
SET industry = 'crypto'
WHERE industry like 'crypto%';

SELECT distinct industry
FROM layoff_staging2
order by 1;

SELECT DISTINCT country,TRIM(trailing '.' from country)
FROM  layoff_staging2
ORDER BY 1;

UPDATE layoff_staging2
SET country=TRIM(trailing '.' from country)
where country like 'united state%';

SELECT date,
str_to_date(date,'%m/%d/%Y')
FROM layoff_staging2;

UPDATE layoff_staging2
SET date=str_to_date(date,'%m/%d/%Y');

ALTER TABLE layoff_staging2
MODIFY COLUMN date DATE;

select * 
from layoff_staging2;

-- modifying null and empty values

select * 
from layoff_staging2
WHERE total_laid_off IS NULL AND
 percentage_laid_off IS NULL;
 
select *
from layoff_staging2
WHERE industry IS NULL
OR industry ='';

select *
from layoff_staging2
WHERE company='airbnb';

select t1.industry,t2.industry
from layoff_staging2 t1
join layoff_staging2 t2 
on t1.company=t2.company and t1.location=t2.location
where t1.industry IS NULL  AND
t2.industry is not null;

UPDATE layoff_staging2 
SET industry= null
where industry='';

update layoff_staging2 t1
join layoff_staging2 t2 
on t1.company=t2.company 
SET t1.industry = t2.industry
where t1.industry is null and t2.industry is not null;

select *
from layoff_staging2
WHERE company='airbnb';

select *
from layoff_staging2;

select * 
from layoff_staging2
WHERE total_laid_off IS NULL AND
 percentage_laid_off IS NULL;
 
delete
from layoff_staging2
WHERE total_laid_off IS NULL AND
percentage_laid_off IS NULL;
 
 select *
from layoff_staging2;

alter table layoff_staging2
drop column row_num;

 select *
from layoff_staging2;

 


