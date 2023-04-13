-- checking the two files(tables) imported to SQL

USE PortofilioProject
GO 
SELECT* FROM CovidDeaths$;

USE PortofilioProject
GO 
SELECT* FROM CovidVaccinations$;

-- selecting the data i am going to use from CovidDeaths table 
USE PortofilioProject
GO 
SELECT location, date, population, total_cases, new_cases, total_deaths
FROM CovidDeaths$
WHERE continent is not NULL
ORDER BY location,date;

--cleaning the data, changing the data type from varchar to bigint 

USE PortofilioProject
GO 
Alter table CovidDeaths$ alter column total_cases bigint; 
Alter table CovidDeaths$ alter column total_deaths bigint; 
Alter table CovidDeaths$ alter column date date;

Alter table CovidVaccinations$ alter column date date;
Alter table CovidVaccinations$ alter column total_tests bigint;
Alter table CovidVaccinations$ alter column new_tests bigint;
Alter table CovidVaccinations$ alter column total_vaccinations bigint;
Alter table CovidVaccinations$ alter column new_vaccinations bigint;

--- cheking for duplicates 
USE PortofilioProject
GO 
SELECT location, date, COUNT(*)
FROM CovidDeaths$
GROUP BY location, date, total_cases, total_deaths 
HAVING COUNT(*) >1;

-- no duplicates found 


-- finding total deaths vs total cases in United States.
-- since the total_cases and the total_deaths are int type i have to change it to float to give me fractions.
USE PortofilioProject
GO 
SELECT location, date, CAST(total_cases AS float) AS total_cases, 
CAST(total_deaths AS float)AS total_deaths, 
CAST(total_deaths AS float)/CAST(total_cases AS float)*100 AS deaths_percentage
FROM CovidDeaths$
WHERE location like '%states%' AND continent is not NULL
ORDER BY location,date desc;

--Death percentage globaly 

USE PortofilioProject
GO 
SELECT location, date, CAST(total_cases AS float) AS total_cases, 
CAST(total_deaths AS float)AS total_deaths, 
CAST(total_deaths AS float)/CAST(total_cases AS float)*100 AS deaths_percentage
FROM CovidDeaths$
WHERE  continent is not NULL
ORDER BY location,date;

--Total case vs population 

USE PortofilioProject
GO 
SELECT location, date, population, CAST(total_cases AS float) AS total_cases, 
CAST(total_cases AS float)/population*100 AS infected_population_percentage
FROM CovidDeaths$
WHERE  continent is not NULL
ORDER BY location,date;

-- countries with the highest infetion rate per population 

USE PortofilioProject
GO 
SELECT location, population, MAX (CAST(total_cases AS float)) AS highest_infection_number, 
MAX(CAST(total_cases AS float)/population)*100 AS infected_population_percentage
FROM CovidDeaths$
--WHERE  continent is not NULL
GROUP BY location, population
ORDER BY infected_population_percentage DESC;

--countries with the highest death count per poulation 

USE PortofilioProject
GO 
SELECT location, population, MAX (CAST(total_deaths AS float)) AS highest_death_number, 
MAX(CAST(total_deaths AS float)/population)*100 AS death_population_percentage
FROM CovidDeaths$
--WHERE  continent is not NULL
GROUP BY location, population 
ORDER BY death_population_percentage DESC; 

SELECT location, MAX (CAST(total_deaths AS float)) AS highest_death_number 
FROM CovidDeaths$ 
WHERE continent is not null 
GROUP BY location
ORDER BY highest_death_number DESC;

--which continent has more covid death 

USE PortofilioProject
GO 
SELECT continent, MAX (CAST(total_deaths AS float)) AS highest_death_number 
FROM CovidDeaths$ 
WHERE continent is not null 
GROUP BY continent 
ORDER BY highest_death_number DESC;

--using joins to combine the two tables 

USE PortofilioProject
GO 
SELECT * 
FROM CovidDeaths$ AS dea
JOIN CovidVaccinations$ AS vac
ON dea.location = vac.location
AND dea.date = vac.date

--population vs vaccination 

USE PortofilioProject
GO 
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
FROM CovidDeaths$ AS dea
JOIN CovidVaccinations$ AS vac
ON dea.location = vac.location
AND dea.date = vac.date
WHERE dea.continent is not null
ORDER BY location, date;

--to get the total number of new vaccination recieved using rolling count 

USE PortofilioProject
GO 
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations, SUM(CAST(vac.new_vaccinations AS FLOAT))  
OVER(PARTITION BY dea.location ORDER BY dea.location, dea.date) AS rolling_count_vac 
FROM CovidDeaths$ AS dea
JOIN CovidVaccinations$ AS vac
ON dea.location = vac.location
AND dea.date = vac.date
WHERE dea.continent is not null
ORDER BY location, date;

--to find what percentage of the population get new vaccine 

USE PortofilioProject
GO 
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations, SUM(CAST(vac.new_vaccinations AS FLOAT))  
OVER(PARTITION BY dea.location ORDER BY dea.location, dea.date) AS rolling_count_vac,
(SUM(CAST(vac.new_vaccinations AS FLOAT)) OVER(PARTITION BY dea.location ORDER BY dea.location, dea.date)/population)*100 AS total_vac_perc
FROM CovidDeaths$ AS dea
JOIN CovidVaccinations$ AS vac
ON dea.location = vac.location
AND dea.date = vac.date
WHERE dea.continent is not null
ORDER BY location, date;

--creat temporary table 
USE PortofilioProject
GO
CREATE TABLE vaccination_population_percentage 
(
continent NVARCHAR(255),
location NVARCHAR(255),
date DATETIME,
populatiion NUMERIC,
new_vaccinations NUMERIC, 
rolling_count_vac NUMERIC
)
INSERT INTO vaccination_population_percentage
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations, SUM(CAST(vac.new_vaccinations AS FLOAT))  
OVER(PARTITION BY dea.location ORDER BY dea.location, dea.date) AS rolling_count_vac
--(SUM(CAST(vac.new_vaccinations AS FLOAT)) OVER(PARTITION BY dea.location ORDER BY dea.location, dea.date)/population)*100 AS total_vac_perc
FROM CovidDeaths$ AS dea
JOIN CovidVaccinations$ AS vac
ON dea.location = vac.location
AND dea.date = vac.date
WHERE dea.continent is not null
ORDER BY location, date;

SELECT *, (rolling_count_vac/populatiion)*100
FROM vaccination_population_percentage

--creating view to store data i want 
USE PortofilioProject
GO 
CREATE VIEW vaccination_population_percentage_view AS 
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations, SUM(CAST(vac.new_vaccinations AS FLOAT))  
OVER(PARTITION BY dea.location ORDER BY dea.location, dea.date) AS rolling_count_vac
--(SUM(CAST(vac.new_vaccinations AS FLOAT)) OVER(PARTITION BY dea.location ORDER BY dea.location, dea.date)/population)*100 AS total_vac_perc
FROM CovidDeaths$ AS dea
JOIN CovidVaccinations$ AS vac
ON dea.location = vac.location
AND dea.date = vac.date
WHERE dea.continent is not null
--ORDER BY location, date;

SELECT* 
FROM vaccination_population_percentage_view;

-- tableau tables 
--1

USE PortofilioProject
GO 
SELECT SUM(CAST(new_cases AS float)) AS total_new_cases, 
SUM(CAST(new_deaths AS float))AS total_new_deaths, 
SUM(CAST(new_deaths AS float))/SUM(CAST(new_cases AS float))*100 AS total_deaths_percentage
FROM CovidDeaths$
WHERE  continent is not NULL
ORDER BY 1,2;

--2

USE PortofilioProject
GO 
SELECT continent, SUM(CAST(new_deaths AS FLOAT)) AS  total_Death_Count
FROM CovidDeaths$
--Where location like '%states%'
WHERE continent is NOT NULL
GROUP BY continent
order by total_Death_Count DESC

---3
USE PortofilioProject
GO 
SELECT location, population, MAX (CAST(total_cases AS float)) AS highest_infection_number, 
MAX(CAST(total_cases AS float)/population)*100 AS infected_population_percentage
FROM CovidDeaths$
WHERE  continent is not NULL
GROUP BY location, population
ORDER BY infected_population_percentage DESC;

---4
USE PortofilioProject
GO 
SELECT location, population, date, MAX (CAST(total_cases AS float)) AS highest_infection_number, 
MAX(CAST(total_cases AS float)/population)*100 AS infected_population_percentage
FROM CovidDeaths$
WHERE  continent is not NULL
GROUP BY location, population, date
ORDER BY infected_population_percentage DESC;
