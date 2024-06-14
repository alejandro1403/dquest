/*

COVID 19 Exploracion de datos 

Datos entre Enero 10 de 2020 hasta Mayo 19 de 2024

Fuente: https://ourworldindata.org/covid-deaths
*/
------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------
-- Tabla completa ordenada por lugar y fecha.
select * 
from CovidProjectSQL..[owid-covid-data]
where continent not like ''
order by location, date desc

-- Valores que se usaran.
select continent, location, date, population, total_cases, new_cases, total_deaths, new_deaths, 
people_vaccinated, people_fully_vaccinated
from CovidProjectSQL..[owid-covid-data] 
where continent not like ''
order by 2, 3

------------------------------------------------------------------------------------------------------
------------------------------------------ Muerte y Letalidad ----------------------------------------
------------------------------------------------------------------------------------------------------
-- Letalidad histórica del COVID en cada pais por fechas.
select location, date, total_cases, total_deaths, round(total_deaths*100/total_cases, 4) as death_percentage
from CovidProjectSQL..[owid-covid-data]
where continent not like ''
order by 1, 2

-- Letalidad histórica del COVID en cada pais.
select location, max(date) as date, max(total_cases) as total_cases, max(total_deaths) as total_deaths, 
round(max(total_deaths)*100/max(total_cases), 4) as death_percentage
from CovidProjectSQL..[owid-covid-data]
where continent not like ''
group by location
order by 1

-- Países con más muertes por COVID, ordenados de mayor a menor número total de muertes.
select location, max(total_deaths) as total_death_count
from CovidProjectSQL..[owid-covid-data] 
where continent not like '' and total_deaths is not null
group by location
order by total_death_count desc

-- Mostrando la cantidad total de muertes por continente.
select continent, sum(total_deaths) as max_total_deaths
from (
select continent, location, max(total_deaths) as total_deaths
from CovidProjectSQL..[owid-covid-data]
group by continent, location
having continent not like '') as subconsulta
group by continent
order by max_total_deaths desc;

-- Muertes totales por COVID y letalidad total del COVID a nivel mundial.
select sum(new_cases) as total_global_cases, sum(new_deaths) as total_global_deaths, round(sum(new_deaths)/sum(new_cases)*100, 4) as global_death_percentage
from CovidProjectSQL..[owid-covid-data]
where continent not like ''

------------------------------------------------------------------------------------------------------
---------------------------------------------- Contagio ----------------------------------------------
------------------------------------------------------------------------------------------------------
-- Probabilidad histórica de contagio de COVID en cada pais por fechas.
select location, date, population, total_cases, round(total_cases*100/population,9) as case_percentage
from CovidProjectSQL..[owid-covid-data]
where continent not like ''
order by 1,2

-- Probabilidad histórica de contagio de COVID en cada país, ordenada de manera descendente por aquellos con la mayor probabilidad de contagio.
select location, max(population) as population, max(total_cases) as total_cases, 
round(max(total_cases)*100/max(population), 4) as case_percentage
from CovidProjectSQL..[owid-covid-data]
where continent not like '' and total_cases is not null
group by location
order by case_percentage desc

------------------------------------------------------------------------------------------------------
-------------------------------------------- Vacunación ----------------------------------------------
------------------------------------------------------------------------------------------------------
-- Población total vacunada con al menos una dosis y población total completamente vacunada en cada país hasta la fecha.
select location, max(population) as population, max(people_vaccinated) as people_vaccinated, 
max(people_fully_vaccinated) as people_fully_vaccinated
from CovidProjectSQL..[owid-covid-data] 
where continent not like ''
group by location
order by 1

-- Utilizando un CTE (Common Table Expression) para calcular el porcentaje de la población vacunada con al menos una dosis 
-- y el porcentaje de la población completamente vacunada en cada país hasta la fecha.
With pop_vac (location, population, people_vaccinated, people_fully_vaccinated)
as (
select location, max(population) as population, max(people_vaccinated) as people_vaccinated, 
max(people_fully_vaccinated) as people_fully_vaccinated
from CovidProjectSQL..[owid-covid-data] 
where continent not like ''
group by location)

select location, population,
round(people_vaccinated*100/population, 4) as people_vaccinated_percentage, 
round(people_fully_vaccinated*100/population, 4) as people_fully_vaccinated_percentage
from pop_vac
order by 1

-- Utilizando una tabla temporal para calcular el porcentaje de la población vacunada que ha recibido el esquema completo de vacunación por pais.
drop table if exists #VaccFull
create table #VaccFull (
location nvarchar(50),
population numeric,
people_vaccinated numeric,
people_fully_vaccinated numeric)

Insert into #VaccFull 
select location, max(population) as population, max(people_vaccinated) as people_vaccinated, 
max(people_fully_vaccinated) as people_fully_vaccinated
from CovidProjectSQL..[owid-covid-data] 
where continent not like ''
group by location

select location, people_vaccinated, people_fully_vaccinated, round(people_fully_vaccinated*100/people_vaccinated, 4) as full_dose_percentage
from #VaccFull
order by 1

------------------------------------------------------------------------------------------------------
--------------------------------------------- Colombia -----------------------------------------------
------------------------------------------------------------------------------------------------------
-- Utilizando dos CTE, uno con los datos de contagio y letalidad del COVID en Colombia y el otro 
-- con los datos de vacunación del COVID en Colombia, se realiza un JOIN para combinarlos, 
-- se crea una VIEW para almacenar estos resultados.
USE CovidProjectSQL 
GO
create view vw_ColombiaLatestData as
with col (location, date, population, total_cases, case_percentage, total_deaths, death_percentage)
as (
select top 1 location, date, population, total_cases, round(total_cases*100/population, 4) as case_percentage, 
total_deaths, round(total_deaths*100/total_cases, 4) as death_percentage
from CovidProjectSQL..[owid-covid-data]
where continent not like '' and location like 'colombia'
order by 2 desc),
vac (location, population, people_vaccinated, people_fully_vaccinated, people_vaccinated_percentage, 
people_fully_vaccinated_percentage , full_dose_percentage)
as (
select location, max(population) as population, max(people_vaccinated) as people_vaccinated, 
max(people_fully_vaccinated) as people_fully_vaccinated,
round(max(people_vaccinated)*100/max(population), 4) as people_vaccinated_percentage, 
round(max(people_fully_vaccinated)*100/max(population), 4) as people_fully_vaccinated_percentage,
round(max(people_fully_vaccinated)*100/max(people_vaccinated), 4) as full_dose_percentage
from CovidProjectSQL..[owid-covid-data]
where location like 'colombia'
group by location)
select c.location, date, c.population, total_cases, total_deaths, people_vaccinated, 
people_fully_vaccinated, case_percentage, death_percentage, people_vaccinated_percentage, 
people_fully_vaccinated_percentage, full_dose_percentage
from col c join
vac odp on c.location=odp.location

------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------
-- Tabla extra la cual se usara para visualizar en Tableau
select continent, location, date, population
, total_cases, total_deaths
, max(total_deaths) over(partition by location)*100/max(total_cases) over(partition by location) as final_lethality
, sum(new_deaths) over(partition by continent) as final_death_per_continent
, total_cases*100/population as case_percentage
, max(people_vaccinated) over(partition by location) as final_vaccinated
, max(people_vaccinated) over(partition by location)*100/max(population) over(partition by location) as final_vaccinated_percentage
from CovidProjectSQL..[owid-covid-data]
where continent not like ''
order by 2,3

