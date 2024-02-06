# Weather trends in France: an in-depth SQL analysis


## Overview
This SQL project focuses on analyzing weather data collected from three French cities (Paris, Lyon and Nice), combining information from from two datasets, such as latitude, longitude, 
datetime, temperature, wind speed, and more. 
The project includes various SQL queries and analyses to extract insights from the datasets. 
The goal here is to gain insights into weather patterns, identify trends, and answer specific questions related to the provided datasets. It is also about understanding and analysing 
weather patterns.

## The data
The data comes from [Meteomatics](https://www.meteomatics.com/), a leading provider of meteorological data. Two datasets are used for this project: 'temperature_france' and 'weather_france'.
The 'temperature_france' dataset contains 1299 rows and 6 columns, whereas 'weather_france' contains 1299 rows and 10 columns.

## Project structure
The project is organized into the following sections:
1.	Dataset information: a brief overview of the dataset, including the columns and data types
2.	Data exploration: exploratory queries and analyses to understand the structure of the data
3.	Further analysis and insights: detailed SQL queries and analyses performed to derive insights from the dataset
4.	Advanced SQL queries: complex queries using advanced SQL features such as window functions, CTEs, subqueries, and aggregations

## Questions answered
The analysis aimed to answer several questions, including – but not limited to:
* How does temperature vary across different locations and time periods?
* Are there any patterns in wind speed and direction?
* Can we identify days with extreme weather conditions?
* How does UV index change with the time of day?

## Skills used
* Data analysis
* Data manipulation
* Data type conversion
* Joins
* Filters
* Aggregate functions
* Window functions
* Common table expressions (CTE)
* Subqueries
* CASE statements
* Views

## Technical tools used
* Microsoft SQL Server: used for analysing and manipulating the data
* Microsoft SQL Server Management Studio: used for storing the datasets
* Data Source: Meteomatics API for weather data