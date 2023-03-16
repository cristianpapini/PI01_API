## <h1 align=center> Project 01

## <h1 align=center> EDA-ETL-FastAPI-Docker Streaming Platforms

<p align="center">
<img src=https://user-images.githubusercontent.com/109157476/213493684-d39b7139-403c-4dac-873f-2505d3ec7fd9.png>
Hi! My name is Cristian Papini and this is my first individual project which is part of my training during the Bootcamp.


## Objetive
The objective of this project is to collect information from databases of different Streaming platforms, carry out an ETL process with them and then, through an API, make different kind of queries.

## Context
Given the enormous amount of content present on today's Streaming platforms, the platforms "Disney, Hulu, Amazon and Netflix" have been chosen for this process. All their titles haven been obtained with their intrinsic characteristics such as ID, duration, classification, among others and extrinsic such as the rating.

<div>
<img src="https://user-images.githubusercontent.com/110403753/209761096-8cfd888f-62a3-4de9-83ac-605f4ce0a025.png" width="200px">
<img src="https://user-images.githubusercontent.com/110403753/209761412-26b311f6-6847-48b6-8c97-6dd020e93372.png" width="200px">
<img src="https://user-images.githubusercontent.com/110403753/209761331-68019653-f285-4d73-b225-2280dbb69e83.png" width="200px">
<img src="https://user-images.githubusercontent.com/110403753/209761759-d7701724-a91c-4ac2-aecf-0d8093edff37.png" width="200px">
</div><hr>

## Workplan
The following video explains in detail the step by step of this project:
https://www.loom.com/share/745dbfe89af64147989a8313d04a339f

#### 1. EDA (Exploratory data analysis)
    
The 4 datasets from the different platforms were explored. They can be seen in the Datasets folder
Disney: disney_plus_titles-score.csv
Amazon: amazon_prime_titles-score.csv
Hulu: hulu_titles-score (2).csv
Netflix: netflix_titles-score.csv
    
#### 2. ETL (Extraction, Transform, Load)
    
The "File" Class was created with different functions to make the necessary transformations to all the datasets.
Among these are:
- Generate an id field made up of the first letter of the platform name, followed by the show_id already present in the dataset.
- Replace the null values of the rating field by the string "G" (which means "general for all audiences").
- Change the format of date fields to YYYY-mm-dd.
- Convert all text fields to lowercase.
- Convert the duration field to duration_int and duration_type.
    
#### 3. Relate the different databases and unify them
Once all the datasets were processed, they were unified to make them easier to consult in a file called "Streamings.csv", which is also found in the Datasets folder.
 
#### 4. Developing an API with FastAPI
On this occasion, the FastAPI Framework was used and the main.py file was created in which the queries were made available:
1. Calculate the number of times a keyword appears in the movie/series title, by platform </p>
2. Calculate the number of movies per platform with a score greater than XX in a given year</p>
3. Mention the title of the film with the second highest score for a given platform, according to the alphabetical order of the titles</p>
4. Mention the film that lasted the longest according to year, platform and type of duration</p>
5. Calculate the number of series and movies by rating</p>


#### 5. API Deployment
To make the consultation available to the members of the data analysis area, the API was deployed through the Deta platform, which uses Docker.
The link with which you can access the project is the following:
https://m2rhz6.deta.dev/


Without further ado, thank you very much for your attention!
