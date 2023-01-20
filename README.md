## <h1 align=center> Proyecto Individual 01

## <h1 align=center> EDA-ETL-FastAPI-Docker en Datasets de plataformas de Streaming

<p align="center">
<img src=https://user-images.githubusercontent.com/109157476/213493684-d39b7139-403c-4dac-873f-2505d3ec7fd9.png>
Hola! Mi nombre es Cristian Ignacio y este es mi primer proyecto individual, el cual forma parte del entrenamiento recibido por Henry durante el Bootcamp

## Objetivos
El objetivo del proyecto es recolectar información de bases de datos de distintas plataformas de Streamings, realizar un proceso de ETL con las mismas para luego, a través de una API, hacer diversas consultas.

## Contexto
Dado a la enorme cantidad de contenido presente en las plataformas de Streamings de hoy en día, se ha elegido para este proceso las plataformas "Disney, Hulu, Amazon y Netflix". De las cuales se obtuvieron todos sus títulos con los que cuentan junto a todas sus características intrínsecas (título, id, duración, clasificación, entre otras) y extrinsecas (como ser el rating).

<div>
<img src="https://user-images.githubusercontent.com/110403753/209761096-8cfd888f-62a3-4de9-83ac-605f4ce0a025.png" width="200px">
<img src="https://user-images.githubusercontent.com/110403753/209761412-26b311f6-6847-48b6-8c97-6dd020e93372.png" width="200px">
<img src="https://user-images.githubusercontent.com/110403753/209761331-68019653-f285-4d73-b225-2280dbb69e83.png" width="200px">
<img src="https://user-images.githubusercontent.com/110403753/209761759-d7701724-a91c-4ac2-aecf-0d8093edff37.png" width="200px">
</div><hr>

## Plan de trabajo
En el siguiente video se explica en detalle el paso a paso de este proyecto:
https://www.loom.com/share/745dbfe89af64147989a8313d04a339f

#### 1. EDA (Exploratory data analysis) Análisis exploratorio de información
    
Se exploraron los 4 datasets de las distintas plataformas. Los mismos se pueden ver en la carpeta Datasets
Disney: disney_plus_titles-score.csv
Amazon: amazon_prime_titles-score.csv
Hulu: hulu_titles-score (2).csv
Netflix: netflix_titles-score.csv
    
#### 2. ETL (Extraction, Transform, Load) Extracción, Transformación y Carga
    
Se creo la Clase Archivo con distintas funciones para hacer las transformaciones necesarias a todos los datasets.
Entre estas se encuentran:
- Generar un campo id conformado por la primera letra del nombre de la plataforma, seguido del show_id ya presente en el dataset.
- Reemplazar los valores nulos del campo rating por la string "G" (que significa "general for all audiences").
- Cambiar el formato de campos de fechas a AAAA-mm-dd.
- Convertir todos los campos de texto a minúsculas.
- Convertir el campo duration en duration_int y duration_type.
    
#### 3. Relacionar las distintas bases de datos y unificarlas
Una vez que todos los datasets estaban procesados se unificaron para hacer más fácil su consulta en un archivo llamado "Streamings.csv", el cual se encuentra tambien en la carpeta Datasets
 
#### 4. Desarrollo de una API con FastAPI
En esta ocasión se utilizó el FrameWork FastAPI y se creó archivo main.py en el cual se disponibilizaron las consultas:
1. Calcular la cantidad de veces que aparece una keyword en el título de peliculas/series, por plataforma </p>
2. Calcular la cantidad de películas por plataforma con un puntaje mayor a XX en determinado año</p>
3. Mencionar el titulo de la segunda película con mayor score para una plataforma determinada, según el orden alfabético de los títulos</p>
4. Mencionar la película que más duró según año, plataforma y tipo de duración</p>
5. Calcular la cantidad de series y películas por rating</p>


#### 5. Despliegue de la API
Para disponibilizar la consulta a los miembros del área de análisis de datos, se hizo el despliegue de la API a través la plataforma Deta, la cual utiliza Docker.
El link con el cual se puede acceder al proyecto es el siguiente:
https://m2rhz6.deta.dev/

Sin más, muchas gracias por su atención!
