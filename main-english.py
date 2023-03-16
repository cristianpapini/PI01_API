import pandas as pd
from fastapi import FastAPI


app = FastAPI(title= 'Streaming platforms data',
                description='Disney, Amazon, Hulu & Netflix')


# The already processed DB is loaded when starting Uvicorn
@app.on_event("startup")
async def startup():
    global DF
    DF = pd.read_csv('Datasets/Streamings.csv')

# Information about the project is loaded
@app.get('/')
async def index():
    return 'PI1 - Henry - Papini Cristian'

# Information about the API is loaded
@app.get('/about')
async def about():
    return 'API created with FastAPI & Uvicorn'

# Query the number of matches in titles with a given keyword, by platform
# URL: /get_word_count/('plataforma','keyword')
@app.get('/get_word_count({plataforma},{keyword})')
async def get_word_count(plataforma:str, keyword:str):
        DF = pd.read_csv('Datasets/Streamings.csv')             # DB is imported
        
        if plataforma == 'disney':
            disney = DF['title'][DF['title'].str.contains(keyword) &                    # It is filtered by rows that contain the keyword specified in its title
                                 DF['id'].str.startswith('d')].count()                  # Filtered by Disney platform
            return f'{disney} matches have been found on the platform Disney'
        
        elif plataforma == 'amazon':
            amazon = DF['title'][DF['title'].str.contains(keyword) &                    # It is filtered by rows that contain the keyword specified in its title
                                 DF['id'].str.startswith('a')].count()                  # Filtered by Amazon platform
            return f'{amazon} matches have been found on the Amazon platform'
        
        elif plataforma == 'hulu':
            hulu = DF['title'][DF['title'].str.contains(keyword) &                      # It is filtered by rows that contain the keyword specified in its title
                               DF['id'].str.startswith('h')].count()                    # Filtered by Hulu platform
            return f'{hulu} matches have been found on the Hulu platform'
        
        elif plataforma == 'netflix':
            netflix = DF['title'][DF['title'].str.contains(keyword) &                   # It is filtered by rows that contain the keyword specified in its title
                                  DF['id'].str.startswith('n')].count()                 # Filtered by Netflix platform
            return f'{netflix} matches have been found on the Netflix platform'    
        
        else:
            return 'The platform entered is not found in the database'                  # It is made known to the user in case an incorrect platform is entered


# Query the number of films, by platform and given a year, that have a score greater than a certain level
# URL: /get_score_count('plataforma',score,año)


@app.get('/get_score_count({plataforma},{score},{anio})')
async def get_score_count(plataforma:str,score: int, anio:int):
        DF = pd.read_csv('Datasets/Streamings.csv')             # DB is imported

        if plataforma == 'disney':
            disney = len(DF[(DF['id'].str.startswith('d')) & # Filter by Disney platform
                            (DF['release_year'] == anio) &   # Filter by specified year
                            (DF['score'] > score) &          # Filter by scores higher than specified
                            (DF['type'] == 'movie')])        # Filter the ones with type = "movie"
            return f'{disney} matches have been found on the Disney platform'
        
        elif plataforma == 'amazon':
            amazon = len(DF[(DF['id'].str.startswith('a')) & # Filter by Amazon platform
                            (DF['release_year'] == anio) &   # Filter by specified year
                            (DF['score'] > score) &          # Filter by scores higher than specified
                            (DF['type'] == 'movie')])        # Filter the ones with type = "movie"
            return f'{amazon} matches have been found on the Amazon platform'
        
        elif plataforma == 'hulu':
            hulu = len(DF[(DF['id'].str.startswith('h')) &   # Filter by Hulu Platform
                          (DF['release_year'] == anio) &     # Filter by specified year
                          (DF['score'] > score) &            # Filter by scores higher than specified
                          (DF['type'] == 'movie')])          # Filter the ones with type = "movie"
            return f'{hulu} matches have been found on the Hulu platform'
        
        elif plataforma == 'netflix':
            netflix = len(DF[(DF['id'].str.startswith('n')) & # Filter by Netflix platform
                             (DF['release_year'] == anio) &   # Filter by specified year
                             (DF['score'] > score) &          # Filter by scores higher than specified
                             (DF['type'] == 'movie')])        # Filter the ones with type = "movie"
            return f'{netflix} matches have been found on the Netflix platform'    
        
        else:
            return 'The platform entered is not found in the database' # It is made known to the user in case an incorrect platform is entered


# Consulta de la segunda pelicula con mayor score de una plataforma dada, según orden alfabético.
# URL: /get_second_score('plataforma)

@app.get("/get_second_score({plataforma})")
async def get_second_score(plataforma:str):
        DF = pd.read_csv('Datasets/Streamings.csv')             # DB is imported

        if plataforma == 'disney':
            df_disney = DF[(DF['id'].str.startswith('d')) &     # Filter by Disney platform
                           (DF['type'] == 'movie')]             # Filter by movies
            df_sorted = df_disney.sort_values(by=['score', 'title'], ascending=[False, True]).reset_index(drop=True) # Sorted by score and title, descending and ascending respectively
            second_score = df_sorted['title'][1]                                                                     # The second from the list is selected
            return f'La segunda pelicula con mayor score (segun orden alfabetico) de la plataforma Disney se llama: {second_score}'
        
        elif plataforma == 'amazon':
            df_amazon = DF[(DF['id'].str.startswith('a')) &     # Filter by Amazon platform
                           (DF['type'] == 'movie')]             # Filter by movies
            df_sorted = df_amazon.sort_values(by=['score', 'title'], ascending=[False, True]).reset_index(drop=True) # Sorted by score and title, descending and ascending respectively
            second_score = df_sorted['title'][1]                                                                     # The second from the list is selected
            return f'La segunda pelicula con mayor score (segun orden alfabetico) de la plataforma Amazon se llama: {second_score}'
            
        elif plataforma == 'hulu':
            df_hulu = DF[(DF['id'].str.startswith('h')) &       # Filter by Hulu platform
                         (DF['type'] == 'movie')]               # Filter by movies
            df_sorted = df_hulu.sort_values(by=['score', 'title'], ascending=[False, True]).reset_index(drop=True) # Sorted by score and title, descending and ascending respectively
            second_score = df_sorted['title'][1]                                                                   # The second from the list is selected
            return f'La segunda pelicula con mayor score (segun orden alfabetico) de la plataforma Hulu se llama: {second_score}'
        
        elif plataforma == 'netflix':
            df_netflix = DF[(DF['id'].str.startswith('n')) &    # Filter by Netflix platform
                            (DF['type'] == 'movie')]            # Filter by movies
            df_sorted = df_netflix.sort_values(by=['score', 'title'], ascending=[False, True]).reset_index(drop=True) # Sorted by score and title, descending and ascending respectively
            second_score = df_sorted['title'][1]                                                                      # The second from the list is selected
            return f'La segunda pelicula con mayor score (segun orden alfabetico) de la plataforma Netflix se llama: {second_score}'    
        
        else:
                return 'The platform entered is not found in the database' # It is made known to the user in case an incorrect platform is entered


# Query for the movie that lasted the longest, according to the year, platform and type of duration specified.
# URL: /get_longest('plataforma', '[min, season]', año))

@app.get("/get_longest({plataforma}, {tipo_duracion}, {anio})")
async def get_longest(plataforma:str, tipo_duracion: str, anio: int):
        DF = pd.read_csv('Datasets/Streamings.csv')                 # DB is imported

        if plataforma == 'disney':
            df_disney = DF[(DF['id'].str.startswith('d')) &                     # Filter by Disney platform
                           (DF['duration_type'].str.contains(tipo_duracion) &   # Filter by those that have the specified duration type
                           (DF['release_year'] == anio))]                       # Se filtra por aquellos lanzados el año especificado
            df_sorted = df_disney.sort_values(by=['duration_int'], ascending=False).reset_index(drop=True)  # The list is ordered by duration, in descending order.
            longest = df_sorted['title'][0]                                                                 # The first title in the list is chosen
            return f'La de mayor duracion de la plataforma Disney en el año {anio} fue: {longest}'
        
        elif plataforma == 'amazon':
            df_amazon = DF[(DF['id'].str.startswith('a')) &                     # Filter by Amazon platform
                           (DF['duration_type'].str.contains(tipo_duracion) &   # Filter by those that have the specified duration type
                           (DF['release_year'] == anio))]                       # Filter by those released in the specified year
            df_sorted = df_amazon.sort_values(by=['duration_int'], ascending=False).reset_index(drop=True) # The list is ordered by duration, in descending order.
            longest = df_sorted['title'][0]                                                                # The first title in the list is chosen
            return f'La de mayor duracion de la plataforma Amazon en el año {anio} fue: {longest}'
            
        elif plataforma == 'hulu':
            df_hulu = DF[(DF['id'].str.startswith('h')) &                       # Filter by Hulu platform
                         (DF['duration_type'].str.contains(tipo_duracion) &     # Filter by those that have the specified duration type
                         (DF['release_year'] == anio))]                         # Filter by those released in the specified year
            df_sorted = df_hulu.sort_values(by=['duration_int'], ascending=False).reset_index(drop=True)  # The list is ordered by duration, in descending order.
            longest = df_sorted['title'][0]                                                               # The first title in the list is chosen
            return f'La de mayor duracion de la plataforma Hulu en el año {anio} fue: {longest}'
        
        elif plataforma == 'netflix':
            df_netflix = DF[(DF['id'].str.startswith('n')) &                    # Filter by Netflix platform
                            (DF['duration_type'].str.contains(tipo_duracion) &  # Filter by those that have the specified duration type
                            (DF['release_year'] == anio))]                      # Filter by those released in the specified year
            df_sorted = df_netflix.sort_values(by=['duration_int'], ascending=False).reset_index(drop=True) # The list is ordered by duration, in descending order.
            longest = df_sorted['title'][0]                                                                 # The first title in the list is chosen
            return f'La de mayor duracion de la plataforma Netflix en el año {anio} fue: {longest}'

        else:
                return 'The platform entered is not found in the database' # It is made known to the user in case an incorrect platform is entered


# Query the number of movies and series with a specific rating.
# URL: /get_rating_count('rating')

@app.get("/get_rating_count({rating})")
async def get_rating_count(rating:str):
        DF = pd.read_csv('Datasets/Streamings.csv')             # DB is imported
        
        cantidad = DF['rating'][(~DF['rating'].str.contains('min')) &            # Movies/series that contain the value "min" in the "rating" column are eliminated
                                (~DF['rating'].str.contains('season')) &         # Movies/series that contain the value "season" in the "rating" column are eliminated
                                (DF['rating'] == rating)].value_counts()         # Movies/series that have the specified rating are filtered and counted
        return f'With this rating you can find {cantidad[0]} movies and series'
