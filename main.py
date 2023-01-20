import pandas as pd
from fastapi import FastAPI


app = FastAPI(title= 'Streaming platforms data',
                description='Disney, Amazon, Hulu & Netflix')


# Se carga la BD ya procesada al iniciar Uvicorn
@app.on_event("startup")
async def startup():
    global DF
    DF = pd.read_csv('Datasets/Streamings.csv')

# Se carga información acerca del proyecto
@app.get('/')
async def index():
    return 'PI1 - Henry - Papini Cristian'

# Se carga información sobre la API
@app.get('/about')
async def about():
    return 'API creada con FastAPI & Uvicorn'

# Consulta de cantidad de coincidencias en titulos con un keyword dado, por plataforma
# URL: /get_word_count/('plataforma','keyword')
@app.get('/get_word_count({plataforma},{keyword})')
async def get_word_count(plataforma:str, keyword:str):
        DF = pd.read_csv('Datasets/Streamings.csv')             # Se importa la BD
        
        if plataforma == 'disney':
            disney = DF['title'][DF['title'].str.contains(keyword) &                    # Se filtra por filas que contengan el keyword especificado en su titulo
                                 DF['id'].str.startswith('d')].count()                  # Se filtra por plataforma disney
            return f'Se han encontrado {disney} coincidencias en la plataforma Disney'
        
        elif plataforma == 'amazon':
            amazon = DF['title'][DF['title'].str.contains(keyword) &                    # Se filtra por filas que contengan el keyword especificado en su titulo
                                 DF['id'].str.startswith('a')].count()                  # Se filtra por plataforma amazon
            return f'Se han encontrado {amazon} coincidencias en la plataforma Amazon'
        
        elif plataforma == 'hulu':
            hulu = DF['title'][DF['title'].str.contains(keyword) &                      # Se filtra por filas que contengan el keyword especificado en su titulo
                               DF['id'].str.startswith('h')].count()                    # Se filtra por plataforma hulu
            return f'Se han encontrado {hulu} coincidencias en la plataforma Hulu'
        
        elif plataforma == 'netflix':
            netflix = DF['title'][DF['title'].str.contains(keyword) &                   # Se filtra por filas que contengan el keyword especificado en su titulo
                                  DF['id'].str.startswith('n')].count()                 # Se filtra por plataforma hulu
            return f'Se han encontrado {netflix} coincidencias en la plataforma Netflix'    
        
        else:
            return 'La plataforma ingresada no se encuentra en la base de datos'                  # Se le da a conocer al usuario en caso que se ingrese una plataforma incorrecta


# Consulta de cantidad de peliculas, por plataforma y dado un año, que tengan un score mayor a un nivel determinado
# URL: /get_score_count('plataforma',score,año)


@app.get('/get_score_count({plataforma},{score},{anio})')
async def get_score_count(plataforma:str,score: int, anio:int):
        DF = pd.read_csv('Datasets/Streamings.csv')             # Se importa la BD

        if plataforma == 'disney':
            disney = len(DF[(DF['id'].str.startswith('d')) & # Se filtra por plataforma disney
                            (DF['release_year'] == anio) &   # Se filtra por año especificado
                            (DF['score'] > score) &          # Se filtra por scores mayor al especificado
                            (DF['type'] == 'movie')])        # Se filtra por peliculas
            return f'Se han encontrado {disney} coincidencias en la plataforma Disney'
        
        elif plataforma == 'amazon':
            amazon = len(DF[(DF['id'].str.startswith('a')) & # Se filtra por plataforma amazon
                            (DF['release_year'] == anio) &   # Se filtra por año especificado
                            (DF['score'] > score) &          # Se filtra por scores mayor al especificado
                            (DF['type'] == 'movie')])        # Se filtra por peliculas
            return f'Se han encontrado {amazon} coincidencias en la plataforma Amazon'
        
        elif plataforma == 'hulu':
            hulu = len(DF[(DF['id'].str.startswith('h')) &   # Se filtra por plataforma hulu
                          (DF['release_year'] == anio) &     # Se filtra por año especificado
                          (DF['score'] > score) &            # Se filtra por scores mayor al especificado
                          (DF['type'] == 'movie')])          # Se filtra por peliculas
            return f'Se han encontrado {hulu} coincidencias en la plataforma Hulu'
        
        elif plataforma == 'netflix':
            netflix = len(DF[(DF['id'].str.startswith('n')) & # Se filtra por plataforma netflix
                             (DF['release_year'] == anio) &   # Se filtra por año especificado
                             (DF['score'] > score) &          # Se filtra por scores mayor al especificado
                             (DF['type'] == 'movie')])        # Se filtra por peliculas
            return f'Se han encontrado {netflix} coincidencias en la plataforma Netflix'    
        
        else:
            return 'La plataforma ingresada no se encuentra en la base de datos' # Se le da a conocer al usuario en caso que se ingrese una plataforma incorrecta


# Consulta de la segunda pelicula con mayor score de una plataforma dada, según orden alfabético.
# URL: /get_second_score('plataforma)

@app.get("/get_second_score({plataforma})")
async def get_second_score(plataforma:str):
        DF = pd.read_csv('Datasets/Streamings.csv')             # Se importa la BD

        if plataforma == 'disney':
            df_disney = DF[(DF['id'].str.startswith('d')) &     # Se filtra por plataforma disney
                           (DF['type'] == 'movie')]             # Se filtra por peliculas
            df_sorted = df_disney.sort_values(by=['score', 'title'], ascending=[False, True]).reset_index(drop=True) # Se la BD por score y titulo, descendiente y ascendente respectivamente
            second_score = df_sorted['title'][1]                                                                     # Se selecciona el segundo de la lista
            return f'La segunda pelicula con mayor score (segun orden alfabetico) de la plataforma Disney se llama: {second_score}'
        
        elif plataforma == 'amazon':
            df_amazon = DF[(DF['id'].str.startswith('a')) &     # Se filtra por plataforma amazon
                           (DF['type'] == 'movie')]             # Se filtra por peliculas
            df_sorted = df_amazon.sort_values(by=['score', 'title'], ascending=[False, True]).reset_index(drop=True) # Se la BD por score y titulo, descendiente y ascendente respectivamente
            second_score = df_sorted['title'][1]                                                                     # Se selecciona el segundo de la lista
            return f'La segunda pelicula con mayor score (segun orden alfabetico) de la plataforma Amazon se llama: {second_score}'
            
        elif plataforma == 'hulu':
            df_hulu = DF[(DF['id'].str.startswith('h')) &       # Se filtra por plataforma hulu
                         (DF['type'] == 'movie')]               # Se filtra por peliculas
            df_sorted = df_hulu.sort_values(by=['score', 'title'], ascending=[False, True]).reset_index(drop=True) # Se la BD por score y titulo, descendiente y ascendente respectivamente
            second_score = df_sorted['title'][1]                                                                   # Se selecciona el segundo de la lista
            return f'La segunda pelicula con mayor score (segun orden alfabetico) de la plataforma Hulu se llama: {second_score}'
        
        elif plataforma == 'netflix':
            df_netflix = DF[(DF['id'].str.startswith('n')) &    # Se filtra por plataforma hulu
                            (DF['type'] == 'movie')]            # Se filtra por peliculas
            df_sorted = df_netflix.sort_values(by=['score', 'title'], ascending=[False, True]).reset_index(drop=True) # Se la BD por score y titulo, descendiente y ascendente respectivamente
            second_score = df_sorted['title'][1]                                                                      # Se selecciona el segundo de la lista
            return f'La segunda pelicula con mayor score (segun orden alfabetico) de la plataforma Netflix se llama: {second_score}'    
        
        else:
                return 'La plataforma ingresada no se encuentra en la base de datos'    # Se le da a conocer al usuario en caso que se ingrese una plataforma incorrecta


# Consulta por la película que más duró, según año, plataforma y tipo de duración especificado.
# URL: /get_longest('plataforma', '[min, season]', año))

@app.get("/get_longest({plataforma}, {tipo_duracion}, {anio})")
async def get_longest(plataforma:str, tipo_duracion: str, anio: int):
        DF = pd.read_csv('Datasets/Streamings.csv')                 # Se importa la BD

        if plataforma == 'disney':
            df_disney = DF[(DF['id'].str.startswith('d')) &                     # Se filtra por plataforma disney
                           (DF['duration_type'].str.contains(tipo_duracion) &   # Se filtra por aquellos que tienen el tipo de duracion especificado
                           (DF['release_year'] == anio))]                       # Se filtra por aquellos lanzados el año especificado
            df_sorted = df_disney.sort_values(by=['duration_int'], ascending=False).reset_index(drop=True)  # Se ordena el listado por duración, de manera descendente
            longest = df_sorted['title'][0]                                                                 # Se elige el primer título de la lista
            return f'La de mayor duracion de la plataforma Disney en el año {anio} fue: {longest}'
        
        elif plataforma == 'amazon':
            df_amazon = DF[(DF['id'].str.startswith('a')) &                     # Se filtra por plataforma amazon
                           (DF['duration_type'].str.contains(tipo_duracion) &   # Se filtra por aquellos que tienen el tipo de duracion especificado
                           (DF['release_year'] == anio))]                       # Se filtra por aquellos lanzados el año especificado
            df_sorted = df_amazon.sort_values(by=['duration_int'], ascending=False).reset_index(drop=True) # Se ordena el listado por duración, de manera descendente
            longest = df_sorted['title'][0]                                                                # Se elige el primer título de la lista
            return f'La de mayor duracion de la plataforma Amazon en el año {anio} fue: {longest}'
            
        elif plataforma == 'hulu':
            df_hulu = DF[(DF['id'].str.startswith('h')) &                       # Se filtra por plataforma hulu
                         (DF['duration_type'].str.contains(tipo_duracion) &     # Se filtra por aquellos que tienen el tipo de duracion especificado
                         (DF['release_year'] == anio))]                         # Se filtra por aquellos lanzados el año especificado
            df_sorted = df_hulu.sort_values(by=['duration_int'], ascending=False).reset_index(drop=True)  # Se ordena el listado por duración, de manera descendente
            longest = df_sorted['title'][0]                                                               # Se elige el primer título de la lista
            return f'La de mayor duracion de la plataforma Hulu en el año {anio} fue: {longest}'
        
        elif plataforma == 'netflix':
            df_netflix = DF[(DF['id'].str.startswith('n')) &                    # Se filtra por plataforma hulu
                            (DF['duration_type'].str.contains(tipo_duracion) &  # Se filtra por aquellos que tienen el tipo de duracion especificado
                            (DF['release_year'] == anio))]                      # Se filtra por aquellos lanzados el año especificado
            df_sorted = df_netflix.sort_values(by=['duration_int'], ascending=False).reset_index(drop=True) # Se ordena el listado por duración, de manera descendente
            longest = df_sorted['title'][0]                                                                 # Se elige el primer título de la lista
            return f'La de mayor duracion de la plataforma Netflix en el año {anio} fue: {longest}'

        else:
                return 'La plataforma ingresada no se encuentra en la base de datos'    # Se le da a conocer al usuario en caso que se ingrese una plataforma incorrecta


# Consulta de cantidad de películas y serie con un rating específico.
# URL: /get_rating_count('rating')

@app.get("/get_rating_count({rating})")
async def get_rating_count(rating:str):
        DF = pd.read_csv('Datasets/Streamings.csv')             # Se importa la BD
        
        cantidad = DF['rating'][(~DF['rating'].str.contains('min')) &            # Se eliminan aquellas películas/series que contengan el valor "min" en la columna "rating"
                                (~DF['rating'].str.contains('season')) &         # Se eliminan aquellas películas/series que contengan el valor "season" en la columna "rating"   
                                (DF['rating'] == rating)].value_counts()         # Se filtran aquellas películas/series que tengan el rating especificado y se cuentan
        return f'Con este rating se encuentran {cantidad[0]} peliculas y series'

