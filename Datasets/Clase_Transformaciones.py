import pandas as pd

class Archivo():

    def csv_process(file: str, company: str, delim: str ):
        
        # Se lee el dataset
            df = pd.read_csv(f'{file}', delimiter= delim)

        #Generacion de campo 'id'

            df.insert(0,'id',company + df['show_id'])
            df.drop('show_id', axis=1, inplace=True)

        # Reemplazo de valores nulos en columna Rating por 'g' (general for all audiences)

            df['rating'] = df['rating'].fillna('g')

        
        # Se llevan las fechas a formato AAAA-MM-DD

            df['date_added'] = pd.to_datetime(df['date_added'])
        
        # Se lleva todo el texto del DataFrame a letra minúscula

            df = df.apply(lambda x: x.astype(str).str.lower())

        # Se divide el campo de 'duration' en 'duration_type' y 'duration_int'
            # Se divide el campo de 'duration' en 'duration_type' y 'duration_int'
            df[["duration_int", "duration_type"]] = df["duration"].str.split(pat=" ", expand=True)
            df.drop('duration', axis=1, inplace=True)
            df['duration_type'].replace({'seasons': 'season'}, inplace=True)
            df = df.apply(lambda x: x.astype(str).str.lower())

        #Cambio formato a columna 'score'
            df['score'] = pd.to_numeric(df['score'])
                     
            return df