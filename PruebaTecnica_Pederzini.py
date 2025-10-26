import gcsfs
import pandas as pd
from google.cloud import bigquery


PROJECT_ID = "arcane-tome-333902"
DATASET_ID = "prueba_tec"
TABLE_NAME = "Tabla_1"
TABLE_ID = F"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"
RUTA_GCS  = "gs://apexamerica-bucket/raw/Apex_Logins.csv"

#Extracción

def extraer(RUTA_GCS: str):
    print(f"Extrayendo datos de {RUTA_GCS}")
    fs = gcsfs.GCSFileSystem()
    try:
        with fs.open(RUTA_GCS,"r") as leerArchivo:
         df= pd.read_csv(leerArchivo)
        return df
    except Exception as e:
        print(f"Error al extraer: {e}")
        return pd.DataFrame()

#Tranformación

def transformacion(df,RUTA_GCS):
    print("Iniciando transformación: ")   
  
    df["Login"] = pd.to_datetime(df["Login"],errors="coerce",dayfirst=True)
    df["Logout"] = pd.to_datetime(df["Logout"],errors="coerce",dayfirst=True)
    df["fecha_ingesta"] = df["Login"].dt.date
    df["nombre_archivo"] = RUTA_GCS.split("/") [-1]

    #Agrego columna "horas" para poder realizar filtro y obterner los campos menores a 9hs

    df["horas"] = [int(str(x).split(":")[0]) for(x) in df["Tiempo total"]]
    df_validos = df[df["horas"] <= 9].copy()

     #Elimino columna "horas" luego de filtrar
    df_validos.drop(columns=["horas"],inplace=True)
    return df_validos    

#carga

def carga_bq(df_validos):
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE") 

    try:
        client = bigquery.Client(project=PROJECT_ID)
        job = client.load_table_from_dataframe( df_validos, TABLE_ID, job_config=job_config )
        job.result()
        print(" Carga a BigQuery completada")
    except Exception as e:
        print(f" Error durante la carga a BigQuery: {e}")




#Orquestación:

def run_etl_pipeline():

      #Extraer
    df_raw = extraer(RUTA_GCS)
    if df_raw.empty:
        return
    
     #Transformar
    df_validos = transformacion(df_raw,RUTA_GCS) 
    

     #Cargar
    carga_bq(df_validos)
     
     #Guardar

    df_validos.to_csv("Apex_Login_Validos.csv", index = False) 
    
     #Ejecutar
if __name__ == "__main__":
    run_etl_pipeline()