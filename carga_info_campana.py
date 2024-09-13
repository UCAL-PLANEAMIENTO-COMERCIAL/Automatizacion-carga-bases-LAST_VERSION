import yaml
import psycopg2
import pandas as pd
import re
import os
import pytz
import datetime
import query_data_variables as qdv
import funciones_generales as fg


def crear_base_csv_gestiones(ubicacion_base, campana_list, pagante_crm_list, convocatoria_list, fecha):
    
    with open("config_files/.credenciales.yaml", 'r') as file:
        config = yaml.safe_load(file)
    
    # Database configuration
    db_config = config['database']    
    conn = psycopg2.connect(
        dbname=db_config['name'],
        user=db_config['user'],
        password=db_config['password'],
        host=db_config['host'],
        port=db_config['port'])

    print(campana_list + " " + pagante_crm_list + " " + convocatoria_list)
    print("Procesando query...")
    query = qdv.get_query(campana_list, convocatoria_list , pagante_crm_list, fecha)
    df = pd.read_sql_query(query, conn)
    conn.close()
    print("Query procesada!")
    
    df = df.dropna(subset=['id_prometeo','sc_fecha', 'hora_seguimiento'])
    df['hora_seguimiento'] = pd.to_datetime(df['hora_seguimiento']).dt.time
    df['sc_fecha'] =  pd.to_datetime(df['sc_fecha']).dt.date
    df['fecha_hora_accion'] = df.apply(lambda x: pd.to_datetime(x['sc_fecha'].strftime('%Y-%m-%d') + ' ' + x['hora_seguimiento'].strftime('%H:%M:%S')), axis=1)
    df['fecha_registro_periodo'] = pd.to_datetime(df['fecha_registro_periodo'])
    
    today_string = fg.fecha_peru_hoy().strftime('%y%m%d')
    nombre_de_la_base = f"{today_string}_{campana_list}_pagantes_{pagante_crm_list}_convo_{convocatoria_list}_fecha_{fecha}"
    print("Guardando en csv...")
    df.to_csv(f"{ubicacion_base}/{nombre_de_la_base}.csv",index=False)
    print("Guardado en csv!")
    return df

