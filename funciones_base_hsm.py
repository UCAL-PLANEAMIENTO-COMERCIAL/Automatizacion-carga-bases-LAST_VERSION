import funciones_generales as fg
import pandas as pd



def es_numero_valido(numero):

    return pd.notna(numero) and len(str(numero)) == 9 and str(numero).startswith("9")

def obtener_numero_valido(fila):
    
    if es_numero_valido(fila['celular2']):
        return fila['celular2']
    elif es_numero_valido(fila['celular']):
        return fila['celular']
    elif es_numero_valido(fila['telefono']):
        return fila['telefono']
    else:
        return None 
    
def crear_bases_hsm(df, ubicacion):
    df_AON = df[df['flg_convocatoria'] == 1]
    df_AON['telefono'] = df_AON.apply(obtener_numero_valido, axis=1)
    df_AON = df_AON.filter([
                         'id_prometeo',
                         'nombres', 
                         'telefono', 
                         'flg_convocatoria',
                         'ult_tipf_dif_sin_contacto_2',
                         'agrupacion_tipificacion_actual',
                         'DIAS_VIDA'])
    
    # Obtener número válido en una nueva columna
    
    #id_prometeo	nombres_lead	telefono	ult_tipf_dif_sin_contacto_2	agrupacion_tipificacion_actual	DIAS_VIDA

        
    valid_agrupaciones = ['VALORES_PERDIDO', 'VALORES_SIN_CONTACTO', 'VALORES_VALORACIONES_POSITIVAS']
    invalid_tipificaciones = [
        "Número no existe", "No quiere estudiar se confundio", "Ya es alumno UCAL", "Interes en pec", 
        "Solicita no contactar", "Destinatario erroneo", "Aún en colegio", "No solicitó información",
        "No contamos con horario", "No contamos con modalidad", "Próxima campaña", "Troll", 
        "Costo muy alto - hasta 600", "No contamos con carrera Ingeneria", "No contamos con carrera Sociales", 
        "No contamos con carrera Negocios", "No contamos con carrera Salud", "No esta de acuerdo con la convalidación",
        "Deuda en UCAL", "No contamos con carrera tecnológica", "Sin contacto - supera toques"
    ]

    # Filtrar los datos según las condiciones
    df_AON = df_AON.query(
        "agrupacion_tipificacion_actual in @valid_agrupaciones & \
        ult_tipf_dif_sin_contacto_2 not in @invalid_tipificaciones"
    )


    today_string = fg.fecha_peru_hoy().strftime('%y%m%d')

    with pd.ExcelWriter(f"{ubicacion}/{today_string}__BASE_HSM_AON_TACTICOS.xlsx") as writer:
        df_AON.to_excel(writer, sheet_name='CONVO_242.2', index=False)

