import pandas as pd
import datetime
import pytz

def fecha_peru_hoy():
    lima_timezone = pytz.timezone('America/Lima')
    lima_time = datetime.datetime.now(lima_timezone)
    return lima_time.date()
today_string = fecha_peru_hoy().strftime('%y%m%d')
hoy = fecha_peru_hoy() 

#path=(f"bases_resultantes/{hoy}_bbdd_ucal_['2024-2']_conv_(0,1)_pagantes_(0,1)_fecha_{today_string}.xlsx")
path2=(f"bases_resultantes/{hoy}_bbdd_ucal_['2025-1']_conv_(0,1)_pagantes_(0,1)_fecha_{today_string}.xlsx")

#df = pd.read_excel(path)
df2 = pd.read_excel(path2)
"""
    'id_prometeo',
    'nombres_lead',
    'telefono',
    'fecha_registro_periodo',
    'ult_programa_interes',
    'flg_convocatoria',
    'desc_programa',
    'ult_tipf_dif_sin_contacto',
    'ult_tipf_dif_sin_contacto_2',
    'flg_charla_colegios',
    'agrupacion_tipificacion_actual',
    'val_pos',
    'DIAS_VIDA',
    'dias_sin_contacto'
"""
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
def filtrarData(base):
    base['telefono'] = base.apply(obtener_numero_valido, axis=1)
    # Filtrar las columnas que quieres conservar
    columnas_a_conservar = [
        'id_prometeo',
        'nombres_lead', 
        'telefono', 
        'ult_tipf_dif_sin_contacto',
        'ult_tipf_dif_sin_contacto_2',
        'agrupacion_tipificacion_actual',
        'DIAS_VIDA',
        'fecha_registro_periodo',
        'ult_programa_interes',
        'flg_convocatoria',
        'ult_tipf_dif_sin_contacto',
        'flg_charla_colegios',
        'val_pos',
        'dias_sin_contacto'
    ]
    
    # Crear un DataFrame nuevo con solo las columnas necesarias
    df_filter = base[columnas_a_conservar]
    return df_filter

#AON CONVOSS---------------------------------------------
def filtrarDataAON(base):
    base_AON=filtrarData(base)
    base_AON = base[base['flg_convocatoria'] == 1]
    base_AON = base_AON[base_AON['agrupacion_tipificacion_actual'].isin(
        ['VALORES_PERDIDO', 'VALORES_SIN_CONTACTO', 'VALORES_VALORACIONES_POSITIVAS'])]
    base_AON = base_AON[~base_AON['ult_tipf_dif_sin_contacto_2'].isin(
        ["Número no existe", "No quiere estudiar se confundio", "Ya es alumno UCAL", "Interes en pec", 
         "Solicita no contactar", "Destinatario erroneo", "No solicitó información",
         "No contamos con horario", "No contamos con modalidad", "Próxima campaña", "Troll", 
         "Costo muy alto - hasta 600", "No contamos con carrera Ingeneria", "No contamos con carrera Sociales", 
         "No contamos con carrera Negocios", "No contamos con carrera Salud", "No esta de acuerdo con la convalidación",
         "Deuda en UCAL", "No contamos con carrera tecnológica", "Sin contacto - supera toques"])]
    # Perform the filtering query
    columnas_a_conservar = [
        'id_prometeo',
        'nombres_lead', 
        'telefono', 
        'ult_tipf_dif_sin_contacto_2',
        'agrupacion_tipificacion_actual',
        'DIAS_VIDA'
    ]
    base_AON=base_AON[columnas_a_conservar]
    return base_AON
    # Guardar el DataFrame filtrado en un nuevo archivo Excel
#AON COLESS---------------------------------------------
def filtrarDataAONCOLE(base):
    base_AON=filtrarData(base)
    base_AON = base[base['flg_charla_colegios'] == 1]
    base_AON = base_AON[base_AON['agrupacion_tipificacion_actual'].isin(
        ['VALORES_PERDIDO', 'VALORES_SIN_CONTACTO', 'VALORES_VALORACIONES_POSITIVAS'])]
    base_AON = base_AON[~base_AON['ult_tipf_dif_sin_contacto_2'].isin(
        ["Número no existe", "No quiere estudiar se confundio", "Ya es alumno UCAL", "Interes en pec", 
         "Solicita no contactar", "Destinatario erroneo", "No solicitó información",
         "No contamos con horario", "No contamos con modalidad", "Próxima campaña", "Troll", 
         "Costo muy alto - hasta 600", "No contamos con carrera Ingeneria", "No contamos con carrera Sociales", 
         "No contamos con carrera Negocios", "No contamos con carrera Salud", "No esta de acuerdo con la convalidación",
         "Deuda en UCAL", "No contamos con carrera tecnológica", "Sin contacto - supera toques"])]
    columnas_a_conservar = [
        'id_prometeo',
        'nombres_lead', 
        'telefono', 
        'ult_tipf_dif_sin_contacto_2',
        'agrupacion_tipificacion_actual',
        'DIAS_VIDA'
    ]
    base_AON=base_AON[columnas_a_conservar]
    return base_AON
#TACTICOS BASES---------------------------------------------
def filtrarTactico(base):
    filtrarData(base)
    base['telefono'] = base.apply(obtener_numero_valido, axis=1)
    return base


path=("BASES_HSM_AON_Y_TACTICOS.xlsx")

with pd.ExcelWriter(path) as writer:
    #(filtrarDataAON(df)).to_excel(writer,sheet_name='CONVO 242', index=False)
    print("data 24.2 has been saved to 'HSM_.xlsx'")
    (filtrarDataAON(df2)).to_excel(writer, sheet_name='CONVO 251', index=False)
    (filtrarDataAONCOLE(df2)).to_excel(writer, sheet_name='COLES 251', index=False)
    print("data 25.1 has been saved to '25_1.xlsx'")
    #(filtrarData(df)).to_excel(writer,sheet_name='TACTICOS 242', index=False)
    (filtrarData(df2)).to_excel(writer, sheet_name='TACTICOS 251', index=False)
    print("data TACTICOS has been saved to '25_1.xlsx'")
    


