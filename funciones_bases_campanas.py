import carga_info_campana as cic
import funciones_generales as fg
import yaml
import pandas as pd
from pathlib import Path
import copy

def obtener_df_de(ubicacion_base, campana_list, pagante_crm_list, convocatoria_list, fecha): 

    today_string = fg.fecha_peru_hoy().strftime('%y%m%d')
    nombre_de_la_base = f"{today_string}_{campana_list}_pagantes_{pagante_crm_list}_convo_{convocatoria_list}_fecha_{fecha}"
    file_path = f"{ubicacion_base}/{nombre_de_la_base}.csv"

    pl = Path(file_path)
    if pl.exists():
        df = pd.read_csv(file_path)

        df['id_prometeo'] = df['id_prometeo'].astype('object')
        df['numero_documento'] = df['numero_documento'].astype('object')
        df['fecha_pagante_crm'] = pd.to_datetime(df['fecha_pagante_crm'])
        df['fecha_registro_periodo'] = pd.to_datetime(df['fecha_registro_periodo'])
        df['fecha_hora_accion'] = pd.to_datetime(df['fecha_hora_accion'])

        return df
    else:
        return cic.crear_base_csv_gestiones(ubicacion_base, campana_list, pagante_crm_list, convocatoria_list, fecha)

def crear_base_leads_unicos(df_total, campana, ubicacion, fecha):
    
    df_total['sc_fecha'] = pd.to_datetime(df_total['sc_fecha'], errors='coerce')
     
    fecha_datetime = pd.to_datetime(fecha)
    df = df_total[(df_total['sc_campana'].isin(campana)) & (df_total['sc_fecha'] < fecha_datetime)]
    
    sin_contacto = ['Sin contacto', 'Corta llamada', 'No contesta', 'Buzón']
    
    val_pos = ['Interesado', 'Evaluando', 'Volver a llamar',  'Revisión de convalidación',
               'Revisión de propuesta', 'Revisión de la malla curricular', 'Indeciso']
    
    black_list = ['Lista negra', 
                  'De UCAL ',
                  'Black List',
                  'No solicitó información',
                  'Ya es alumno IEDUCA', 
                  'Destinatario erróneo', 
                  'Número no existe']
    
    pagante = ['Se inscribio', 'Pagante', 'Se inscribió']
    promesa_de_pago = ['Promesa de pago']
    perdido = ['Perdido', 'Económico', 'Ya se matriculó en otra institución',
               'No Interes - Por Competencia', 'Por salud',  'No interés - Por la malla curricular', 
               'No interés - Por Horario', 'Distancia', 'Financiamiento',
               'No Interés - Por distancia', 'No hay carrera',
               'No Interés - Por convalidación', 'Carrera de interés',
               'Competencia', 'Horario', 'Ubicación', 'Modalidad', 'Quiere instituto', 'Estudiando en otra institución'
               ]
    interes_colegios = ['1', '2', '3', '4']
    para_la_proxima = ['Aun en colegio', 'No Interés - Próximo periodo' ]
   


    groups = df.groupby('id_prometeo')
    
    columnas_calculadas = [
            'id_prometeo',
            'sin_contacto_consecutivos',
            'perdido_actual',
            'black_list_actual',
            'ult_tipf_dif_sin_contacto',
            'ult_tipf_dif_sin_contacto_2',
            'nombres_ult_dif_sin_contacto',
            'apellidos_ult_dif_sin_contacto',
            'ult_fecha_dif_sin_contacto',
            'cantidad_contactos',
            'recuperado_actual',
            'contactado',
            'nc_hasta_contacto',
            'cantidad_tipificaciones',
            'tipifs_hasta_val_pos',
            'cant_val_pos',
            'cant_val_pos-vall',
            'prim_tipif_dif_sin_contacto',
            'prim_tipif_dif_sin_contacto2',
            'prim_tipif_dif_sin_contacto_fecha',
            'flg_traslados',
            'flg_charla_colegios',
            'ingresos_web',
            'agrupacion_tipificacion_actual',
            'flg_gestionado',
            'prim_tipif_no_TI',
            'prim_tipif_no_TI2',
            'seg_tipif_no_TI',
            'seg_tipif_no_TI2',
            'fecha_primera_tipif'
            ]

    lista_calculada = [] 

    new_registro_plantilla = {col: None for col in columnas_calculadas}

    for name, group in groups:
        registro_calculado = new_registro_plantilla

        toques_sin_contacto = 0
        perdido_actual = 0
        perdido_actual = 0
        recuperado_actual = 0
        black_list_actual = 0
        ult_desc_1_dif_sc = ""
        ult_desc_2_dif_sc = ""
        nombre = ""
        apellido = ""
        ult_fecha_dif_sc = ""
        contactos = 0
        contactado = 0
        flag_primer_contacto = True
        nc_hasta_contacto_v = 0
        cantidad_tipificaciones_v = 0
        flag_primera_val_pos = True
        tipifs_hasta_val_pos_v = 0
        cantidad_valpos_v = 0
        c_valpos_m = 0
        pc1 = ""
        pc2 = ""
        pcf = ""
        flg_traslado = 0
        flg_charla_colegio = 0
        ingreso_web = 0
        agrupacion_tipf = ""
        pagante_crm = 0
        flg_gestionado_v = 0
        prim_tipif_no_TI_v = ""
        seg_tipif_no_TI_v = ""

        prim_tipif_no_TI2_v = ""
        seg_tipif_no_TI2_v = ""
        primer_toque_no_TI = False
        segundo_toque_no_TI = False
        fecha_prim_tipif = ""
        flg_prim_tipf = False
        for index, row in group.iterrows():
            desc_1 = row['desc_resultado_1']
            desc_2 = row['desc_resultado_2']

            nombre_asesor = row['nombre_asesor']
            
            if not flg_prim_tipf:
                fecha_prim_tipif = row['fecha_hora_accion']
                flg_prim_tipf = True

            cantidad_tipificaciones_v += 1
            if row['pagante_crm'] == 1:
                pagante_crm = 1
            if row['desc_canal'] == 'Traslados':
                flg_traslado = 1
            
            if row['desc_canal'] == 'Charla Colegio':
                flg_charla_colegio = 1
           
            if not((desc_1 == "Interesado" or desc_1 == "Evaluando") and nombre_asesor == "TI"):
                flg_gestionado_v = 1
            ## codigo de rodri, remover
            
            if row['desc_canal'] in ['Traslados', 'Telemarketing'] and primer_toque_no_TI and not segundo_toque_no_TI:
                seg_tipif_no_TI_v = desc_1
                seg_tipif_no_TI2_v = row['fecha_hora_accion']
                segundo_toque_no_TI = True
            if row['desc_canal'] in ['Traslados', 'Telemarketing'] and not primer_toque_no_TI:
                prim_tipif_no_TI_v = desc_1
                prim_tipif_no_TI2_v = row['fecha_hora_accion']
                primer_toque_no_TI = True
            
            if desc_1 in sin_contacto:
                toques_sin_contacto += 1
            elif nombre_asesor != "TI":
                contactos += 1
                contactado = 1
                if flag_primer_contacto:
                    flag_primer_contacto = False
                    nc_hasta_contacto_v = cantidad_tipificaciones_v - 1 
                    pc1 = desc_1
                    pc2 = desc_2
                    pcf = row['fecha_hora_accion']
                toques_sin_contacto = 0
                ult_desc_1_dif_sc = desc_1
                ult_desc_2_dif_sc = desc_2
                nombre = row['nombre_asesor']
                apellido = row['apellidos_asesor']
                ult_fecha_dif_sc = row['fecha_hora_accion']
    
            if desc_1 in perdido:
                if black_list_actual == 0:
                    perdido_actual = 1
                recuperado_actual = 0
            elif desc_1 in black_list:
                black_list_actual = 1
                recuperado_actual = 0
                if perdido_actual:
                    perdido_actual = 0
            elif desc_1 in val_pos and row['nombre_asesor'] != "TI":
                if desc_1 != "Volver a llamar":
                    c_valpos_m += 1
                cantidad_valpos_v += 1
                if perdido_actual:
                    perdido_actual = 0
                    recuperado_actual = 1
                if black_list_actual:
                    black_list_actual = 0
                    recuperado_actual = 1
                if flag_primera_val_pos:
                    flag_primera_val_pos = False
                    tipifs_hasta_val_pos_v = cantidad_tipificaciones_v - 1
            elif desc_1 in val_pos and row['nombre_asesor'] == "TI":
                ingreso_web +=1

        if perdido_actual == 1:
            agrupacion_tipf = "VALORES_PERDIDO"
        if black_list_actual == 1:
            agrupacion_tipf = "VALORES_BLACK_LIST"
        if ult_desc_1_dif_sc in val_pos:
            agrupacion_tipf = "VALORES_VALORACIONES_POSITIVAS"
        if ult_desc_1_dif_sc == "":
            agrupacion_tipf = "VALORES_SIN_CONTACTO"
        if ult_desc_1_dif_sc in pagante or pagante_crm == 1:
            agrupacion_tipf = "VALORES_PAGANTE"
        if ult_desc_1_dif_sc in promesa_de_pago:
            agrupacion_tipf = "VALORES_PROMESA_DE_PAGO"
        if ult_desc_1_dif_sc in interes_colegios:
            agrupacion_tipf = "VALORES_VALORACIONES_POSITIVAS"

        registro_calculado['id_prometeo'] = row['id_prometeo']
        registro_calculado['sin_contacto_consecutivos'] = toques_sin_contacto
        registro_calculado['perdido_actual'] = perdido_actual
        registro_calculado['black_list_actual'] = black_list_actual
        registro_calculado['ult_tipf_dif_sin_contacto'] = ult_desc_1_dif_sc
        registro_calculado['ult_tipf_dif_sin_contacto_2'] =ult_desc_2_dif_sc
        registro_calculado['nombres_ult_dif_sin_contacto'] = nombre
        registro_calculado['apellidos_ult_dif_sin_contacto'] = apellido
        registro_calculado['ult_fecha_dif_sin_contacto'] = ult_fecha_dif_sc
        registro_calculado['cantidad_contactos'] = contactos
        registro_calculado['recuperado_actual'] = recuperado_actual
        registro_calculado['contactado'] = contactado
        registro_calculado['nc_hasta_contacto'] = nc_hasta_contacto_v
        registro_calculado['cantidad_tipificaciones'] = cantidad_tipificaciones_v
        registro_calculado['tipifs_hasta_val_pos'] = tipifs_hasta_val_pos_v
        registro_calculado['cant_val_pos'] = cantidad_valpos_v
        registro_calculado['cant_val_pos-vall'] = c_valpos_m
        registro_calculado['prim_tipif_dif_sin_contacto'] = pc1
        registro_calculado['prim_tipif_dif_sin_contacto2'] = pc2
        registro_calculado['prim_tipif_dif_sin_contacto_fecha'] = pcf
        registro_calculado['flg_traslados'] = flg_traslado
        registro_calculado['flg_charla_colegios'] = flg_charla_colegio
        registro_calculado['ingresos_web'] = ingreso_web
        registro_calculado['agrupacion_tipificacion_actual'] = agrupacion_tipf
        registro_calculado['flg_gestionado'] = flg_gestionado_v
        registro_calculado['prim_tipif_no_TI'] = prim_tipif_no_TI_v
        registro_calculado['prim_tipif_no_TI2'] =prim_tipif_no_TI2_v
        registro_calculado['seg_tipif_no_TI'] = seg_tipif_no_TI_v
        registro_calculado['seg_tipif_no_TI2'] = seg_tipif_no_TI2_v
        registro_calculado['fecha_primera_tipif'] = fecha_prim_tipif
        lista_calculada.append(copy.deepcopy(registro_calculado))

    
    df_calculado = pd.DataFrame(lista_calculada)
    print("dim df2: ", df_calculado.shape)
    df_resultado = df.loc[df.groupby('id_prometeo')['fecha_hora_accion'].idxmax()].reset_index(drop=True)
    print("uniquue id_prometeo in df: ", df_resultado['id_prometeo'].nunique())
    print("uniquue id_prometeo in df2: ", df_calculado['id_prometeo'].nunique())
    df_resultado = pd.merge(df_resultado, df_calculado, on='id_prometeo', how='inner')
    print(df_resultado.shape)
    print(df_resultado['id_prometeo'].nunique())
    df_resultado['fecha_registro_periodo'] = pd.to_datetime(df_resultado['fecha_registro_periodo'])
    # df_resultado['fecha_registro_periodo'] = df_resultado['fecha_registro_periodo'].dt.date
    df_resultado['ult_fecha_dif_sin_contacto'] = pd.to_datetime(df_resultado['ult_fecha_dif_sin_contacto'])#.dt.date
    # Obtener el lunes de la semana de la fecha de ingreso
    df_resultado['val_pos'] = 0
    df_resultado.loc[(df_resultado['ult_tipf_dif_sin_contacto'].isin(val_pos)) & (df_resultado['nombres_ult_dif_sin_contacto'] != "TI"), 'val_pos'] = 1

    print(df_resultado['fecha_registro_periodo'].dtype)
    today_string = fg.fecha_peru_hoy().strftime('%y%m%d')
 
    df_resultado['DIAS_VIDA'] = (fg.fecha_peru_hoy() - df_resultado['fecha_registro_periodo'].dt.date).dt.days
    df_resultado['dias_sin_contacto'] = (fg.fecha_peru_hoy() - df_resultado['ult_fecha_dif_sin_contacto'].dt.date).dt.days

    df_resultado.to_excel(f"{ubicacion}/{fecha}_bbdd_ucal_{campana}_conv_(0,1)_pagantes_(0,1)_fecha_{today_string}.xlsx", index=False)
    return df_resultado
