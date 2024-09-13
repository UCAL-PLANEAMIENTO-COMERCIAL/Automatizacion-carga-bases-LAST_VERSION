import psycopg2
import pandas as pd
import re
import os
import pytz
import datetime

def borrar_carpeta(folder):
    for filename in os.listdir(folder):
        file_path = os.path.join(folder, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % (file_path, e))

def is_valid_telephone_number(number):
    pattern = r'^9\d{8}$'
    if bool(re.match(pattern, str(number))):
        return 1
    return 0

def crear_base_del_dia(ubicacion):
    conn = psycopg2.connect(
        dbname='prd',
        user='pbiuser',
        password='U6id3HLXmvWn',
        host='ieduca-dl-redshift-cluster.cxyqhjo3st83.us-east-1.redshift.amazonaws.com',
        port='5439')
    
    ################consulta a redshift
    campana = "('2024-2')"
    pagante_crm = "(0)"
    convocatoria = "(0,1)"
    query = f"""
    SELECT  
        gs.sc_prospecto as id_prometeo,
        a.id_seguimiento,
        a.sc_seguimiento,
        a.tipo_seguimiento,
        a.id_participacion,
        s.nombres +' ' +s.apellido_paterno + ' ' + s.apellido_materno as Nombres_Lead,
        s.telefono,
        s.email,
        s.numero_documento,
        gs.pagante_crm,
        gs.fecha_pagante_crm,
        gs.fecha_registro_periodo,
        s.email,
        COALESCE(s.itr_llamada,0) as itr_llamada,
        COALESCE(s.itr_chat,0) as itr_chat,
        COALESCE(s.itr_llamada,0) + COALESCE(s.itr_chat,0) as total_interacciones,
        gs.programa_ult as ult_programa_interes,
        gs.resultado_primer_contacto,
    	gs.tipo_registro,
    	gs.canal_atribucion,
        gs.subcanal,
        gs.flg_convocatoria,
        a.id_resultado1,
        l1.desc_resultado as desc_resultado_1, 
        a.id_resultado2,
        l2.desc_resultado as desc_resultado_2, 
        a.contactabilidad,
        a.cod_interes,
        a.id_fecha_seguimiento,
        f.sc_fecha, 
        a.hora_seguimiento,
        a.id_periodo,
        b.id_campana,
        c.sc_campana,
        a.id_programa,
        h.desc_programa,
        i.desc_nivel,
        a.id_canal,
        d.desc_canal,
        a.id_sede,
        a.id_turno,
        a.id_asesor,
        g.nombres,
        g.apellidos,
        g.email,
        g.id_unidad_negocio as UNE_asesor,
        g.fecha_creacion as Fecha_creacion_asesor,
        g.perfil,
        a.id_unidad_negocio,
        j.desc_unidad_negocio,
        a.tipo_matricula,
        a.codigo_origen,
        a.efectividad_validez,
        a.desc_interes,
        pc.codigo_modular,
        s.anio_fin_colegio,
        s.utm_campana,
        s.sc_participacion
    FROM 
        comercial.dts_kpi_comer_fact_seguimiento as a
    LEFT JOIN financiero.dts_kpi_dim_periodo as b ON a.id_periodo=b.id_periodo 
    LEFT JOIN financiero.dts_kpi_dim_campana as c ON b.id_campana=c.id_campana 
    LEFT JOIN financiero.dts_kpi_dim_canal as d ON a.id_canal=d.id_canal 
    LEFT JOIN financiero.dts_kpi_dim_resultado as l1 ON a.id_resultado1=l1.id_resultado 
    LEFT JOIN financiero.dts_kpi_dim_resultado as l2 ON a.id_resultado2=l2.id_resultado 
    LEFT JOIN financiero.dts_kpi_dim_asesor as g ON a.id_asesor =g.id_asesor 
    LEFT JOIN financiero.dts_kpi_dim_programa as h ON a.id_programa =h.id_programa 
    LEFT JOIN financiero.dts_kpi_dim_nivel as i ON h.id_nivel =i.id_nivel 
    LEFT JOIN financiero.dts_kpi_dim_unidad_negocio as j ON a.id_unidad_negocio =j.id_unidad_negocio 
    LEFT JOIN financiero.dts_kpi_dim_fecha as f ON a.id_fecha_seguimiento = f.id_fecha
    LEFT JOIN comercial.dts_kpi_comer_fact_semantica as s on a.id_participacion = s.id_participacion 
    LEFT JOIN comercial.dts_aws_kpi_comer_fact_gestion_campana as gs on a.id_participacion =gs.id_participacion  
    LEFT JOIN comercial.dts_kpi_comer_fact_colegios as pc on a.id_participacion = pc.id_participacion
    WHERE 
        j.desc_unidad_negocio ='UCAL'
        AND c.sc_campana in {campana}
        AND gs.flg_convocatoria in {convocatoria}
        AND gs.pagante_crm in {pagante_crm}
        
    ORDER BY f.sc_fecha ASC;
    """
    df = pd.read_sql_query(query, conn)
    ########### cerrar conexion
    conn.close()
    df = df.dropna(subset=['id_prometeo','id_fecha_seguimiento', 'hora_seguimiento'])
    df['hora_seguimiento'] = pd.to_datetime(df['hora_seguimiento']).dt.time
    df['sc_fecha'] =  pd.to_datetime(df['sc_fecha']).dt.date
    df['fecha_hora_accion'] = df.apply(lambda x: pd.to_datetime(x['sc_fecha'].strftime('%Y-%m-%d') + ' ' + x['hora_seguimiento'].strftime('%H:%M:%S')), axis=1)
    df['fecha_registro_periodo'] = pd.to_datetime(df['fecha_registro_periodo'])
    nan_values = df['id_prometeo'].isna().sum()
    
    # In[42]:
    
    
    df.sort_values(by=['id_prometeo', 'fecha_hora_accion'], inplace=True)
    
    
    # In[43]:
    
    
    sin_contacto = ['Sin contacto', 'Corta llamada', 'No contesta', 'Buzón',]
    
    val_pos = ['Interesado', 'Evaluando', 'Volver a llamar',  'Revisión de convalidación',
               'Revisión de propuesta', 'Revisión de la malla curricular', 'Indeciso']
    
    black_list = ['Lista negra', 'De UCAL ',
                  'Black List', 
                  'Se inscribio',
                  'No solicitó información',
                  'Pagante', 'Se inscribió', 'Ya es alumno IEDUCA', 'Destinatario erróneo', 'Número no existe']
    
    perdido = ['Perdido', 'Económico', 'Ya se matriculó en otra institución',
               'No Interes - Por Competencia', 'Por salud',  'No interés - Por la malla curricular', 
               'No interés - Por Horario', 'Distancia', 'Financiamiento',
               'No Interés - Por distancia', 'No hay carrera',
               'No Interés - Por convalidación', 'Carrera de interés',
               'Competencia', 'Horario', 'Ubicación', 'Modalidad', 'Quiere instituto', 'Estudiando en otra institución'
               ]
    para_la_proxima = ['Aun en colegio', '2', '1', '3', '4', 'No Interés - Próximo periodo' ]
    
    
    # In[44]:
    
    
    groups = df.groupby('id_prometeo')
    
    
    clmn_toques_sc = []
    perdidos_actuales = []
    black_list_actuales = []
    clm_ur1_nsc = []
    clm_ur2_nsc = []
    nombres_ult_dif_sin_contacto = []
    apellidos_ult_dif_sin_contacto = []
    clm_uf_nsc = []
    clm_contactos_lead = []
    recuperados_actuales = []
    contactados_actuales = []
    primera_fecha = []
    nc_hasta_contacto = []
    cantidad_tipificaciones = []
    tipifs_hasta_val_pos = []
    cantidad_valpos = []
    c_valpos_m_volver_a_llamar =[]

    prim_tipif_dif_sin_contacto = []
    prim_tipif_dif_sin_contacto2 = []
    prim_tipif_dif_sin_contacto_fecha =[]
    flg_traslados = [] 
    flg_charla_colegios =[]
    ingresos_web =[] 
    for name, group in groups:
    
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

        for index, row in group.iterrows():
            # if index == 0:
            #     primera_fecha.extend([row['sc_fecha']] * len(group))
            desc_1 = row['desc_resultado_1']
            desc_2 = row['desc_resultado_2']
            nombre_asesor = row['nombres']
            cantidad_tipificaciones_v += 1
            if row['desc_canal'] == 'Traslados':
                flg_traslado = 1
            if row['desc_canal'] == 'Charla Colegio':
                flg_charla_colegio = 1
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
                    pcf = row['sc_fecha']
                toques_sin_contacto = 0
                ult_desc_1_dif_sc = desc_1
                ult_desc_2_dif_sc = desc_2
                nombre = row['nombres']
                apellido = row['apellidos']
                ult_fecha_dif_sc = row['sc_fecha']
    
            if desc_1 in perdido:
                if black_list_actual == 0:
                    perdido_actual = 1
                recuperado_actual = 0
            elif desc_1 in black_list:
                black_list_actual = 1
                recuperado_actual = 0
                if perdido_actual:
                    perdido_actual = 0
            elif desc_1 in val_pos and row['nombres'] != "TI":
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
            elif desc_1 in val_pos and row['nombres'] == "TI":
                ingreso_web +=1
    
        clmn_toques_sc.extend([toques_sin_contacto] * len(group))
        perdidos_actuales.extend([perdido_actual] * len(group))
        black_list_actuales.extend([black_list_actual] * len(group))
        clm_ur1_nsc.extend([ult_desc_1_dif_sc] * len(group))
        clm_ur2_nsc.extend([ult_desc_2_dif_sc] * len(group))
        nombres_ult_dif_sin_contacto.extend([nombre] * len(group))
        apellidos_ult_dif_sin_contacto.extend([apellido] * len(group))
        clm_uf_nsc.extend([ult_fecha_dif_sc] * len(group))
        clm_contactos_lead.extend([contactos] * len(group))
        recuperados_actuales.extend([recuperado_actual] * len(group))
        contactados_actuales.extend([contactado] * len(group))
        nc_hasta_contacto.extend([nc_hasta_contacto_v] * len(group))
        tipifs_hasta_val_pos.extend([tipifs_hasta_val_pos_v] * len(group))
        cantidad_tipificaciones.extend([cantidad_tipificaciones_v]*len(group))
        cantidad_valpos.extend([cantidad_valpos_v]*len(group))
        c_valpos_m_volver_a_llamar.extend([c_valpos_m]*len(group))
        prim_tipif_dif_sin_contacto.extend([pc1]*len(group))
        prim_tipif_dif_sin_contacto2.extend([pc2]*len(group))
        prim_tipif_dif_sin_contacto_fecha.extend([pcf]*len(group))
        flg_traslados.extend([flg_traslado] * len(group))
        flg_charla_colegios.extend([flg_charla_colegio]*len(group))
        ingresos_web.extend([ingreso_web]*len(group))

    df['sin_contacto_consecutivos'] = clmn_toques_sc
    df['perdido_actual'] = perdidos_actuales
    df['black_list_actual'] = black_list_actuales
    df['ult_tipf_dif_sin_contacto'] = clm_ur1_nsc
    df['ult_tipf_dif_sin_contacto_2'] = clm_ur2_nsc
    df['nombres_ult_dif_sin_contacto'] = nombres_ult_dif_sin_contacto
    df['apellidos_ult_dif_sin_contacto'] = apellidos_ult_dif_sin_contacto
    df['ult_fecha_dif_sin_contacto'] = clm_uf_nsc
    df['cantidad_contactos'] = clm_contactos_lead
    df['recuperado_actual'] = recuperados_actuales
    df['contactado'] = contactados_actuales
    # df['fecha_primera_accion'] = primera_fecha
    df['nc_hasta_contacto'] = nc_hasta_contacto
    df['cantidad_tipificaciones'] = cantidad_tipificaciones
    df['tipifs_hasta_val_pos'] = tipifs_hasta_val_pos
    df['cant_val_pos'] = cantidad_valpos
    df['cant_val_pos-vall'] = c_valpos_m_volver_a_llamar
    df['prim_tipif_dif_sin_contacto'] = prim_tipif_dif_sin_contacto
    df['prim_tipif_dif_sin_contacto2'] = prim_tipif_dif_sin_contacto2
    df['prim_tipif_dif_sin_contacto_fecha'] = prim_tipif_dif_sin_contacto_fecha
    df['flg_traslados'] = flg_traslados
    df['flg_charla_colegios'] = flg_charla_colegios
    df['ingresos_web'] = ingresos_web

    df_resultado = df.loc[df.groupby('id_participacion')['fecha_hora_accion'].idxmax()]
    
    df_resultado['telefono_valido'] = df_resultado['telefono'].apply(is_valid_telephone_number)
    
    df_resultado['fecha_registro_periodo'] = pd.to_datetime(df['fecha_registro_periodo'])
    df_resultado['fecha_registro_periodo'] = df_resultado['fecha_registro_periodo'].dt.date
    
    # Obtener el lunes de la semana de la fecha de ingreso
    df_resultado['lunes_semana'] = df_resultado['fecha_registro_periodo'] - pd.to_timedelta(df['fecha_registro_periodo'].dt.dayofweek, unit='D')
    df_resultado['val_pos'] = 0
    df_resultado.loc[(df_resultado['ult_tipf_dif_sin_contacto'].isin(val_pos)) & (df_resultado['nombres_ult_dif_sin_contacto'] != "TI"), 'val_pos'] = 1
    
    lima_timezone = pytz.timezone('America/Lima')
    
    # Get the current time in Lima timezone
    lima_time = datetime.datetime.now(lima_timezone)
    
    # Get today's date in Lima timezone
    today_date_lima = lima_time.date()
    today_string = today_date_lima.strftime('%y%m%d')
    
    
    borrar_carpeta(ubicacion)
    
    df_resultado.to_excel(f"{ubicacion}{today_string}_bbdd_ucal_{campana}_conv_{convocatoria}_pagantes_{pagante_crm}.xlsx", index=False)
    
crear_base_del_dia("base_resultante/")
