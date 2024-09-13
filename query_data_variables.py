def get_query(campana, convocatoria, pagante_crm, fecha):
    query = f"""
select 

    sem.sc_prospecto as id_prometeo,
    sem.nombres,
    sem.apellido_paterno,
    sem.apellido_materno,
    sem.nombres +' ' +sem.apellido_paterno + ' ' + sem.apellido_materno as Nombres_Lead,
    fp.telefono,
    fp.celular,
    fp.celular2,
    sem.email,
    sem.numero_documento,
    sem.pagante_crm,
    sem.fecha_pagante_crm,
    gc.fecha_registro_periodo,
    gc.programa_ult as ult_programa_interes,
    gc.tipo_registro,
    sem.canal_atribucion,
    sem.subcanal,
    gc.flg_convocatoria,
    sem.campana as sc_campana,
    dc.desc_canal,
    f.sc_fecha,
    s.hora_seguimiento,
    l1.desc_resultado as desc_resultado_1,
    l2.desc_resultado as desc_resultado_2,
    ase.nombres as nombre_asesor,
    ase.apellidos as apellidos_asesor,
    ase.email as email_asesor,
    sem.utm_campana,
    sem.urlsource,
    col.codigo_modular,
    sem.anio_fin_colegio,
    sem.boleta_real,
    sem.boleta_nominal,
    sem.escala_inicial,
    sem.escala_final

from

    comercial.dts_kpi_comer_fact_semantica AS sem

    JOIN comercial.dts_kpi_comer_fact_seguimiento AS s
    ON sem.id_participacion = s.id_participacion
    
    LEFT JOIN financiero.dts_kpi_dim_fecha AS f
    ON s.id_fecha_seguimiento = f.id_fecha

    LEFT JOIN financiero.dts_kpi_dim_resultado AS l1
    ON s.id_resultado1 = l1.id_resultado

    LEFT JOIN financiero.dts_kpi_dim_resultado AS l2
    ON s.id_resultado2 = l2.id_resultado

    LEFT JOIN financiero.dts_kpi_dim_asesor AS ase
    ON ase.id_asesor = s.id_asesor        

    LEFT JOIN financiero.dts_kpi_dim_colegio AS col
    ON col.id_colegio = sem.id_colegio

    LEFT JOIN comercial.dts_kpi_comer_fact_prospeccion AS fp
    ON sem.id_prospecto = fp.id_prospecto
    
    LEFT JOIN financiero.dts_kpi_dim_canal AS dc
    ON s.id_canal = dc.id_canal
    JOIN comercial.dts_aws_kpi_comer_fact_gestion_campana AS gc
    ON sem.id_participacion = gc.id_participacion
WHERE
    s.audit_state = 1
    AND sem.unidad_negocio = 'UCAL'
    AND sem.campana in {campana}
    AND gc.flg_convocatoria in {convocatoria}
    AND sem.pagante_crm in {pagante_crm}
    AND f.sc_fecha < '{fecha}'
order by id_prometeo ASC, sc_fecha ASC, hora_seguimiento ASC
    """
    return query
