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
        s.utm_campana
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
        AND c.sc_campana in ('2024-2', '2025-1')
        AND gs.flg_convocatoria in (0,1)
        AND gs.pagante_crm in (0,1)
        
    ORDER BY f.sc_fecha ASC;
