import yaml
import funciones_bases_campanas as fbc
import funciones_base_hsm as fbh
import funciones_generales as fg
from dateutil.relativedelta import relativedelta

def main():
    fg.borrar_carpeta('bases_resultantes\\')
    
    with open("config_files/campanas_activas.yaml", 'r') as file:
        query_params = yaml.safe_load(file)
    
    campanas = ','.join([f"'{param}'" for param in query_params['campanas']])
    campana_list = f"({campanas})"
    pagante_crm = f"({','.join(map(str, query_params['pagante_crm']))})"
    convocatoria = f"({','.join(map(str, query_params['convocatoria']))})"
    hoy = fg.fecha_peru_hoy() 
    today_string = fg.fecha_peru_hoy().strftime('%y%m%d')

    df = fbc.obtener_df_de("base_del_dia", campana_list, pagante_crm, convocatoria, hoy)

    # obtener base up to date
    for base in query_params["bases_resultantes"]:
        df_XX = fbc.crear_base_leads_unicos(df, base, "bases_resultantes", hoy)
        #fbh.crear_bases_hsm(df_XX,)
    
    if query_params["comparable"]:
        # obtener base MTD - 1
        fecha_mes_pasado = hoy - relativedelta(months=1)
        mes_pasado = fecha_mes_pasado.strftime('%Y-%m-%d')
        
        for base in query_params["bases_resultantes"]:
            df_XX = fbc.crear_base_leads_unicos(df, base, "bases_resultantes", mes_pasado)
    
        # obtener base YTD - 1
        
        fecha_ano_pasado = hoy - relativedelta(years=1)
        ano_pasado = fecha_ano_pasado.strftime('%Y-%m-%d')
        campana_pasada_list = fg.restar_ano_en_string(campana_list)
        bases_resultantes = fg.restar_ano_en_lista(query_params["bases_resultantes"])

        df = fbc.obtener_df_de("base_del_dia", campana_pasada_list, pagante_crm, convocatoria, ano_pasado)
        for base in bases_resultantes:
            df_XX = fbc.crear_base_leads_unicos(df, base, "bases_resultantes", ano_pasado)
            
    folder_id="1rUCuzefmDHHvUCYRCLLkcOxuDiqSgiC0"
    
    #path=(f"bases_resultantes/{hoy}_bbdd_ucal_['2024-2']_conv_(0,1)_pagantes_(0,1)_fecha_{today_string}.xlsx")
    path2=(f"bases_resultantes/{hoy}_bbdd_ucal_['2025-1']_conv_(0,1)_pagantes_(0,1)_fecha_{today_string}.xlsx")

    #fg.upload_to_drive(path, folder_id)
    fg.upload_to_drive(path2, folder_id)
    
    """
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
        """

    
    return 

if __name__ == "__main__":
    main()
