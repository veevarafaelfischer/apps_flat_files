import datetime
import os

import pandas as pd
from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED
from _public.shared.sql import SQL

sql: SQL = SQL()
path = fr"D:\ETL\pentaho\process\network\reports\apps_flat_files\help\tests\export"
file_csv = "OpenData Basic Data - HCP - PH - 20240712.csv"
path_csv = fr"{path}\{file_csv}"

def write_to_csv(path_csv):
    print('Iniciando...')
    sql_text = f""" 
        SET NOCOUNT ON
        drop table if exists #specialties 
        SELECT * 
        into #specialties
        FROM vod.dbo.reference 
        WHERE [Reference Type] = 'Specialty'
        
        SET NOCOUNT OFF
        SELECT DISTINCT 
            A.vid__v,
            A.hcp_type__v,
            A.hcp_status__v,
            A.formatted_name__v,
            A.national_id__v,
            A.national_inactive_id__v,
            B.PT_BR AS specialty_1__v,
            C.PT_BR AS specialty_2__v,
            D.PT_BR AS specialty_3__v,
            E.PT_BR AS specialty_4__v,
            F.PT_BR AS specialty_5__v,
            G.PT_BR AS specialty_6__v,
            H.PT_BR AS specialty_7__v,
            I.PT_BR AS specialty_8__v,
            J.PT_BR AS specialty_9__v,
            K.PT_BR AS specialty_10__v,
            A.created_date__v,
            A.modified_date__v
        FROM
            vod.dbo.hcp A WITH(NOLOCK) 
        LEFT JOIN #specialties B ON B.[Network Code] = A.specialty_1__v 
        LEFT JOIN #specialties C ON C.[Network Code] = A.specialty_2__v 
        LEFT JOIN #specialties D ON D.[Network Code] = A.specialty_3__v 
        LEFT JOIN #specialties E ON E.[Network Code] = A.specialty_4__v 
        LEFT JOIN #specialties F ON F.[Network Code] = A.specialty_5__v 
        LEFT JOIN #specialties G ON G.[Network Code] = A.specialty_6__v 
        LEFT JOIN #specialties H ON H.[Network Code] = A.specialty_7__v 
        LEFT JOIN #specialties I ON I.[Network Code] = A.specialty_8__v 
        LEFT JOIN #specialties J ON J.[Network Code] = A.specialty_9__v 
        LEFT JOIN #specialties K ON K.[Network Code] = A.specialty_10__v 
        WHERE
            A.primary_country__v = 'BR'
            AND A.hcp_type__v = 'PH'
            AND A.data_privacy_opt_out__v IS NULL 
    """
    df_date = pd.read_sql(sql_text, sql.getConn())

    print('Iniciando criacao do arquivo csv: ', path_csv)
    df_date.to_csv(path_csv, index=False)
    return 'CSV criado com sucesso'

write_to_csv(path_csv)
print('Arquivo gerado com sucesso: ', path_csv)

exit(0)