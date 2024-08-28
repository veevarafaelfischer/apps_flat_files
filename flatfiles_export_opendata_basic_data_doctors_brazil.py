import datetime
import os

import pandas as pd
from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED
from _public.shared.sql import SQL

sql: SQL = SQL()
path = fr"D:\ETL\pentaho\files\network\reports\apps_flat_files"
file_csv = "OpenData Basic Data - Doctors Brazil_1.csv"
path_csv = fr"{path}\csv\{file_csv}"
path_zip = fr"{path}\zip\OpenData Basic Data - Doctors Brazil_[VID_MIN]_[VID_MAX]_[DATAHORA].zip"

def write_to_csv(path_csv):
    print('Iniciando...')
    sql_text = f""" 
        SET NOCOUNT ON
        DROP TABLE IF EXISTS [apps_opendata_flatfiles].[dbo].[Result_OpenData Basic Data - Doctors Brazil] 
       SELECT DISTINCT 
            hcp.vid__v AS vid__v,
            hcp.hcp_type__v AS hcp_type__v,
            hcp.hcp_status__v AS hcp_status__v,
            hcp.formatted_name__v AS formatted_name__v,
            hcp.national_id__v AS national_id__v,
            hcp.national_inactive_id__v AS national_inactive_id__v,
            hcp.specialty_1__v AS specialty_1__v,
            hcp.specialty_2__v AS specialty_2__v,
            hcp.specialty_3__v AS specialty_3__v,
            hcp.specialty_4__v AS specialty_4__v,
            hcp.specialty_5__v AS specialty_5__v,
            hcp.specialty_6__v AS specialty_6__v,
            hcp.specialty_7__v AS specialty_7__v,
            hcp.specialty_8__v AS specialty_8__v,
            hcp.specialty_9__v AS specialty_9__v,
            hcp.specialty_10__v AS specialty_10__v,
            address.administrative_area__v,
            address.locality__v,
            address.postal_code__v,
            hcp.created_date__v AS created_date__v,
            hcp.modified_date__v AS modified_date__v
        INTO [apps_opendata_flatfiles].[dbo].[Result_OpenData Basic Data - Doctors Brazil] 
        FROM
            vod.dbo.hcp WITH(NOLOCK) 
        LEFT JOIN vod.dbo.address WITH(NOLOCK) 
                ON hcp.vid__v = address.entity_vid__v
            AND address.address_status__v = 'A'
        WHERE
            primary_country__v = 'BR'
            AND hcp_type__v = 'D'
            AND data_privacy_opt_out__v IS NULL
            
        SET NOCOUNT OFF
        SELECT * 
        FROM [apps_opendata_flatfiles].[dbo].[Result_OpenData Basic Data - Doctors Brazil] WITH(NOLOCK)  
    """
    df_date = pd.read_sql(sql_text, sql.getConn())

    print('Iniciando criacao do arquivo csv: ', path_csv)
    df_date.to_csv(path_csv, index=False)
    return 'CSV criado com sucesso'

def get_min_max_vid():
    list_result = sql.itemsList('''
        SELECT MIN(vid__v) AS vid_min, MAX(vid__v) AS vid_max  
        FROM [apps_opendata_flatfiles].[dbo].[Result_OpenData Basic Data - Doctors Brazil] WITH(NOLOCK)  
    ''')

    vid_min = 0
    vid_max = 0
    for item in list_result[1]:
        vid_min = item['vid_min']
        vid_max = item['vid_max']

    return str(vid_min), str(vid_max)

def write_to_zip(path_zip=None):
    vid_min, vid_max = get_min_max_vid()
    print('VID MIN: ', vid_min, ' -> MAX: ', vid_max)

    path_zip = path_zip.replace('[VID_MIN]', vid_min).replace('[VID_MAX]', vid_max).replace('[DATAHORA]', datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S'))
    print('Iniciando criacao do arquivo zip: ', path_zip)
    with ZipFile(path_zip, "w", ZIP_DEFLATED, compresslevel=9) as archive:
        for file in Path(fr'{path}\csv').rglob(fr"{file_csv}"):
            archive.write(file, arcname=file.name)
    print('ZIP criado com sucesso')
    return path_zip

def remove_temp():
    if os.path.isfile(path_csv):
        os.remove(path_csv)

write_to_csv(path_csv)
write_to_zip(path_zip)
# remove_temp()
exit(0)