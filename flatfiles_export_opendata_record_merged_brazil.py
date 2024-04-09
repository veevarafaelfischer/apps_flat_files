import datetime
import os

import pandas as pd
from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED
from _public.shared.sql import SQL

sql: SQL = SQL()
path = fr"D:\ETL\pentaho\files\network\reports\apps_flat_files"
file_csv = "OpenData - Record Merged Brazil_1.csv"
path_csv = fr"{path}\csv\{file_csv}"
path_zip = fr"{path}\zip\OpenData - Record Merged Brazil_[VID_MIN]_[VID_MAX]_[DATAHORA].zip"

def write_to_csv(path_csv):
    print('Iniciando...')
    sql_text = f""" 
        SET NOCOUNT ON
        DROP TABLE IF EXISTS [apps_opendata_flatfiles].[dbo].[Result_OpenData - Record Merged Brazil] 
       SELECT
            DISTINCT primary_country__v as primary_country__v,
            'HCP' as entity_type__v,
            vid__v as vid__v,
            record_merged_vid__v as record_merged_vid__v,
            modified_date__v as modified_date__v
        INTO [apps_opendata_flatfiles].[dbo].[Result_OpenData - Record Merged Brazil]
        FROM
            vod.dbo.hcp
        WHERE
            record_state__v = 'MERGED_INTO' 
            and primary_country__v = 'BR'
            
        UNION ALL 
        
        SELECT
            DISTINCT primary_country__v as primary_country__v,
            'HCO' as entity_type__v,
            vid__v as vid__v,
            record_merged_vid__v as record_merged_vid__v,
            modified_date__v as modified_date__v
        FROM
            vod.dbo.hco
        WHERE
            record_state__v = 'MERGED_INTO' 
            and primary_country__v = 'BR'
            
            
        SET NOCOUNT OFF
        SELECT * 
        FROM [apps_opendata_flatfiles].[dbo].[Result_OpenData - Record Merged Brazil] 
        ORDER BY
            1,
            2,
            5
    """
    df_date = pd.read_sql(sql_text, sql.getConn())

    print('Iniciando criacao do arquivo csv: ', path_csv)
    df_date.to_csv(path_csv, index=False)
    return 'CSV criado com sucesso'

def get_min_max_vid():
    list_result = sql.itemsList('''
        SELECT MIN(vid__v) AS vid_min, MAX(vid__v) AS vid_max  
        FROM [apps_opendata_flatfiles].[dbo].[Result_OpenData - Record Merged Brazil] 
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