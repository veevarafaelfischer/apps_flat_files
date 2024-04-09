from os import path, environ, stat
from google.cloud import storage
from _public.shared.sql import SQL
from _public.shared.utils import *

isDebug = True
isList = True
files_path = r"D:\ETL\pentaho\files\network\reports\apps_flat_files"
credentials_path = r"D:\ETL\pentaho\process\network\reports\apps_flat_files\private\google_cloud"


def list_buckets(storage_client):
	try:
		lst_buckets = storage_client.list_buckets()
		for bucket_item in lst_buckets:
			print(bucket_item.name)
	except Exception as ex:
		print(ex)


def list_files_bucket(bucket, blob_name='', extension='*'):
	try:
		arr_blobs_specific = bucket.list_blobs(prefix=rf'{blob_name}')
		arr_result = []
		if len(blob_name) > 0:
			blob_name = r'/'

		for blob in arr_blobs_specific:
			blob_filename = blob.name.replace(blob_name + '/', '')
			arr_result.append(blob_filename)

			print(blob_name + blob_filename)

		if extension != '*':
			arr_result = [match for match in arr_result if extension in match]

		return arr_result
	except Exception as ex:
		print(ex)


def upload_to_bucket(storage_client, content_name, diretorio, file_path, bucket_name):
	try:
		bucket = storage_client.get_bucket(bucket_name)
		diretorio_blob_upload = diretorio
		if len(diretorio_blob_upload) > 0:
			diretorio_blob_upload = rf'{diretorio_blob_upload}/'
		blob = bucket.blob(rf'{diretorio_blob_upload}{content_name}')
		blob.upload_from_filename(filename=file_path, timeout=600)  # timeout em segundos (10 minutos)
		return True

	except Exception as e:
		print(e)
		return False


def send_files(storage_client, bucket, bucket_name, blob_name, extension, is_contain_csv, is_contain_zip):
	total_upload = 0

	arr_files = get_files_paths(files_path, is_contain_csv, is_contain_zip)
	lst_arquivos_google_cloud = list_files_bucket(bucket, rf'{blob_name}', extension)
	arr_files_verify = [match for match in arr_files if extension in match]
	arr_files_upload = []

	for file_actual in arr_files_verify:
		file_name = path.basename(file_actual)
		if not file_name in lst_arquivos_google_cloud:
			arr_files_upload.append(file_actual)

	for file in arr_files_upload:
		content_name = path.basename(file)
		bln_ok = upload_to_bucket(storage_client, content_name, blob_name, file, bucket_name)
		# bln_ok = True
		file_size = stat(file).st_size

		sql_text = rf'''
			INSERT INTO [apps_opendata_flatfiles].[dbo].[log_file_sent_client] ([rel_config_client_data_access_id], [filename_sent], [file_size_sent], [remote_directory_sent], [status_result_ftp]) 
			VALUES ({rel_config_client_data_access_id}, '{content_name}', '{file_size}', '{blob_name}', '{bln_ok}')
		'''
		bln_ok_upload, total_execucao, error = sql.itemExecute(sql_text)
		print(content_name, '(', file_size, ') => ', bln_ok, ' => ', bln_ok_upload)

		total_upload = total_upload + iif(bln_ok is True, 1, 0)

	return total_upload


# Conexão com o SQL Server
sql: SQL = SQL()
blOk, lst_dados_acesso, total_itens = sql.itemsList('''
	SELECT DISTINCT
		[config_client].[id] AS [config_client_id],
		[config_client].[name] AS [config_client_name],
		[config_client_data_access_google_cloud].[certificate_json] AS [google_cloud_certificate_json],
		[config_client_data_access_google_cloud].[bucket] AS [google_cloud_bucket],
		[config_client_data_access_google_cloud].[remote_directory] AS [google_cloud_directory],
		[config_client_data_access_google_cloud].[local_directory] AS [local_directory]
	FROM apps_opendata_flatfiles.[dbo].[config_client_data_access_google_cloud]
	INNER JOIN apps_opendata_flatfiles.dbo.config_client ON config_client.id = [config_client_data_access_google_cloud].config_client_id
		AND config_client.active = 1
	WHERE [config_client_data_access_google_cloud].active = 1
''')

if blOk is False:
	exit(-1)

for dado_acesso in lst_dados_acesso:
	config_client_id = dado_acesso['config_client_id']
	config_client_name = dado_acesso['config_client_name']
	google_cloud_certificate_json = dado_acesso['google_cloud_certificate_json']
	google_cloud_bucket = dado_acesso['google_cloud_bucket']
	google_cloud_directory = dado_acesso['google_cloud_directory']

	try:
		environ['GOOGLE_APPLICATION_CREDENTIALS'] = rf'{credentials_path}\{google_cloud_certificate_json}'
		bucket_name = google_cloud_bucket
		diretorio = google_cloud_directory

		storage_client = storage.Client()
		bucket = storage_client.get_bucket(bucket_name)

		if isDebug is True:
			list_buckets(storage_client)
			list_files_bucket(bucket, '')
			list_files_bucket(bucket, rf'{diretorio}')
		else:

			blOk_extension, lst_extension, total_itens_extension = sql.itemsList(rf'''
				SELECT DISTINCT
					rel_config_client_data_access.id AS [rel_config_client_data_access_id],  
					config_client_data_type.extension
				FROM apps_opendata_flatfiles.dbo.rel_config_client_data_access
				INNER JOIN apps_opendata_flatfiles.dbo.config_client_data_type ON config_client_data_type.id = rel_config_client_data_access.config_client_data_type_id
					AND config_client_data_type.active = 1
				WHERE rel_config_client_data_access.config_client_id = {config_client_id}
					AND rel_config_client_data_access.active = 1
			''')

			if blOk_extension is False:
				print('Extensão não configurada para o cliente: ', config_client_id, ' => ', config_client_name)
				continue

			for dado_extension in lst_extension:
				rel_config_client_data_access_id = dado_extension['rel_config_client_data_access_id']
				extension = dado_extension['extension']

				total_upload = send_files(storage_client, bucket, bucket_name, diretorio, extension, 'csv' in extension, 'zip' in extension)
				print('total_upload: ', total_upload)
				if isList is True:
					list_files_bucket(bucket, rf'{diretorio}')

		storage_client.close()
		storage_client = None

		if isDebug is False:
			send_notification(canal=slackMessage.GOOGLE_CHAT_URL_NOTIFICATION_SUCCESS, mensagem=fr'App FlatFiles - Google Cloud - Processo finalizado com sucesso para o cliente: {config_client_name}')
	except Exception as ex:
		send_notification(canal=slackMessage.GOOGLE_CHAT_URL_NOTIFICATION_ERROR, mensagem=fr'App FlatFiles - Google Cloud - Erro ocorrido no processo do cliente {config_client_name} - Error: {str(ex)}')
		print(ex)

exit(0)
