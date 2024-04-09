from os import path, stat
from ftplib import FTP_TLS, FTP
from _public.shared.sql import SQL
from _public.shared.utils import *

isDebug = False
isList = True

# Conexão com o SQL Server
sql: SQL = SQL()
blOk, lst_dados_acesso, total_itens = sql.itemsList('''
	SELECT DISTINCT 
		[config_client].[id] AS [config_client_id],
		[config_client].[name] AS [config_client_name],
		[config_client_data_access_ftp].[host] AS [ftp_host], 
		[config_client_data_access_ftp].[username] AS [ftp_username], 
		[config_client_data_access_ftp].[anonymous] AS [ftp_user_anonymous], 
		[config_client_data_access_ftp].[port] AS [ftp_port],
		CONVERT(VARCHAR, DECRYPTBYPASSPHRASE(config_client_security.phrase_security, [config_client_data_access_ftp].password)) AS [ftp_password], 
		[config_client_data_access_ftp].[config_client_data_access_ftp_type_id] AS [ftp_type_id], 
		[config_client_data_access_ftp].[remote_directory],
		[config_client_data_access_ftp].[local_directory] 
	FROM apps_opendata_flatfiles.[dbo].[config_client_data_access_ftp] 
	INNER JOIN apps_opendata_flatfiles.dbo.config_client ON config_client.id = [config_client_data_access_ftp].config_client_id
		AND config_client.active = 1 
	INNER JOIN apps_opendata_flatfiles.dbo.config_client_security ON config_client_security.config_client_id = [config_client].id 
	INNER JOIN apps_opendata_flatfiles.dbo.config_client_data_access_ftp_type ON config_client_data_access_ftp_type.id = [config_client_data_access_ftp].config_client_data_access_ftp_type_id 
		AND config_client_data_access_ftp_type.active = 1 
	WHERE [config_client_data_access_ftp].active = 1 
''')

if blOk is False:
	exit(-1)

for dado_acesso in lst_dados_acesso:
	config_client_id = dado_acesso['config_client_id']
	config_client_name = dado_acesso['config_client_name']
	ftp_type_id = dado_acesso['ftp_type_id']
	ftp_host = dado_acesso['ftp_host']
	ftp_port = dado_acesso['ftp_port']
	ftp_user = dado_acesso['ftp_username']
	ftp_pwd = dado_acesso['ftp_password']
	ftp_remote_directory = dado_acesso['remote_directory']
	local_directory = dado_acesso['local_directory'] #r"D:\ETL\pentaho\files\network\reports\apps_flat_files"

	try:
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
			arr_files = get_files_paths(local_directory, 'csv' in extension, 'zip' in extension)

			# Setado com dados de conexão com o servidor FTP
			if ftp_type_id == 1:	#SFTP
				ftp = FTP_TLS(host=ftp_host)
				ftp.prot_p()	# Método relacionado a segurança TSL
			else:	# elif FTP_TYPE_ID == 4:
				ftp = FTP()
				ftp.connect(host=ftp_host, port=ftp_port)
			print('FTP - Host encontrado: ', ftp_host)

			ftp.login(user=ftp_user, passwd=ftp_pwd)
			print('FTP - Login efetuado com sucesso.')


			arr_files_upload = []
			ftp.cwd(ftp_remote_directory)	# Mudança de diretório ftp para o diretorio principal
			lst_arquivos_ftp = list(ftp.nlst())
			arr_files_not_matched = [match for match in lst_arquivos_ftp if extension in match]

			if isDebug is False:
				arr_files_upload = [match for match in arr_files if extension in match]
				# arr_files_upload = arr_files.copy()

				for file_actual in arr_files_upload:
					file_name = path.basename(file_actual)
					if file_name in arr_files_not_matched:
						arr_files_upload.remove(file_actual)

				for file_upload in arr_files_upload:
					file_name = path.basename(file_upload)
					file_size = stat(file_upload).st_size

					file = open(file_upload, 'rb')  # arquivo para enviar
					result_upload_file_ftp = ftp.storbinary(fr'STOR {file_name}', file)  # envia o arquivo
					file.close()  # fecha  close file and FTP

					sql_text = rf'''
						INSERT INTO [apps_opendata_flatfiles].[dbo].[log_file_sent_client] ([rel_config_client_data_access_id], [filename_sent], [file_size_sent], [remote_directory_sent], [status_result_ftp]) 
						VALUES ({rel_config_client_data_access_id}, '{file_name}', '{file_size}', '{ftp_remote_directory}', '{result_upload_file_ftp}')
					'''
					blnOk_Upload, total_execucao, error = sql.itemExecute(sql_text)
					print(file_upload, '(', file_size, ') => ', result_upload_file_ftp, ' => ', blnOk_Upload)


			if isList is True:
				files = ''
				lst_arquivos_ftp = list(ftp.nlst())
				arr_files_not_matched = [match for match in lst_arquivos_ftp if extension in match]
				for file_actual in arr_files_not_matched:
					file_name = path.basename(file_actual)
					files = files + '\n- File: '+ file_name
				print('Arquivos do cliente:' + files)

			# Fecha a conexao FTP
			print('FTP - Fechando conexao')
			ftp.quit()
			print('FTP - Conexao fechada com sucesso')

		send_notification(canal=slackMessage.GOOGLE_CHAT_URL_NOTIFICATION_SUCCESS, mensagem=fr'App FlatFiles - FTP - Processo finalizado com sucesso para o cliente: {config_client_name}')
	except Exception as ex:
		send_notification(canal=slackMessage.GOOGLE_CHAT_URL_NOTIFICATION_ERROR, mensagem=fr'App FlatFiles - FTP - Erro ocorrido no processo do cliente {config_client_name} - Error: {str(ex)}')
		print(ex)


exit(0)
