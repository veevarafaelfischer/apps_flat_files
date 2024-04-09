SELECT DISTINCT
	[config_client_data_access_ftp].[host] AS [ftp_host],
	[config_client_data_access_ftp].[username] AS [ftp_username],
	[config_client_data_access_ftp].[anonymous] AS [ftp_user_anonymous],
	[config_client_data_access_ftp].[port] AS [ftp_port],
	CONVERT(VARCHAR, DECRYPTBYPASSPHRASE(config_client_security.phrase_security, [config_client_data_access_ftp].password)) AS [ftp_password],
	[config_client_data_access_ftp].[config_client_data_access_ftp_type_id] AS [ftp_type_id],
	[config_client_data_access_ftp].[remote_directory]
FROM apps_opendata_flatfiles.[dbo].[config_client_data_access_ftp]
INNER JOIN apps_opendata_flatfiles.dbo.config_client ON config_client.id = [config_client_data_access_ftp].config_client_id
	AND config_client.active = 1
INNER JOIN apps_opendata_flatfiles.dbo.config_client_security ON config_client_security.config_client_id = [config_client].id
INNER JOIN apps_opendata_flatfiles.dbo.config_client_data_access_ftp_type ON config_client_data_access_ftp_type.id = [config_client_data_access_ftp].config_client_data_access_ftp_type_id
	AND config_client_data_access_ftp_type.active = 1
WHERE [config_client_data_access_ftp].active = 1



SELECT DISTINCT
	config_client_data_type.*
FROM apps_opendata_flatfiles.dbo.rel_config_client_data_access
INNER JOIN apps_opendata_flatfiles.dbo.config_client_data_type ON config_client_data_type.id = rel_config_client_data_access.config_client_data_type_id
	AND config_client_data_type.active = 1
WHERE rel_config_client_data_access.config_client_id = 1
	AND rel_config_client_data_access.active = 1