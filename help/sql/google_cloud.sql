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



SELECT DISTINCT
    rel_config_client_data_access.id AS [rel_config_client_data_access_id],
    config_client_data_type.extension
FROM apps_opendata_flatfiles.dbo.rel_config_client_data_access
INNER JOIN apps_opendata_flatfiles.dbo.config_client_data_type ON config_client_data_type.id = rel_config_client_data_access.config_client_data_type_id
    AND config_client_data_type.active = 1
WHERE rel_config_client_data_access.config_client_id = 2
    AND rel_config_client_data_access.active = 1