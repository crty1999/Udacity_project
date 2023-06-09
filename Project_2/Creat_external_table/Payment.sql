IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'SynapseDelimitedTextFormat') 
	CREATE EXTERNAL FILE FORMAT [SynapseDelimitedTextFormat] 
	WITH ( FORMAT_TYPE = DELIMITEDTEXT ,
	       FORMAT_OPTIONS (
			 FIELD_TERMINATOR = ',',
			 USE_TYPE_DEFAULT = FALSE
			))
GO

IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'cristi_cristi_dfs_core_windows_net') 
	CREATE EXTERNAL DATA SOURCE [cristi_dfs_core_windows_net] 
	WITH (
		LOCATION = 'abfss://cristi@cristi.dfs.core.windows.net', 
		TYPE = HADOOP 
	)
GO

CREATE EXTERNAL TABLE dbo.staging_payment (
	[payment_id] bigint,
	[date] nvarchar(4000),
	[amount] float,
	[account_number] bigint
	)
	WITH (
	LOCATION = 'publicpaymentimport.csv',
	DATA_SOURCE = [cristi_cristi_dfs_core_windows_net],
	FILE_FORMAT = [SynapseDelimitedTextFormat]
	)
GO


SELECT TOP 100 * FROM dbo.staging_payment
GO