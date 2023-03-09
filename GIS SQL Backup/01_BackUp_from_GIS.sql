-- Backing Up Production Database from Production Instance
:CONNECT TOP-ARCGIS-SQL\GIS
USE [master]
BACKUP DATABASE [Production]
TO DISK ='E:\GIS-VIEW\MSSQL16.GISVIEW\MSSQL\Backup\Viewer.bak'
WITH INIT
	, FORMAT
;