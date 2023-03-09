-- Restoring new copy of Production Database in Viewer Instance
-- Kill Connections to Viewer Database by setting to Single User
:CONNECT TOP-ARCGIS-SQL\GISVIEW
USE [master]
IF DB_ID('Viewer') IS NOT NULL
    ALTER DATABASE [Viewer]
    SET SINGLE_USER
    WITH ROLLBACK IMMEDIATE
;
-- Restoring Viewer from Production Backup
RESTORE DATABASE [Viewer]
FROM DISK = N'E:\GIS-VIEW\MSSQL16.GISVIEW\MSSQL\Backup\Viewer.bak'
WITH REPLACE
    , FILE = 1
	, MOVE 'Production' TO 'E:\GIS-VIEW\MSSQL16.GISVIEW\MSSQL\DATA\Viewer.mdf'
	, MOVE 'Production_log' TO 'E:\GIS-VIEW\MSSQL16.GISVIEW\MSSQL\DATA\Viewer_log.ldf'
	, RECOVERY
;
-- Set Viewer Database to Single User
ALTER DATABASE [Viewer]
SET SINGLE_USER
WITH ROLLBACK IMMEDIATE
;
-- Re-Setting Logins
USE [Viewer]
ALTER USER sde WITH LOGIN = sde
ALTER USER publisher_view WITH LOGIN = publisher_view
;
-- Setting Viewer Database to Multi User
USE [master]
ALTER DATABASE [Viewer]
SET MULTI_USER
;
