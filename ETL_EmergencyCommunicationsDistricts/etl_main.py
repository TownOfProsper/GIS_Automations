import arcpy
import data_appender
import data_loader
import data_cleaner
import data_push
from timeit import default_timer as timer

# Run weekly for new Collin Updates

SDE_connections = r"B:\EGDB_Connections\ForAdministrationOnly"
SDE_staging = fr"{SDE_connections}\Admin_Staging.sde"
SDE_production = fr"{SDE_connections}\Admin_Production.sde"
FD_emergencycommdistricts = fr"{SDE_staging}\Staging.SDE.ETL_EmergencyCommsDistricts"

arcpy.AcceptConnections(SDE_staging, False)
arcpy.DisconnectUser(SDE_staging, "ALL")
arcpy.env.workspace = FD_emergencycommdistricts
arcpy.env.overwriteOutput = True

start = timer()
# Load Various Data Sources to Staging SDE
data_loader.from_on_premise()
data_loader.from_nct911()
data_loader.create_expanded_fc_set()

# Appending Operations
data_appender.collin_addresses_to_expanded()
data_appender.collin_roads_to_expanded()
data_appender.denton_addresses_to_expanded()
data_appender.denton_roads_to_expanded()

# Data Cleaning Operations
data_cleaner.prosper_full_address()

# Data Push to Production
data_push.staging_to_production(SDE_production)
data_push.staging_get_metadata(SDE_production)
data_push.database_maintenance(SDE_production)
end = timer()
print(f"\tOperations complete in {((end - start)/60):.2f} minutes")
arcpy.AcceptConnections(SDE_staging, True)
