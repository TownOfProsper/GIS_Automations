import arcpy
import data_loader
import data_cleaner
import data_appender
from timeit import default_timer as timer

# run monthly for new DenCo Updates
SDE_connections = r"B:\EGDB_Connections\ForAdministrationOnly"
SDE_staging = fr"{SDE_connections}\Admin_Staging.sde"
FD_emergency_comm_districts = fr"{SDE_staging}\Staging.SDE.ETL_EmergencyCommsDistricts"
Denton_Clean_Roads = fr"{FD_emergency_comm_districts}\Denton_Clean_Roads"
Denton_Clean_Addresses = fr"{FD_emergency_comm_districts}\Denton_Clean_Addresses"
Denton_Dirty_Roads = fr"{FD_emergency_comm_districts}\Denton_Dirty_Roads"
Denton_Dirty_Addresses = fr"{FD_emergency_comm_districts}\Denton_Dirty_Addresses"

arcpy.AcceptConnections(SDE_staging, False)
arcpy.DisconnectUser(SDE_staging, "ALL")
arcpy.env.workspace = FD_emergency_comm_districts
arcpy.env.overwriteOutput = True

start = timer()
# Load Denton Data to Staging SDE
data_loader.from_denco911()
# Data Cleaning Operations
data_cleaner.denton_add_nguid()
# Roads
sedf_dirty_roads = data_cleaner.sedf(Denton_Dirty_Roads)
sedf_dirty_roads = data_cleaner.white_space(sedf_dirty_roads)
sedf_dirty_roads = data_cleaner.roads_add_fields(sedf_dirty_roads)
sedf_dirty_roads = data_cleaner.roads_enforce_schema(sedf_dirty_roads)
sedf_dirty_roads = data_cleaner.roads_data_hygiene(sedf_dirty_roads)
sedf_dirty_roads = data_cleaner.roads_street_names(sedf_dirty_roads)
sedf_dirty_roads = data_cleaner.roads_nct_class(sedf_dirty_roads)
sedf_dirty_roads = data_cleaner.roads_road_class(sedf_dirty_roads)
sedf_dirty_roads = data_cleaner.roads_road_surface(sedf_dirty_roads)
sedf_dirty_roads = data_cleaner.roads_maintained_by(sedf_dirty_roads)
sedf_dirty_roads = data_cleaner.roads_zip_codes(sedf_dirty_roads)
sedf_dirty_roads = data_cleaner.roads_drop_fields(sedf_dirty_roads)
sedf_dirty_roads = data_cleaner.roads_rename_fields(sedf_dirty_roads)
# Addresses
domain_directionals = data_cleaner.denton_get_domain("Directionals")
sedf_dirty_addresses = data_cleaner.sedf(Denton_Dirty_Addresses)
sedf_dirty_addresses = data_cleaner.white_space(sedf_dirty_addresses)
sedf_dirty_addresses = data_cleaner.addresses_add_fields(sedf_dirty_addresses)
sedf_dirty_addresses = data_cleaner.addresses_enforce_schema(sedf_dirty_addresses)
sedf_dirty_addresses = data_cleaner.addresses_data_hygiene(sedf_dirty_addresses)
sedf_dirty_addresses = data_cleaner.addresses_nct_type(sedf_dirty_addresses)
sedf_dirty_addresses = data_cleaner.addresses_place_type(sedf_dirty_addresses)
sedf_dirty_addresses = data_cleaner.addresses_street_names(
    sedf_dirty_addresses, domain_directionals
)
sedf_dirty_addresses = data_cleaner.refill_na(sedf_dirty_addresses)
sedf_dirty_addresses = data_cleaner.addresses_drop_fields(sedf_dirty_addresses)
sedf_dirty_addresses = data_cleaner.addresses_rename_fields(sedf_dirty_addresses)
# Load Clean Data
sedf_dirty_roads.spatial.to_featureclass(location=Denton_Clean_Roads, sanitize_columns=False)
sedf_dirty_addresses.spatial.to_featureclass(
    location=Denton_Clean_Addresses, sanitize_columns=False
)
# Appending Operations
data_appender.denton_roads_corrected()
data_appender.denton_addresses_corrected()
data_appender.denton_addresses_labeling()
data_appender.denton_addresses_multifamily()
end = timer()
print(f"\tOperations complete in {((end - start)/60):.2f} minutes")
arcpy.AcceptConnections(SDE_staging, True)
