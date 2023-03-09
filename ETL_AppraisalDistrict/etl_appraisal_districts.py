import arcpy
import data_cleaner
import data_getter
from timeit import default_timer as timer

SDE_connections = r"B:\EGDB_Connections\ForAdministrationOnly"
SDE_staging = fr"{SDE_connections}\Admin_Staging.sde"
SDE_production = fr"{SDE_connections}\Admin_Production.sde"
FD_appraisal_districts = fr"{SDE_staging}\Staging.SDE.ETL_AppraisalDistricts"


ccad_zip_url = r"https://www.collincad.org/downloads/finish/16-gis-downloads/385-geodatabase-export-with-appraisal-data"
ccad_fgdb = "CCAD_Data_Public"
ccad_fc_parcels_in = "parcels_with_appraisal_data_R5"
dcad_zip_url = r"https://www.dentoncad.com/data/_uploaded/GISPub/nightly_geodatabase.gdb.zip"
dcad_fgdb = "nightly_geodatabase"
dcad_fc_parcels_in = "Parcels"


print("starting operations")
arcpy.AcceptConnections(SDE_staging, False)
arcpy.DisconnectUser(SDE_staging, "ALL")
arcpy.env.workspace = FD_appraisal_districts
arcpy.env.overwriteOutput = True

start = timer()
data_getter.download_unzip_push(ccad_zip_url, ccad_fgdb, ccad_fc_parcels_in, "Archive_CCAD")
data_getter.download_unzip_push(dcad_zip_url, dcad_fgdb, dcad_fc_parcels_in, "Archive_DCAD")

data_cleaner.fc_reduced()
data_cleaner.ccad_add_fields()
data_cleaner.dcad_add_fields()
ccad_sedf = data_cleaner.get_sedf("CCAD")
ccad_sedf = data_cleaner.ccad_cleaning(ccad_sedf)
dcad_sedf = data_cleaner.get_sedf("DCAD")
dcad_sedf = data_cleaner.dcad_cleaning(dcad_sedf)
data_cleaner.combine_sedf_and_push(ccad_sedf, dcad_sedf)

end = timer()
print(f"\tOperations complete in {((end - start)/60):.2f} minutes")
arcpy.AcceptConnections(SDE_staging, True)