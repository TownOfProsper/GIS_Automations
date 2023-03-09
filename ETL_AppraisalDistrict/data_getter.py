import arcpy
import tempfile
import zipfile
from io import BytesIO
from urllib.request import urlopen

SDE_connections = r"B:\EGDB_Connections\ForAdministrationOnly"
SDE_staging = fr"{SDE_connections}\Admin_Staging.sde"
FD_appraisal_districts = fr"{SDE_staging}\Staging.SDE.ETL_AppraisalDistricts"
FGDB_connections = r"B:\EGDB_Connections\Data_AppraisalDistricts"


def download_unzip_push(zip_url, gis_fgdb_name, fc_parcel_in, fc_parcel_out:str):
    """Download a zip file from a URL and immediately extract to folder location"""
    arcpy.env.workspace = FD_appraisal_districts
    arcpy.env.overwriteOutput = True
    with tempfile.TemporaryDirectory() as download_folder:
        with urlopen(zip_url) as zip_in_transit:
            with zipfile.ZipFile(BytesIO(zip_in_transit.read())) as zip_ref:
                zip_ref.extractall(download_folder)
            extracted_fgdb = fr"{download_folder}\{gis_fgdb_name}.gdb"
            arcpy.Copy_management(extracted_fgdb, f"{FGDB_connections}\{gis_fgdb_name}.gdb")
            arcpy.conversion.FeatureClassToFeatureClass(
                in_features=fr"{extracted_fgdb}\{fc_parcel_in}",
                out_path=arcpy.env.workspace,
                out_name=fc_parcel_out,
            )
    print(f"\tdownloaded {fc_parcel_out.split('_')[-1]} parcels")
