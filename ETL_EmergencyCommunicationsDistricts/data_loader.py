import arcgis
import arcpy
import keyring
import tempfile
import zipfile

SDE_connections = r"B:\EGDB_Connections\ForAdministrationOnly"
SDE_staging = fr"{SDE_connections}\Admin_Staging.sde"
SDE_nct911 = fr"{SDE_connections}\Admin_NCT911.sde"
FD_emergency_comm_districts = fr"{SDE_staging}\Staging.SDE.ETL_EmergencyCommsDistricts"
FGDB_connections = r"B:\EGDB_Connections\Data_EmergencyCommunicationsDistricts"
FGDB_denco911 = fr"{FGDB_connections}\Denco911.gdb"


def from_on_premise():
    """Load Town Data from NCT-911 On-Premise Replica"""
    print("Begin Loading Operations:\n\tNCT-911 local data")
    arcpy.env.workspace = FD_emergency_comm_districts
    arcpy.env.overwriteOutput = True
    origin_sde_connection = SDE_nct911
    dict_onPremiseData = {
        "NCT911_Collin.SDE.SiteStructureAddressPoints": "Prosper_Addresses",
        "NCT911_Collin.SDE.RoadCenterlines": "Prosper_Roads",
    }
    for key, value in dict_onPremiseData.items():
        # Load Prosper Data to staging
        if key == "NCT911_Collin.SDE.RoadCenterlines":
            arcpy.conversion.FeatureClassToFeatureClass(
                in_features=fr"{origin_sde_connection}\{key}",
                out_path=arcpy.env.workspace,
                out_name=value,
                where_clause="NCT_Class <> 39"
            )
        else:
            arcpy.conversion.FeatureClassToFeatureClass(
                in_features=fr"{origin_sde_connection}\{key}",
                out_path=arcpy.env.workspace,
                out_name=value,
            )


def create_expanded_fc_set():
    """Load Town Data from NCT-911 On-Premise Replica"""
    arcpy.env.workspace = FD_emergency_comm_districts
    arcpy.env.overwriteOutput = True
    dict_prosper_data = {
        "Prosper_Addresses": "Expanded_Addresses",
        "Prosper_Roads": "Expanded_Roads",
    }
    print("\t\tCreating expanded Feature Class templates from Prosper Data")
    for key, value in dict_prosper_data.items():
        # Load Prosper Data to staging
        arcpy.conversion.FeatureClassToFeatureClass(
            in_features=key, out_path=arcpy.env.workspace, out_name=value
        )


def from_nct911():
    """Load Collin County Data from NCT-911 Open Data Portal"""
    arcpy.env.workspace = FD_emergency_comm_districts
    arcpy.env.overwriteOutput = True
    print("\tNCT-911 Collin data")
    #key ring is set on the computer the script is run from
    #if your credentials change, it must be updated
    username_NCT911 = keyring.get_password("NCT-911", "username")
    password_NCT911 = keyring.get_password("NCT-911", username_NCT911)
    gis_url = r"https://gisportal.nct911.org/portal"
    gis_fgdb_name = "NCT911_PUBLIC_SAFETY_COLLIN_COUNTY"
    gis_item = "File Geodatabase"
    dict_NCT911Data = {
        "NCT911_PUBLIC_SAFETY_COLLIN_COUNTY_SITESTRUCTUREADDRESSPOINTS": "Collin_Addresses",
        "NCT911_PUBLIC_SAFETY_COLLIN_COUNTY_ROADCENTERLINES": "Collin_Roads",
        "NCT911_PUBLIC_SAFETY_COLLIN_COUNTY_INCORPORATEDMUNICIPALITY": "Collin_Boundaries",
    }
    # Log into NCT911
    portal_NCT911 = arcgis.gis.GIS(
        url=gis_url, username=username_NCT911, password=password_NCT911
    )
    print(f"\t\tLogged in to {portal_NCT911.properties.portalName} as {portal_NCT911.properties.user.username}")
    # Download Collin County FGDB from NCT-911 and Load to Staging EGDB
    query_NCT911_Collin = portal_NCT911.content.search(query=gis_fgdb_name, item_type=gis_item)
    print(f"\t\tAttempting to download: {query_NCT911_Collin}")
    with tempfile.TemporaryDirectory() as download_folder:
        downloaded_fgdb = query_NCT911_Collin[0].download(save_path=download_folder)
        with zipfile.ZipFile(downloaded_fgdb, "r") as zip_ref:
            zip_ref.extractall(download_folder)
        extracted_fgdb = fr"{download_folder}\{gis_fgdb_name}.gdb"
        arcpy.Copy_management(extracted_fgdb, f"{FGDB_connections}\{gis_fgdb_name}.gdb")
        # Iterate through Dictionary for Featureclasses of Interest
        print("\t\tWriting feature classes to Staging SDE")
        for key, value in dict_NCT911Data.items():
            arcpy.conversion.FeatureClassToFeatureClass(
                in_features=fr"{extracted_fgdb}\{key}",
                out_path=arcpy.env.workspace,
                out_name=value,
            )


def from_denco911():
    """Load Denton County from Monthly File Geodatabase"""
    arcpy.env.workspace = FD_emergency_comm_districts
    arcpy.env.overwriteOutput = True
    print("\tDenCo-911 data")
    origin_fgdb = FGDB_denco911
    dict_DenCo911Data = {
        "SiteAddressPoint1": "Denton_Dirty_Addresses",
        "RoadCenterline1": "Denton_Dirty_Roads",
    }
    for key, value in dict_DenCo911Data.items():
        arcpy.conversion.FeatureClassToFeatureClass(
            in_features=fr"{origin_fgdb}\{key}", out_path=arcpy.env.workspace, out_name=value,
        )
