import arcpy
import time

SDE_connections = r"B:\EGDB_Connections\ForAdministrationOnly"
SDE_production = fr"{SDE_connections}\Admin_Production.sde"
SDE_staging = fr"{SDE_connections}\Admin_Staging.sde"
FD_emergency_comm_districts = fr"{SDE_staging}\Staging.SDE.ETL_EmergencyCommsDistricts"
FD_appraisal_districts = fr"{SDE_staging}\Staging.SDE.ETL_AppraisalDistricts"


def database_maintenance(enterprise_geodatabase_connection: str):
    print("Begin Database Maintenance")
    """This process performs database maintenance operations: Analyze, Compress, Rebuild"""
    # set environment
    arcpy.env.workspace = enterprise_geodatabase_connection
    arcpy.env.overwriteOutput = True

    # sever active connections
    arcpy.DisconnectUser(enterprise_geodatabase_connection, "ALL")
    arcpy.AcceptConnections(enterprise_geodatabase_connection, False)
    time.sleep(15)
    arcpy.DisconnectUser(enterprise_geodatabase_connection, "ALL")

    # database maintenance operations
    list_tbl = arcpy.ListTables()
    list_fc = arcpy.ListFeatureClasses()
    datasets = arcpy.ListDatasets()
    datasets += list_tbl + list_fc
    # analyze and rebuild indexes makes the compress more performant
    arcpy.management.AnalyzeDatasets(
        enterprise_geodatabase_connection,
        "SYSTEM",
        datasets,
        "ANALYZE_BASE",
        "ANALYZE_DELTA",
        "ANALYZE_ARCHIVE",
    )
    arcpy.management.RebuildIndexes(
        enterprise_geodatabase_connection, "SYSTEM", datasets, "ALL"
    )
    arcpy.management.Compress(enterprise_geodatabase_connection)
    # the compress fragments the index and requires a rebuild via analyze and rebuild indexes
    arcpy.management.AnalyzeDatasets(
        enterprise_geodatabase_connection,
        "SYSTEM",
        datasets,
        "ANALYZE_BASE",
        "ANALYZE_DELTA",
        "ANALYZE_ARCHIVE",
    )
    arcpy.management.RebuildIndexes(
        enterprise_geodatabase_connection, "SYSTEM", datasets, "ALL"
    )
    arcpy.management.AnalyzeDatasets(
        enterprise_geodatabase_connection,
        "SYSTEM",
        datasets,
        "ANALYZE_BASE",
        "ANALYZE_DELTA",
        "ANALYZE_ARCHIVE",
    )
    arcpy.AcceptConnections(enterprise_geodatabase_connection, True)


def database_version_management(enterprise_geodatabase_connection: str):
    """This process reconciles and rebuilds all current versions"""


def staging_get_metadata(enterprise_geodatabase_connection: str):
    """This process overwrites the production layers with data from the staging enterprise geodatabase"""
    print("Begin Metadata Operations")
    arcpy.env.workspace = enterprise_geodatabase_connection
    arcpy.AcceptConnections(enterprise_geodatabase_connection, False)
    arcpy.DisconnectUser(enterprise_geodatabase_connection, "ALL")
    metadata_folder = "B:\Repository\Metadata"
    list_automated_features = ["Addresses", "Roads", "Parcels"]
    list_datasets = arcpy.ListDatasets()
    for dataset in list_datasets:
        list_fc = arcpy.ListFeatureClasses(feature_dataset=fr"{dataset}")
        for fc in list_fc:
            fc_md = arcpy.metadata.Metadata(
                fr"{enterprise_geodatabase_connection}\{dataset}\{fc}"
            )
            if not fc_md.tags:
                fc_md.tags = "Town of Prosper"
                fc_md.save()
            if (fc).split(".")[-1] in list_automated_features:
                print(f"\t{fc}")
                if fc_md.tags == "Town of Prosper":
                    fc_md.importMetadata(
                        fr"{metadata_folder}\Metadata_{(fc).split('.')[-1]}.xml"
                    )
                    fc_md.save()
                else:
                    fc_md.exportMetadata(
                        fr"{metadata_folder}\Metadata_{(fc).split('.')[-1]}.xml"
                    )
            else:
                continue
    arcpy.AcceptConnections(enterprise_geodatabase_connection, True)


def staging_to_production(enterprise_geodatabase_connection: str):
    """This process writes metadata to staging layers"""
    print("Begin Push Operation")
    arcpy.env.workspace = enterprise_geodatabase_connection
    arcpy.AcceptConnections(enterprise_geodatabase_connection, False)
    arcpy.DisconnectUser(enterprise_geodatabase_connection, "ALL")
    dict_staging_to_production = {
        fr"{FD_emergency_comm_districts}\Staging.SDE.Expanded_Addresses": [
            fr"{SDE_production}\Production.SDE.Addressing",
            "Addresses",
        ],
        fr"{FD_emergency_comm_districts}\Staging.SDE.Expanded_Roads": [
            fr"{SDE_production}\Production.SDE.Transportation",
            "Roads",
        ],
        fr"{FD_appraisal_districts}\Staging.SDE.Parcels": [
            fr"{SDE_production}\Production.SDE.Land_Records",
            "Parcels",
        ],
    }
    for key, value in dict_staging_to_production.items():
        arcpy.conversion.FeatureClassToFeatureClass(key, value[0], value[1])
        arcpy.management.AddGlobalIDs(value[0])
        arcpy.management.EnableEditorTracking(
            value[0],
            "created_user",
            "created_date",
            "last_edited_user",
            "last_edited_date",
            "ADD_FIELDS",
            "UTC",
        )
        arcpy.management.RegisterAsVersioned(value[0], "NO_EDITS_TO_BASE")

    arcpy.AcceptConnections(enterprise_geodatabase_connection, True)
