import arcpy

SDE_connections = r"B:\EGDB_Connections\ForAdministrationOnly"
SDE_staging = fr"{SDE_connections}\Admin_Staging.sde"
FD_emergency_comm_districts = fr"{SDE_staging}\Staging.SDE.ETL_EmergencyCommsDistricts"

Fire_Boundaries = "Fire_ResponseAreas"
Collin_Boundaries = "Collin_Boundaries"
Collin_Addresses = "Collin_Addresses"
Collin_Roads = "Collin_Roads"
Denton_Clean_Roads = "Denton_Clean_Roads"
Denton_Corrected_Roads = "Denton_Corrected_Roads"
Denton_Enrich_ReplaceAnchor_Roads = "Denton_Enrich_ReplaceAnchor_Roads"
Denton_Enrich_AnchorPoints_Roads = "Denton_Enrich_AnchorPoints_Roads"
Denton_Enrich_MultiFamily_Addresses = "Denton_Enrich_MultiFamily_Addresses"
Denton_Enrich_MultiFamily_Parcels = "Denton_Enrich_MultiFamily_Parcels"
Denton_Clean_Addresses = "Denton_Clean_Addresses"
Denton_Corrected_Addresses = "Denton_Corrected_Addresses"
Denton_Enrich_Labels_Addresses = "Denton_Enrich_Labels_Addresses"
Expanded_Addresses = "Expanded_Addresses"
Expanded_Roads = "Expanded_Roads"
Prosper_Roads = "Prosper_Roads"


def denton_roads_corrected():
    """Load Subset of Denton Data for Appending to Expanded_ Dataset"""
    arcpy.env.workspace = FD_emergency_comm_districts
    arcpy.env.overwriteOutput = True
    print("\t\troads- loading subset of DenCo-911 data to expanded dataset")
    SQL_where_clause = """"Inc_Muni" = 'Prosper'"""
    prosper_boundaries = arcpy.management.SelectLayerByAttribute(
        in_layer_or_view=Collin_Boundaries,
        selection_type="NEW_SELECTION",
        where_clause=SQL_where_clause,
    )
    SQL_where_clause = """"AreaName" = 'District 10 - Artesia'"""
    artesia_boundaries = arcpy.management.SelectLayerByAttribute(
        in_layer_or_view=Fire_Boundaries,
        selection_type="NEW_SELECTION",
        where_clause=SQL_where_clause,
    )
    roads_buffered = arcpy.management.SelectLayerByLocation(
        in_layer=Denton_Clean_Roads,
        overlap_type="INTERSECT",
        select_features=prosper_boundaries,
        search_distance="3.5 Miles",
        selection_type="NEW_SELECTION",
    )
    arcpy.SelectLayerByAttribute_management(
        in_layer_or_view=Collin_Boundaries, selection_type="CLEAR_SELECTION"
    )
    roads_remove_prosper = arcpy.management.SelectLayerByLocation(
        in_layer=roads_buffered,
        overlap_type="HAVE_THEIR_CENTER_IN",
        select_features=Collin_Boundaries,
        search_distance="25 Feet",
        selection_type="REMOVE_FROM_SELECTION",
    )
    roads_remove_artesia = arcpy.management.SelectLayerByLocation(
        in_layer=roads_remove_prosper,
        overlap_type="HAVE_THEIR_CENTER_IN",
        select_features=artesia_boundaries,
        selection_type="REMOVE_FROM_SELECTION",
    )
    roads_remove_anchors = arcpy.management.SelectLayerByLocation(
        in_layer=roads_remove_artesia,
        overlap_type="INTERSECT",
        select_features=Denton_Enrich_AnchorPoints_Roads,
        search_distance="5 Feet",
        selection_type="REMOVE_FROM_SELECTION",
    )
    arcpy.conversion.FeatureClassToFeatureClass(
        in_features=roads_remove_anchors,
        out_path=arcpy.env.workspace,
        out_name=Denton_Corrected_Roads,
    )
    arcpy.management.Append(
        inputs=Denton_Enrich_ReplaceAnchor_Roads,
        target=Denton_Corrected_Roads,
        schema_type="NO_TEST",
    )


def denton_addresses_corrected():
    """Load Subset of Denton Data for Appending to Expanded_ Dataset"""
    arcpy.env.workspace = FD_emergency_comm_districts
    arcpy.env.overwriteOutput = True
    print("\t\taddresses- loading subset of DenCo-911 data to expanded dataset")
    SQL_where_clause = """"Inc_Muni" = 'Prosper'"""
    prosper_boundaries = arcpy.management.SelectLayerByAttribute(
        in_layer_or_view=Collin_Boundaries,
        selection_type="NEW_SELECTION",
        where_clause=SQL_where_clause,
    )
    addresses_buffered = arcpy.management.SelectLayerByLocation(
        in_layer=Denton_Clean_Addresses,
        overlap_type="INTERSECT",
        select_features=prosper_boundaries,
        search_distance="3.5 Miles",
        selection_type="NEW_SELECTION",
    )
    arcpy.SelectLayerByAttribute_management(
        in_layer_or_view=Collin_Boundaries, selection_type="CLEAR_SELECTION"
    )
    addresses_remove_nct911 = arcpy.management.SelectLayerByLocation(
        in_layer=addresses_buffered,
        overlap_type="INTERSECT",
        select_features=Collin_Boundaries,
        selection_type="REMOVE_FROM_SELECTION",
    )
    addresses_add_labels = arcpy.management.SelectLayerByLocation(
        in_layer=addresses_remove_nct911,
        overlap_type="INTERSECT",
        select_features=Denton_Enrich_Labels_Addresses,
        search_distance="100 Feet",
        selection_type="ADD_TO_SELECTION",
    )
    addresses_remove_multifamily = arcpy.management.SelectLayerByLocation(
        in_layer=addresses_add_labels,
        overlap_type="INTERSECT",
        select_features=Denton_Enrich_MultiFamily_Parcels,
        selection_type="REMOVE_FROM_SELECTION",
    )
    arcpy.conversion.FeatureClassToFeatureClass(
        in_features=addresses_remove_multifamily,
        out_path=arcpy.env.workspace,
        out_name=Denton_Corrected_Addresses,
    )


def denton_addresses_labeling():
    """Correct Labeling & Geometry issues in Subset of Denton Data"""
    arcpy.env.workspace = FD_emergency_comm_districts
    arcpy.env.overwriteOutput = True
    dict_labeled_addresses = dict()
    with arcpy.da.SearchCursor(
        in_table=Denton_Enrich_Labels_Addresses,
        field_names=["SITEADDID", "LabelRotation", "SHAPE@"],
    ) as scursor:
        for srow in scursor:
            dict_labeled_addresses.update({srow[0]: srow})
    with arcpy.da.UpdateCursor(
        in_table=Denton_Corrected_Addresses, field_names=["SITEADDID", "Addtl_Loc1", "SHAPE@"]
    ) as ucursor:
        for urow in ucursor:
            key = urow[0]
            if dict_labeled_addresses.get(key) is not None:
                labeled_address = dict_labeled_addresses.get(key)
                urow[0] = labeled_address[0]
                urow[1] = str(labeled_address[1])
                urow[2] = labeled_address[2]
                ucursor.updateRow(urow)


def denton_addresses_multifamily():
    """Correct MultiFamily Labeling & Geometry issues in Subset of Denton Data"""
    arcpy.env.workspace = FD_emergency_comm_districts
    arcpy.env.overwriteOutput = True
    arcpy.management.Append(
        inputs=Denton_Enrich_MultiFamily_Addresses,
        target=Denton_Corrected_Addresses,
        schema_type="NO_TEST",
    )


def collin_addresses_to_expanded():
    """Load Subset of Collin Addresses and Append to Expanded_ Dataset"""
    print("Begin Append Operations\n\tCollin Addresses")
    arcpy.env.workspace = FD_emergency_comm_districts
    arcpy.env.overwriteOutput = True
    SQL_where_clause = """"Inc_Muni" = 'Prosper'"""
    prosper_boundaries = arcpy.management.SelectLayerByAttribute(
        in_layer_or_view=Collin_Boundaries,
        selection_type="NEW_SELECTION",
        where_clause=SQL_where_clause,
    )
    addresses_buffered = arcpy.management.SelectLayerByLocation(
        in_layer=Collin_Addresses,
        overlap_type="INTERSECT",
        select_features=prosper_boundaries,
        search_distance="3.5 Miles",
        selection_type="NEW_SELECTION",
    )
    addresses_remove_prosper = arcpy.management.SelectLayerByLocation(
        in_layer=addresses_buffered,
        overlap_type="INTERSECT",
        select_features=prosper_boundaries,
        selection_type="REMOVE_FROM_SELECTION",
    )
    addresses_remove_response_areas = arcpy.management.SelectLayerByLocation(
        in_layer=addresses_remove_prosper,
        overlap_type="INTERSECT",
        select_features=Fire_Boundaries,
        selection_type="REMOVE_FROM_SELECTION",
    )
    addresses_remove_near_labels = arcpy.management.SelectLayerByLocation(
        in_layer=addresses_remove_response_areas,
        overlap_type="INTERSECT",
        select_features=Denton_Enrich_Labels_Addresses,
        search_distance="100 Feet",
        selection_type="REMOVE_FROM_SELECTION",
    )
    arcpy.management.Append(
        inputs=addresses_remove_near_labels, target=Expanded_Addresses, schema_type="NO_TEST",
    )


def collin_roads_to_expanded():
    """Load Subset of Collin Roads and Append to Expanded_ Dataset"""
    arcpy.env.workspace = FD_emergency_comm_districts
    arcpy.env.overwriteOutput = True
    print("\tCollin Roads")
    SQL_where_clause = """"Inc_Muni" = 'Prosper'"""
    prosper_boundaries = arcpy.management.SelectLayerByAttribute(
        in_layer_or_view=Collin_Boundaries,
        selection_type="NEW_SELECTION",
        where_clause=SQL_where_clause,
    )
    roads_buffered = arcpy.management.SelectLayerByLocation(
        in_layer=Collin_Roads,
        overlap_type="INTERSECT",
        select_features=prosper_boundaries,
        search_distance="3.5 Miles",
        selection_type="NEW_SELECTION",
    )
    collin_roads_remove_prosper_by_boundary = arcpy.management.SelectLayerByLocation(
        in_layer=roads_buffered,
        overlap_type="HAVE_THEIR_CENTER_IN",
        select_features=prosper_boundaries,
        selection_type="REMOVE_FROM_SELECTION",
    )
    set_prosper_roads_nguids = set()
    with arcpy.da.SearchCursor(Prosper_Roads, ["RCL_NGUID"]) as scursor:
        for srow in scursor:
            if srow[0] is not None:
                set_prosper_roads_nguids.add(srow[0])
    sql_list = "', '".join(list(set_prosper_roads_nguids))
    collin_roads_remove_prosper_by_nguid = arcpy.management.SelectLayerByAttribute(
        in_layer_or_view=collin_roads_remove_prosper_by_boundary,
        selection_type="REMOVE_FROM_SELECTION",
        where_clause=f"RCL_NGUID IN ('{sql_list}')",
    )
    arcpy.management.Append(
        inputs=collin_roads_remove_prosper_by_nguid,
        target=Expanded_Roads,
        schema_type="NO_TEST",
    )


def denton_roads_to_expanded():
    """Load Subset of Denton Roads and Append to Expanded_ Dataset"""
    print("\tDenton Roads")
    arcpy.management.Append(
        inputs=Denton_Corrected_Roads, target=Expanded_Roads, schema_type="NO_TEST",
    )


def denton_addresses_to_expanded():
    """Load Subset of Denton Addresses and Append to Expanded_ Dataset"""
    print("\tDenton Addresses")
    arcpy.management.Append(
        inputs=Denton_Corrected_Addresses, target=Expanded_Addresses, schema_type="NO_TEST",
    )
