import arcpy
import pandas as pd

SDE_connections = r"B:\EGDB_Connections\ForAdministrationOnly"
SDE_staging = fr"{SDE_connections}\Admin_Staging.sde"
SDE_viewer = r"B:\EGDB_Connections\PublisherView_Production.sde"
FD_appraisal_districts = fr"{SDE_staging}\Staging.SDE.ETL_AppraisalDistricts"
FGDB_connections = r"B:\EGDB_Connections\Data_AppraisalDistricts"
viewer_boundary = fr"{SDE_viewer}\Production.SDE.Administrative_Boundaries"
viewer_city_limits = fr"{viewer_boundary}\Production.SDE.City"


code_block_concatenate = """
def concatenate_county(PROP_ID, county_initial):
    if PROP_ID:
        return f"{county_initial}{PROP_ID}"
    else:
        return county_initial
"""

code_block_imp_value = """
def imp_val(imp_homestead, imp_nonhomestead):
    if imp_homestead:
        return imp_homestead
    elif imp_nonhomestead:
        return imp_nonhomestead
    else:
        return
"""

code_block_appraisal_information = """
def url_link(prop_id, url_text):
    if prop_id == 0.0:
        return "No Appraisal Information Available"
    elif isinstance(prop_id, float):
        prop_id = str(prop_id).replace('.0','')
        return f"{url_text}{prop_id}"
    elif prop_id:
        return f"{url_text}{prop_id}"
    else:
        return "No Appraisal Information Available"
"""

parcels_column_order = [
 'prosper_id',
 'prop_id',
 'geo_id',
 'file_as_name',
 'confidential_flag',
 'dba_name',
 'addr_line1',
 'addr_line2',
 'addr_line3',
 'addr_city',
 'addr_state',
 'addr_zip',
 'ml_deliverable',
 'abs_subdv_cd',
 'abs_subdv_desc',
 'block',
 'tract_or_lot',
 'legal_desc',
 'legal_desc_2',
 'situs_num',
 'situs_street_prefx',
 'situs_street',
 'situs_street_sufix',
 'situs_city',
 'situs_state',
 'situs_zip',
 'situs_display',
 'city',
 'school',
 'exemptions',
 'all_entities',
 'legal_acreage',
 'eff_size_acres',
 'land_total_sqft',
 'living_area',
 'state_cd',
 'class_cd',
 'property_use_cd',
 'commercial_flag',
 'eff_yr_blt',
 'yr_blt',
 'zoning',
 'land_type_cd',
 'beds',
 'baths',
 'stories',
 'units',
 'pool',
 'cert_val_yr',
 'cert_imprv_hstd_val',
 'cert_imprv_non_hstd_val',
 'cert_land_hstd_val',
 'cert_land_non_hstd_val',
 'cert_market',
 'cert_appraised_val',
 'cert_assessed_val',
 'cert_imprv_val',
 'appraisal_information',
 'SHAPE']

def fc_reduced():
    """Add Additional Enrichment Fields to CCAD Data"""
    arcpy.env.workspace = FD_appraisal_districts
    arcpy.env.overwriteOutput = True
    print("\treducing data to 3.5 mile buffer")
    list_appraisal_districts = ["CCAD", "DCAD"]
    for district in list_appraisal_districts:
        district_selected = arcpy.SelectLayerByLocation_management(
            f"Archive_{district}",
            "INTERSECT",
            select_features=viewer_city_limits,
            search_distance="3 Miles",
        )
        arcpy.FeatureClassToFeatureClass_conversion(
            district_selected, FD_appraisal_districts, f"Reduced_{district}"
        )


def ccad_add_fields():
    """Add Additional Enrichment Fields to CCAD Data"""
    arcpy.env.workspace = FD_appraisal_districts
    arcpy.env.overwriteOutput = True
    print("\t\tadding fields to CCAD data")
    CCAD_reduced = "Reduced_CCAD"
    ccad_fields = arcpy.ListFields(CCAD_reduced)
    if "cert_imprv_val" not in ccad_fields:
        arcpy.AddField_management(CCAD_reduced, "cert_imprv_val", "TEXT")
    arcpy.CalculateField_management(
        CCAD_reduced,
        "prosper_id",
        "concatenate_county(!GIS_DBO_Parcel_PROP_ID!, 'C')",
        "Python3",
        code_block=code_block_concatenate,
    )
    arcpy.CalculateField_management(
        CCAD_reduced,
        "cert_imprv_val",
        "imp_val(!GIS_DBO_AD_Public_cert_imprv_hs!, !GIS_DBO_AD_Public_cert_imprv_no!)",
        "Python3",
        code_block=code_block_imp_value,
    )
    arcpy.CalculateField_management(
        CCAD_reduced,
        "appraisal_information",
        "url_link(!GIS_DBO_Parcel_PROP_ID!, 'https://www.collincad.org/propertysearch?prop_id=')",
        "Python",
        code_block=code_block_appraisal_information,
    )
def dcad_add_fields():
    """Add Additional Enrichment Fields to DCAD Data"""
    arcpy.env.workspace = FD_appraisal_districts
    arcpy.env.overwriteOutput = True
    print("\t\tadding fields to DCAD data")
    DCAD_reduced = "Reduced_DCAD"
    arcpy.CalculateField_management(
        DCAD_reduced,
        "prosper_id",
        "concatenate_county(!prop_id!, 'D')",
        "Python3",
        code_block=code_block_concatenate,
    )
    arcpy.CalculateField_management(
        DCAD_reduced,
        "prosper_id",
        "str(!prosper_id!).replace('.0', '')",
        "Python3",
        code_block=code_block_concatenate,
    )
    arcpy.CalculateField_management(
        DCAD_reduced,
        "appraisal_information",
        "url_link(!prop_id!, 'https://propaccess.trueautomation.com/clientdb/Property.aspx?cid=19&prop_id=')",
        "Python3",
        code_block=code_block_appraisal_information,
    )

def get_sedf(district: str):
    """Create Spatially Enabled Dataframe"""
    print(f"\tcreating SEDF for {district}")
    sedf = pd.DataFrame.spatial.from_featureclass(
        fr"{FD_appraisal_districts}\Reduced_{district}"
    )
    return sedf


def ccad_cleaning(CCAD_sedf):
    """Drop Fields from CCAD Data and Regularize Field Names"""
    print("\t\tcleaning CCAD data")
    CCAD_sedf["confidential_flag"] = CCAD_sedf["GIS_DBO_AD_Public_confidential_"]
    CCAD_sedf["ml_deliverable"] = CCAD_sedf["GIS_DBO_AD_Public_ml_deliverabl"]
    CCAD_sedf["abs_subdv_desc"] = CCAD_sedf["GIS_DBO_AD_Public_abs_subdv_des"]
    CCAD_sedf["situs_street_prefx"] = CCAD_sedf["GIS_DBO_AD_Public_situs_street_"]
    CCAD_sedf["situs_street_sufix"] = CCAD_sedf["GIS_DBO_AD_Public_situs_street1"]
    CCAD_sedf["eff_size_acres"] = CCAD_sedf["GIS_DBO_AD_Public_eff_size_acre"]
    CCAD_sedf["land_total_sqft"] = CCAD_sedf["GIS_DBO_AD_Public_land_total_sq"]
    CCAD_sedf["property_use_cd"] = CCAD_sedf["GIS_DBO_AD_Public_property_use_"]
    CCAD_sedf["commercial_flag"] = CCAD_sedf["GIS_DBO_AD_Public_commercial_fl"]
    CCAD_sedf["cert_imprv_hstd_val"] = CCAD_sedf["GIS_DBO_AD_Public_cert_imprv_hs"]
    CCAD_sedf["cert_imprv_non_hstd_val"] = CCAD_sedf["GIS_DBO_AD_Public_cert_imprv_no"]
    CCAD_sedf["cert_land_hstd_val"] = CCAD_sedf["GIS_DBO_AD_Public_cert_land_hst"]
    CCAD_sedf["cert_land_non_hstd_val"] = CCAD_sedf["GIS_DBO_AD_Public_cert_land_non"]
    CCAD_sedf["cert_appraised_val"] = CCAD_sedf["GIS_DBO_AD_Public_cert_appraise"]

    CCAD_sedf = CCAD_sedf.drop(
        columns=[
            "OBJECTID",
            "GIS_DBO_Parcel_PROP_ID",
            "GIS_DBO_Parcel_GEO_ID",
            "GIS_DBO_Parcel_created_user",
            "GIS_DBO_Parcel_created_date",
            "GIS_DBO_Parcel_last_edited_user",
            "GIS_DBO_Parcel_last_edited_date",
            "GIS_DBO_Parcel_GlobalID",
            "GIS_DBO_AD_Public_confidential_",
            "GIS_DBO_AD_Public_pct_ownership",
            "GIS_DBO_AD_Public_ml_deliverabl",
            "GIS_DBO_AD_Public_abs_subdv_ref",
            "GIS_DBO_AD_Public_abs_subdv_des",
            "GIS_DBO_AD_Public_mapsco",
            "GIS_DBO_AD_Public_udi_parent_pr",
            "GIS_DBO_AD_Public_condo_pct",
            "GIS_DBO_AD_Public_situs_street_",
            "GIS_DBO_AD_Public_situs_street1",
            "GIS_DBO_AD_Public_tif",
            "GIS_DBO_AD_Public_deed_book_id",
            "GIS_DBO_AD_Public_deed_book_pag",
            "GIS_DBO_AD_Public_deed_num",
            "GIS_DBO_AD_Public_deed_dt",
            "GIS_DBO_AD_Public_deed_type_cd",
            "GIS_DBO_AD_Public_eff_size_acre",
            "GIS_DBO_AD_Public_land_sqft",
            "GIS_DBO_AD_Public_land_total_sq",
            "GIS_DBO_AD_Public_hood_cd",
            "GIS_DBO_AD_Public_property_use_",
            "GIS_DBO_AD_Public_prop_type_cd",
            "GIS_DBO_AD_Public_commercial_fl",
            "GIS_DBO_AD_Public_percent_compl",
            "GIS_DBO_AD_Public_prop_create_d",
            "GIS_DBO_AD_Public_property_stat",
            "GIS_DBO_AD_Public_curr_val_yr",
            "GIS_DBO_AD_Public_curr_imprv_hs",
            "GIS_DBO_AD_Public_curr_imprv_no",
            "GIS_DBO_AD_Public_curr_land_hst",
            "GIS_DBO_AD_Public_curr_ag_use_v",
            "GIS_DBO_AD_Public_curr_ag_marke",
            "GIS_DBO_AD_Public_curr_market",
            "GIS_DBO_AD_Public_curr_ag_loss",
            "GIS_DBO_AD_Public_curr_appraise",
            "GIS_DBO_AD_Public_curr_land_non",
            "GIS_DBO_AD_Public_curr_ten_perc",
            "GIS_DBO_AD_Public_curr_assessed",
            "GIS_DBO_AD_Public_cert_imprv_hs",
            "GIS_DBO_AD_Public_cert_imprv_no",
            "GIS_DBO_AD_Public_cert_land_hst",
            "GIS_DBO_AD_Public_cert_land_non",
            "GIS_DBO_AD_Public_cert_ag_use_v",
            "GIS_DBO_AD_Public_cert_ag_marke",
            "GIS_DBO_AD_Public_cert_ag_loss",
            "GIS_DBO_AD_Public_cert_appraise",
            "GIS_DBO_AD_Public_cert_ten_perc",
            "GIS_DBO_AD_Public_parent_year",
            "GIS_DBO_AD_Public_parent_id",
            "GIS_DBO_AD_Public_parent_block",
            "GIS_DBO_AD_Public_parent_tract",
            "GIS_DBO_AD_Public_parent_acres",
        ]
    )
    CCAD_sedf.columns = CCAD_sedf.columns.str.replace("GIS_DBO_Parcel_", "").str.replace(
        "GIS_DBO_AD_Public_", ""
    )
    CCAD_sedf = CCAD_sedf[CCAD_sedf["prosper_id"] != "C"]
    CCAD_sedf["confidential_flag"] = CCAD_sedf["confidential_flag"].mask(CCAD_sedf["confidential_flag"] != "F", "T")
    return CCAD_sedf

def dcad_cleaning(DCAD_sedf):
    """Drop Fields from DCAD Data and Regularize Field Names"""
    print("\tcleaning DCAD data")
    DCAD_sedf = DCAD_sedf.drop(
        columns=["main_imprv", "ag_exempt", "special_dist", "OBJECTID"])
    DCAD_sedf = DCAD_sedf.rename(
        columns={
            "owner_name": "file_as_name",
            "situs": "situs_display",
            "land_sqft": "land_total_sqft",
            "cad_zoning": "zoning",
            "prop_val_yr": "cert_val_yr",
            "cert_appr_val": "cert_appraised_val",
            "cert_asses_val": "cert_assessed_val",
            "cert_mkt_val": "cert_market",
            "main_imprv_val": "cert_imprv_val",
        }
    )
    DCAD_sedf = DCAD_sedf[DCAD_sedf["prosper_id"] != "D"]
    return DCAD_sedf


def combine_sedf_and_push(CCAD_sedf, DCAD_sedf):
    """Combine sedf from both appraisal districts"""
    print("\tcombining Dataframes")
    sedf_combined = pd.concat([CCAD_sedf, DCAD_sedf], join='outer', ignore_index=True)
    sedf_combined = sedf_combined[parcels_column_order]
    fc_combined = sedf_combined.spatial.to_featureclass(
        location=fr"{FD_appraisal_districts}\Parcels"
    )
    fc_combined_within_town = arcpy.SelectLayerByLocation_management(
        fc_combined, "HAVE_THEIR_CENTER_IN", select_features=viewer_city_limits
    )
    arcpy.CalculateField_management(
        fc_combined_within_town, "situs_city", "'PROSPER'", "Python3"
    )