import arcgis
import arcpy
import pandas as pd

SDE_connections = r"B:\EGDB_Connections\ForAdministrationOnly"
SDE_staging = fr"{SDE_connections}\Admin_Staging.sde"
FD_emergency_comm_districts = fr"{SDE_staging}\Staging.SDE.ETL_EmergencyCommsDistricts"
FGDB_connections = r"B:\EGDB_Connections\Data_EmergencyCommunicationsDistricts"
FGDB_nct911 = fr"{FGDB_connections}\NCT911_PUBLIC_SAFETY_COLLIN_COUNTY.gdb"


def prosper_full_address():
    """This subroutine alters the Full Address Field for the Addresses FeatureClasses, adding additional information for use by Tyler Enterprise Permitting and Licensing"""
    print("Begin Cleaning Operation")
    arcpy.env.workspace = FD_emergency_comm_districts
    arcpy.env.overwriteOutput = True
    list_AddressesFeatureClasses = ["Prosper_Addresses", "Expanded_Addresses"]
    for fc in list_AddressesFeatureClasses:
        where_clause = "Address IS NOT NULL"
        field = "Address"
        list_fields = "!Building!, !Unit!, !Place_Type!"
        selection_addresses = arcpy.management.SelectLayerByAttribute(
            in_layer_or_view=fc, selection_type="NEW_SELECTION", where_clause=where_clause,
        )
        expression = f"makeFullAddress(!{field}!, [{list_fields}])"
        code_block = """def makeFullAddress(field, list_fields):
        bldg = False
        ste = False
        unit = False
        if list_fields[0] is not None:
            if "BLDG" in list_fields[0]:
                bldg = list_fields[0]
            else:
                bldg = f"BLDG {list_fields[0]}"
        if list_fields[1] is not None:
            if list_fields[2] == "Residential":
                if "UNIT" in list_fields[1]:
                    unit = list_fields[1]
                else:
                    unit = f"UNIT {list_fields[1]}"
            else:
                if "STE" in list_fields[1]:
                    ste = list_fields[1]
                else:
                    ste = f"STE {list_fields[1]}"
        list_concatentation = list(filter(None, ([field, bldg, ste, unit])))
        full_address = " ".join(list_concatentation)
        return full_address
        """
        arcpy.management.CalculateField(
            in_table=selection_addresses,
            field=field,
            expression=expression,
            expression_type="PYTHON3",
            code_block=code_block,
        )


def denton_get_domain(domain_name: str) -> list:
    """This subroutine gets a domain from the DenCo911 FGDB"""
    arcpy.env.workspace = FGDB_nct911
    list_domain = list()
    Domains_NCT = arcpy.da.ListDomains(FGDB_nct911)
    for domain in Domains_NCT:
        if domain.name == "domain_name":
            for key in domain.codedValues:
                list_domain.append(key)
    return list_domain


def denton_add_nguid():
    """This subroutine creates a NENA Global ID for [Addresses, Roads]"""
    arcpy.env.workspace = FD_emergency_comm_districts
    dict_fc = {
        "Denton_Dirty_Addresses": "Site_NGUID",
        "Denton_Dirty_Roads": "RCL_NGUID",
    }
    for key, value in dict_fc.items():
        if arcpy.ListFields(key, value):
            print(f"\tNENA GlobalID Exists Already at {key, value}!")
        else:
            arcpy.management.CalculateField(
                in_table=key,
                field=value,
                expression="str(!GlobalID!) + '@DENCOAREA911.DST.TX.US'",
                expression_type="PYTHON3",
                field_type="TEXT",
            )


def sedf(featureclass: str):
    """This creates a spatially enabled dataframe"""
    sedf = arcgis.features.GeoAccessor.from_featureclass(featureclass)
    return sedf


def white_space(sedf):
    """This subroutine fixes white spaces issues"""
    for col in sedf.columns:
        if sedf[col].dtype != "object":
            continue
        else:
            sedf[col] = sedf[col].str.replace(pat=" +", repl=" ", regex=True)
            sedf[col] = sedf[col].fillna("")
            sedf[col] = sedf[col].map(str.strip)
    return sedf


def refill_na(sedf):
    """This subroutine fixes white spaces issues"""
    for col in sedf.columns:
        if col == "SHAPE":
            continue
        sedf[col].mask(sedf[col] == "", None, inplace=True)
        sedf[col].mask(sedf[col] == 0, None, inplace=True)
        if sedf[col].dtype == "object":
            sedf[col] = sedf[col].str.replace(pat=" +", repl=" ", regex=True)
    return sedf


def roads_add_fields(sedf):
    """This subroutine adds fields from scratch or copies based on existing fields"""
    sedf["NCT_Class"] = ""
    sedf["PostComm_L"] = ""
    sedf["PostComm_R"] = ""
    sedf["Source"] = sedf["DiscrpAgID"]
    sedf["IncMuni_L"] = sedf["MUNILEFT"]
    sedf["IncMuni_R"] = sedf["MUNIRIGHT"]
    sedf["UnincCom_L"] = sedf["MUNILEFT"]
    sedf["UnincCom_R"] = sedf["MUNIRIGHT"]
    return sedf


def roads_enforce_schema(sedf):
    """This subroutine fixes types and configures fields to conform to NCT911 schema"""
    sedf["NO_MSAG"] = pd.to_numeric(sedf["NO_MSAG"], errors="coerce")
    sedf["PD"] = sedf["PD"].str[:9]
    sedf["SD"] = sedf["SD"].str[:9]
    sedf["SN"] = sedf["SN"].str[:60]
    sedf["GC1_EXCEPTION"] = sedf["GC1_EXCEPTION"].str[:20]
    sedf["ZIPLEFT"] = sedf["ZIPLEFT"].str[:7]
    sedf["ZIPRIGHT"] = sedf["ZIPRIGHT"].str[:7]
    sedf["RangeFr"] = sedf["RangeFr"].fillna(0).astype(int)
    sedf["RangeTo"] = sedf["RangeTo"].fillna(0).astype(int)
    sedf["ROADCLASS"] = sedf["ROADCLASS"].fillna(1)
    sedf["ROADCLASS"] = sedf["ROADCLASS"].astype(str)
    sedf["ROADLEVEL"] = sedf["ROADLEVEL"].fillna(0)
    sedf["ROADLEVEL"] = sedf["ROADLEVEL"].astype(str)
    sedf["ESNLEFT"] = sedf["ESNLEFT"].fillna(0)
    sedf["ESNLEFT"] = sedf["ESNLEFT"].astype(int)
    sedf["ESNLEFT"] = sedf["ESNLEFT"].astype(str)
    sedf["ESNLEFT"] = sedf["ESNLEFT"].str.zfill(5)
    sedf["ESNRIGHT"] = sedf["ESNRIGHT"].fillna(0)
    sedf["ESNRIGHT"] = sedf["ESNRIGHT"].astype(int)
    sedf["ESNRIGHT"] = sedf["ESNRIGHT"].astype(str)
    sedf["ESNRIGHT"] = sedf["ESNRIGHT"].str.zfill(5)
    return sedf


# TO DO, more targeted speed limits replacement
# sedf["SPEEDLIMIT"].mask(cond=sedf["SPEEDLIMIT"] == 1.0, other=35, inplace=True)
def roads_data_hygiene(sedf):
    """This subroutine performs simple data hygiene operations"""
    sedf["ONEWAYDIR"].mask(cond=sedf["ONEWAYDIR"] == "", other="B", inplace=True)
    sedf["Parity_L"].mask(cond=sedf["Parity_L"] == "", other="B", inplace=True)
    sedf["Parity_R"].mask(cond=sedf["Parity_R"] == "", other="B", inplace=True)
    sedf["SPEEDLIMIT"].mask(cond=sedf["SPEEDLIMIT"] == 1.0, other=35, inplace=True)
    sedf["IncMuni_L"].mask(cond=sedf["MUNILEFT"].str.contains("COUNTY"), other="", inplace=True)
    sedf["IncMuni_R"].mask(
        cond=sedf["MUNIRIGHT"].str.contains("COUNTY"), other="", inplace=True
    )
    sedf["UnincCom_L"].where(
        cond=sedf["MUNILEFT"].str.contains("COUNTY"), other="", inplace=True
    )
    sedf["UnincCom_R"].where(
        cond=sedf["MUNIRIGHT"].str.contains("COUNTY"), other="", inplace=True
    )
    sedf["St_Notes1"] = (
        sedf["Alias1"]
        .str.cat(sedf[["Alias2", "Alias3", "Alias4", "Alias5"]], sep=" ", na_rep="",)
        .str.strip()
        .str[:225]
    )
    return sedf


def roads_street_names(sedf):
    """This subroutine cleans Street Names and concatenates the Full Street Name"""
    dict_StreetNames_Cleaner = {
        "I 35": "INTERSTATE HIGHWAY 35",
        "I35": "INTERSTATE HIGHWAY 35",
    }
    sedf["SN"].str.replace("HWY ", "HIGHWAY ")
    for key, value in dict_StreetNames_Cleaner.items():
        sedf["SN"].mask(
            cond=sedf["SN"].str.contains(key, regex=False), other=value, inplace=True,
        )
    sedf["St_FullName"] = (
        sedf["PD"]
        .astype("str")
        .str.cat(sedf[["PT", "SN", "ST", "SD", "StN_PosMod"]], sep=" ", na_rep="",)
        .str.strip()
        .str[:255]
    )
    return sedf


def roads_nct_class(sedf):
    """This subroutine conforms the NCT_Class to match the NCT-911 domain"""
    dict_NCT_Class = {
        "0": "1",
        "1": "1",
        "10": "3",
        "2": "1",
        "20": "34",
        "25": "3",
        "3": "1",
        "30": "5",
        "4": "1",
        "40": "7",
        "5": "14",
        "50": "6",
        "9": "15",
    }
    for key, value in dict_NCT_Class.items():
        sedf["NCT_Class"].mask(
            cond=(sedf["ROADCLASS"] == key), other=value, inplace=True,
        )
    sedf["NCT_Class"] = sedf["NCT_Class"].astype("int64")
    return sedf


def roads_road_class(sedf):
    """This subroutine conforms the RoadClass to match the NCT-911 domain"""
    dict_RoadClass = {
        "0": "Local",
        "1": "Secondary",
        "10": "Secondary",
        "2": "Secondary",
        "20": "Primary",
        "25": "Secondary",
        "3": "Other",
        "30": "Primary",
        "4": "Local",
        "40": "Primary",
        "5": "Other",
        "50": "Primary",
        "9": "Private",
    }
    for key, value in dict_RoadClass.items():
        sedf["ROADCLASS"].mask(
            cond=(sedf["ROADCLASS"] == key), other=value, inplace=True,
        )
    return sedf


def roads_road_surface(sedf):
    """This subroutine conforms the RoadSurface to match the NCT-911 domain"""
    dict_RoadSurface = {
        "0.0": "Unknown",
        "1.0": "Asphalt",
        "2.0": "Gravel",
        "3.0": "Dirt",
        "9.0": "Unknown",
    }
    for key, value in dict_RoadSurface.items():
        sedf["ROADLEVEL"].mask(
            cond=(sedf["ROADLEVEL"] == key), other=value, inplace=True,
        )
    return sedf


def roads_maintained_by(sedf):
    """This subroutine conforms the MaintainedBy to match the NCT-911 domain"""
    dict_MaintainedBy = {
        "COUNTY": "County",
        "TEXAS WOMANS UNIVERSITY": "Private",
        "UNIVERSITY OF NORTH TEXAS": "Private",
        "NTTA": "Private",
        "TEXDOT": "State",
        "TXDOT": "State",
    }
    list_MaintainedBy = list(dict_MaintainedBy.values())
    for key, value in dict_MaintainedBy.items():
        sedf["MAINTBY"].mask(
            cond=sedf["MAINTBY"].str.contains(key, regex=True), other=value, inplace=True,
        )
    sedf["MAINTBY"].where(
        cond=sedf["MAINTBY"].isin(list_MaintainedBy), other="City", inplace=True
    )
    return sedf


def roads_zip_codes(sedf):
    """This subroutine assigns Postal Communities based on ZIP Code"""
    dict_ZIP = {
        "76226": "ARGYLE",
        "76227": "AUBREY",
        "75007": "CARROLLTON",
        "75010": "CARROLLTON",
        "75009": "CELINA",
        "75019": "COPPELL",
        "75287": "DALLAS",
        "76234": "DECATUR",
        "76201": "DENTON",
        "76205": "DENTON",
        "76207": "DENTON",
        "76208": "DENTON",
        "76209": "DENTON",
        "76210": "DENTON",
        "75022": "FLOWER MOUND",
        "75028": "FLOWER MOUND",
        "76177": "FORT WORTH",
        "75033": "FRISCO",
        "75034": "FRISCO",
        "75036": "FRISCO",
        "76240": "GAINESVILLE",
        "76117": "HALTOM  CITY",
        "76052": "HASLET",
        "76247": "JUSTIN",
        "76249": "KRUM",
        "75065": "LAKE DALLAS",
        "75057": "LEWISVILLE",
        "75067": "LEWISVILLE",
        "75077": "LEWISVILLE",
        "75068": "LITTLE ELM",
        "76258": "PILOT POINT",
        "75024": "PLANO",
        "75093": "PLANO",
        "76259": "PONDER",
        "75078": "PROSPER",
        "76078": "RHOME",
        "76262": "ROANOKE",
        "76266": "SANGER",
        "76092": "SOUTHLAKE",
        "75056": "THE COLONY",
        "76271": "TIOGA",
        "76272": "VALLEY VIEW",
    }
    for key, value in dict_ZIP.items():
        sedf["PostComm_L"].mask(
            cond=(sedf["ZIPLEFT"] == key), other=value, inplace=True,
        )
        sedf["PostComm_R"].mask(
            cond=(sedf["ZIPRIGHT"] == key), other=value, inplace=True,
        )
    return sedf


def roads_drop_fields(sedf):
    """This subroutine drops fields"""
    sedf.drop(
        [
            "Comments",
            "DelRec",
            "Miles",
            "Alias1",
            "Alias2",
            "Alias3",
            "Alias4",
            "Alias5",
            "CENTERLINEID",
            "FULLNAME",
            "FEDROUTE",
            "FEDRTETYPE",
            "AFEDRTE",
            "AFEDRTETYPE",
            "STROUTE",
            "STRTETYPE",
            "ASTRTE",
            "ASTRTETYPE",
            "CTYROUTE",
            "INWATER",
            "CFCCCODE",
            "LLO_A",
            "RLO_A",
            "LHI_A",
            "RHI_A",
            "MUNILEFT",
            "MUNIRIGHT",
            "LASTEDITOR",
            "CREATION_DATE",
            "CREATION_USER",
            "MODIFY_DATE",
            "MODIFY_USER",
            "NO_MSAG",
            "EVENODD",
            "ChangeType",
            "Effective",
            "NodeBeg",
            "NodeEnd",
            "City",
            "PlainSt",
            "OWNEDBY",
        ],
        axis=1,
        inplace=True,
    )
    return sedf


def roads_rename_fields(sedf):
    """This subroutine renames fields to conform to NCT-911 schema"""
    sedf.rename(
        {
            "FROMLEFT": "FromAddr_L",
            "TOLEFT": "ToAddr_L",
            "FROMRIGHT": "FromAddr_R",
            "TORIGHT": "ToAddr_R",
            "PD": "St_PreDir",
            "PT": "St_PreTyp",
            "SN": "St_Name",
            "ST": "St_PosTyp",
            "SD": "St_PosDir",
            "StN_PosMod": "St_PosMod",
            "ROADCLASS": "RoadClass",
            "ONEWAYDIR": "OneWay",
            "ROADLEVEL": "Surface",
            "MAINTBY": "Maint_Auth",
            "ZIPLEFT": "PostCode_L",
            "ZIPRIGHT": "PostCode_R",
            "GC1_EXCEPTION": "GC_Exception",
            "SPEEDLIMIT": "SpeedLimit",
            "MSAGLEFT": "MSAGComm_L",
            "MSAGRIGHT": "MSAGComm_R",
            "ESNLEFT": "ESN_L",
            "ESNRIGHT": "ESN_R",
            "LASTUPDATE": "DateUpdate",
            "RangeFr": "RangeLow",
            "RangeTo": "RangeHigh",
        },
        axis=1,
        inplace=True,
    )
    return sedf


def addresses_add_fields(sedf):
    """This subroutine adds fields from scratch or copies based on existing fields"""
    sedf["Addtl_Loc1"] = ""
    sedf["Building"] = ""
    sedf["Inc_Muni"] = ""
    sedf["NCT_Type"] = ""
    sedf["Place_Type"] = ""
    sedf["Room"] = ""
    sedf["Uninc_Comm"] = ""
    sedf["Unit"] = ""
    sedf["DiscrpAgID"] = "DENCOAREA911.DST.TX.US"
    sedf["Nbrhd_Comm"] = sedf["Alias1"].str[:100]

    return sedf


def addresses_enforce_schema(sedf):
    """This subroutine fixes types and configures fields to conform to schema"""
    sedf["Building"] = sedf["Building"].str[:25]
    sedf["Country"] = sedf["Country"].str[:35]
    sedf["GC1_EXCEPTION"] = sedf["GC1_EXCEPTION"].str[:20]
    sedf["Inc_Muni"] = sedf["Inc_Muni"].str[:100]
    sedf["Room"] = sedf["Room"].str[:25]
    sedf["Source"] = sedf["Source"].str[:35]
    sedf["State"] = sedf["State"].str[:35]
    sedf["Uninc_Comm"] = sedf["Uninc_Comm"].str[:100]
    sedf["Unit"] = sedf["Unit"].str[:25]
    sedf["PD"] = sedf["PD"].str[:9]
    sedf["SD"] = sedf["SD"].str[:9]
    sedf["SN"] = sedf["SN"].str[:60]
    sedf["ST"] = sedf["ST"].str.upper()
    sedf["ESN"] = sedf["ESN"].astype(str).str.zfill(5).str[:5]
    sedf["CREATION_DATE"] = sedf["CREATION_DATE"].astype(float)
    sedf["MODIFY_DATE"] = sedf["MODIFY_DATE"].astype(float)
    sedf["RPID"] = sedf["RPID"].fillna("0").astype("int64")
    sedf["X"] = sedf["X"].astype(float)
    sedf["Y"] = sedf["Y"].astype(float)
    sedf["GPSX"] = pd.to_numeric(sedf["GPSX"], errors="coerce").astype(float)
    sedf["GPSY"] = pd.to_numeric(sedf["GPSY"], errors="coerce").astype(float)
    sedf["NO_MSAG"] = pd.to_numeric(sedf["NO_MSAG"], errors="coerce")
    sedf["NO_MSAG"] = sedf["NO_MSAG"].fillna(0).astype("int64")
    return sedf


def addresses_data_hygiene(sedf):
    """This subroutine performs simple data hygiene operations"""
    sedf["Addtl_Loc2"] = (
        sedf["Alias1"]
        .str.cat(sedf[["Alias2", "Alias3", "Alias4", "Alias5"]], sep=" ", na_rep="",)
        .str.strip()
        .str[:255]
    )
    sedf["Building"].mask(
        cond=sedf["ALTUNITTYPE"] == "BLDG", other=sedf["ALTUNITID"], inplace=True,
    )
    sedf["Building"].mask(
        cond=sedf["UNITTYPE"] == "BLDG", other=sedf["UNITID"], inplace=True,
    )
    sedf["GPSX"].mask(
        cond=(sedf["GPSY"] == 0) | (sedf["GPSY"] > 40), other=0, inplace=True,
    )
    sedf["GPSX"].mask(cond=sedf["GPSX"] > 0, other=0, inplace=True)
    sedf["GPSY"].mask(cond=sedf["GPSY"] > 40, other=0, inplace=True)
    sedf["Inc_Muni"].where(
        cond=sedf["MUNICIPALITY"].str.contains("COUNTY", regex=True),
        other=sedf["MUNICIPALITY"],
        inplace=True,
    )
    sedf["Room"].mask(
        cond=sedf["UNITTYPE"] == "RM", other=sedf["UNITID"], inplace=True,
    )
    sedf["Uninc_Comm"].mask(
        cond=sedf["MUNICIPALITY"].str.contains("COUNTY", regex=True),
        other=sedf["MUNICIPALITY"],
        inplace=True,
    )
    sedf["Unit"].where(
        cond=(sedf["UNITTYPE"] == "BLDG") | (sedf["UNITTYPE"] == "RM"),
        other=sedf["UNITID"],
        inplace=True,
    )
    return sedf


def addresses_nct_type(sedf):
    """This subroutine conforms the NCT_Class to match the NCT-911 domain"""
    dict_NCT_Type = {
        "R1|R4|R6": "Single Family Residential",
        "R2": "Apartment",
        "CL": "Hotel",
        "R3": "Mobile Home",
        "P1": "Government",
        "P2": "Medical Office",
        "P3": "Place of Worship",
        "P4": "Education",
        "P6|PS": "Public Safety",
        "P7": "Fire",
        "P9": "EMS",
        "P0": "Cemetary",
        "B3": "Barn",
        "B5": "Heliport",
        "D1": "Restaurant",
        "D2": "Gas Station",
        "D3": "Bank",
        "D5": "Golf Course",
        "E3": "Telephone Box",
        "F3": "Library",
        "F5": "Hangar",
        "I3": "Telephone Tower",
    }
    for key, value in dict_NCT_Type.items():
        sedf["NCT_Type"].mask(
            cond=sedf["ADDRCLASS"].str.contains(key, regex=True), other=value, inplace=True,
        )
    sedf["NCT_Type"].mask(cond=sedf["ADDRCLASS"] == "", other="Other", inplace=True)
    return sedf


def addresses_place_type(sedf):
    """This subroutine conforms the PlaceType to match the NCT-911 domain"""
    dict_Place_Type = {
        "R.": "Residential",
        "C.|D.|H1": "Commercial",
        "I.": "Industrial",
        "U.|I3|I2": "Utilities",
        "B.|F.": "Transportation",
        "P1|PS|P6|P7|F3": "Government",
        "R5|P2|P3|P4|P5": "Institution",
    }
    for key, value in dict_Place_Type.items():
        sedf["Place_Type"].mask(
            cond=sedf["ADDRCLASS"].str.contains(key, regex=True), other=value, inplace=True,
        )
    sedf["Place_Type"].mask(cond=sedf["ADDRCLASS"] == "", other="Other", inplace=True)
    return sedf


def addresses_street_names(sedf, domain_list: list):
    """This subroutine cleans Street Names and concatenates the Full Street Name & Full Address"""
    dict_StreetNames_Cleaner = {
        "I 35": "INTERSTATE HIGHWAY 35",
        "I35": "INTERSTATE HIGHWAY 35",
    }
    sedf["SN"].str.replace("HWY ", "HIGHWAY ")
    for key, value in dict_StreetNames_Cleaner.items():
        sedf["SN"].mask(
            cond=sedf["SN"].str.contains(key, regex=False), other=value, inplace=True,
        )
    sedf["ST"].mask(cond=sedf["ST"].isin(domain_list), other="", inplace=True)
    sedf["Address"] = (
        sedf["ADDRNUM"]
        .astype("str")
        .str.cat(sedf[["PD", "PT", "SN", "ST", "SD", "StN_PosMod"]], sep=" ", na_rep="",)
        .str.strip()
        .str[:75]
    )
    sedf["St_FullName"] = (
        sedf["PD"]
        .astype("str")
        .str.cat(sedf[["PT", "SN", "ST", "SD", "StN_PosMod"]], sep=" ", na_rep="",)
        .str.strip()
        .str[:75]
    )
    return sedf


def addresses_drop_fields(sedf):
    """This subroutine drops fields"""
    sedf.drop(
        [
            "DelRec",
            "LR",
            "PictureID",
            "RPID",
            "MapBookPage",
            "OLD_ADDRESS",
            "Alias1",
            "Alias2",
            "Alias3",
            "Alias4",
            "Alias5",
            "ADDPTKEY",
            "ADDRRANGE",
            "ALTUNITTYPE",
            "UNITTYPE",
            "UNITID",
            "ALTUNITID",
            "FULLADDR",
            "PLACENAME",
            "MUNICIPALITY",
            "PSAP",
            "USNGCOORD",
            "POINTTYPE",
            "CAPTUREMETH",
            "STATUS",
            "ADDRCLASS",
            "DATE_ASS",
            "CREATION_DATE",
            "CREATION_USER",
            "MODIFY_DATE",
            "MODIFY_USER",
            "NO_MSAG",
            "GlobalID",
            "ChangeType",
            "X",
            "Y",
            "Site_UID",
            "Apt",
            "LASTUPDATEUTC",
            "DiscrpAgID",
            "Hyperlink",
            "Comments",
            "FULLNAME",
        ],
        axis=1,
        inplace=True,
        errors='ignore',
    )

    return sedf


def addresses_rename_fields(sedf):
    """This subroutine renames fields to conform to NCT-911 schema"""
    sedf.rename(
        {
            "GPSX": "Long",
            "GPSY": "Lat",
            "ParcelNum": "Parcel_ID",
            "PD": "St_PreDir",
            "PT": "St_PreTyp",
            "SN": "St_Name",
            "ST": "St_PosTyp",
            "SD": "St_PosDir",
            "StN_PosMod": "St_PosMod",
            "PREADDRNUM": "AddNum_Pre",
            "ADDRNUMSUF": "AddNum_Suf",
            "ADDRNUM": "Add_Number",
            "Zip": "Post_Code",
            "MSAG": "MSAGComm",
            "OLDLASTUPDATE": "DateUpdate",
            "LASTEDITOR": "UpdateID",
            "GC1_EXCEPTION": "GC_Exception",
        },
        axis=1,
        inplace=True,
    )

    return sedf
