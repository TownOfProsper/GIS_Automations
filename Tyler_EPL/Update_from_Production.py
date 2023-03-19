import arcpy

print("Beginning Operations")
# Refresh Source Layers
arcpy.env.overwriteOutput = True
arcpy.env.addOutputsToMap = False
sde_viewer = r"B:\EGDB_Connections\PublisherView_Production.sde"
sde_tyler_epl = r"B:\EGDB_Connections\ForAdministrationOnly\Admin_Tyler_EPL.sde"
sde_tyler_epl_fds_ago = rf"{sde_tyler_epl}\Tyler_EPL.SDE.AGO_Services"
arcpy.AcceptConnections(sde_tyler_epl, False)
arcpy.DisconnectUser(sde_tyler_epl, "ALL")
update_features = [
    r"Production.SDE.Addressing\Production.SDE.Addresses",
    r"Production.SDE.Engineering\Production.SDE.CapitalImprovementsProgram",
    r"Production.SDE.Land_Records\Production.SDE.Parcels",
    r"Production.SDE.Land_Records\Production.SDE.Subdivisions",
    r"Production.SDE.Transportation\Production.SDE.Roads",
    r"Production.SDE.Planning\Production.SDE.ImpactFeeAreas",
]
for fc in update_features:
    print(f"\tUpdating feature class: {fc.split('.')[-1]}")
    arcpy.conversion.FeatureClassToFeatureClass(
        rf"{sde_viewer}\{fc}", sde_tyler_epl_fds_ago, fc.split(".")[-1]
    )
arcpy.AcceptConnections(sde_tyler_epl, True)
print("Complete")
