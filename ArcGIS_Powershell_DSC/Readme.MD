### Some quick notes on the ArcGIs Powershell DSC
Run the following PS commands
```powershell
Install-Module 'PSDscResources' -Verbose
Install-Module -Name ArcGIS -Verbose
[DSCLocalConfigurationManager()]
configuration LCMConfig
{
    Node localhost
    {
        Settings
        {
            ConfigurationMode = 'ApplyOnly'
        }
    }
}

```
