# Assigns Graph API permissions to an Enterprise App

# Tenant Id
$TenantId = "<MyTenantId>"
# List of permission names
$RolesToAssign = @(
    "CloudPC.Read.All"
)
# DisplayName of the Enterprise App you are assigning permissions to,
# in this case it is the name of the Automation Account, representing the system-managed identity
$EnterpriseAppName = "<MyAppName>"
# Connect to Graph
Import-Module Microsoft.Graph.Applications
Connect-Graph -TenantId $TenantId -NoWelcome
# Get the service principals
$GraphApp = Get-MgServicePrincipal -Filter "AppId eq '00000003-0000-0000-c000-000000000000'" # Microsoft Graph
$EnterpriseApp = Get-MgServicePrincipal -Filter "DisplayName eq '$EnterpriseAppName'"
# Assign the roles
foreach ($Role in $RolesToAssign) {
    $Role = $GraphApp.AppRoles | Where-Object { $_.Value -eq $Role }
    $params = @{
        principalId = $EnterpriseApp.Id
        resourceId = $GraphApp.Id
        appRoleId = $Role.Id
    }
    New-MgServicePrincipalAppRoleAssignment -ServicePrincipalId $EnterpriseApp.Id -BodyParameter $params
}