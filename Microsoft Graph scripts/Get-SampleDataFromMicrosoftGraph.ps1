# Get sample data from a Microsoft Graph endpoint

# Using MgGraph
Connect-MgGraph -NoWelcome -Scopes "CloudPC.Read.All"
$GraphRequest = Invoke-MgGraphRequest -Method GET "https://graph.microsoft.com/v1.0/deviceManagement/virtualEndpoint/cloudPCs"  
$GraphRequest.value

# Using a web request with an access token
# Uses my Get-MicrosoftGraphAccessToken function: https://gist.github.com/SMSAgentSoftware/664dc71350a6d926ea1ec7f41ad2ed77
$GraphToken = Get-MicrosoftGraphAccessToken
$URL = "https://graph.microsoft.com/v1.0/deviceManagement/virtualEndpoint/cloudPCs"  
$headers = @{'Authorization'="Bearer " + $GraphToken}
$GraphRequest = Invoke-WebRequest -URI $URL -Headers $headers -Method GET
if ($GraphRequest.StatusCode -eq 200)
{
    $Content = ($GraphRequest.Content | ConvertFrom-Json).value
}
$Content