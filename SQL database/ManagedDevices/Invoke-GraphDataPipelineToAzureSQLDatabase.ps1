##############################################################################################
# Azure Automation Runbook to extract data from MS Graph and load it into Azure SQL Database #
##############################################################################################


#region ----------------------------------------------- Variables -------------------------------------------------
$ProgressPreference = 'SilentlyContinue' # Speeds up web requests
$429RetryCount = 5 # How many times to retry a request if a 429 status code is received
# Azure SQL database connection string
$AzSQLDBConnectionString = "Server=tcp:hts001azrsql001.database.windows.net,1433;Initial Catalog=DeviceReporting;Persist Security Info=False;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;"
#endregion --------------------------------------------------------------------------------------------------------


#region -------------------------------------------- Graph Endpoints ----------------------------------------------
$GraphEndpoints = [ordered]@{
    "ManagedDevices" = @{
        # Required
        Endpoint = "deviceManagement/managedDevices"
        # Required
        APIVersion= "beta"
        # Optional
        SelectProperties = "id,userId,deviceName,managedDeviceOwnerType,enrolledDateTime,lastSyncDateTime,complianceState,managementAgent,osVersion,azureADRegistered,deviceEnrollmentType,emailAddress,azureADDeviceId,deviceRegistrationState,isEncrypted,userPrincipalName,model,manufacturer,serialNumber,userDisplayName,configurationManagerClientHealthState,managedDeviceName,managementCertificateExpirationDate,usersLoggedOn,joinType,skuFamily,autopilotEnrolled"
        # Optional
        Filter = "operatingSystem eq 'Windows'"
        # Optional
        #Expand = "members"
    }
}
#endregion --------------------------------------------------------------------------------------------------------


#region ----------------------------------------------- Functions -------------------------------------------------
# Function to pop a web request and handle exceptions
Function script:Invoke-WebRequestPro {
    Param ($URL,$Headers,$Method)
    try 
    {
        $WebRequest = Invoke-WebRequest -Uri $URL -Method $Method -Headers $Headers -UseBasicParsing
    }
    catch 
    {
        $Response = $_
        $WebRequest = [PSCustomObject]@{
            Message = $response.Exception.Message
            StatusCode = $response.Exception.Response.StatusCode
            StatusDescription = $response.Exception.Response.StatusDescription
        }
    }
    Return $WebRequest
}

# Function to get data from MS Graph
Function Get-GraphData {
    Param ($Endpoint,$APIVersion,$SelectProperties,$Filter,$Expand,$URL)
    If (-not $URL) 
    {
        $URL = "https://graph.microsoft.com/$APIVersion/$Endpoint"
        if ($Filter -and $SelectProperties) 
        {
            $URL += "?`$filter=$Filter&`$select=$SelectProperties"
        }
        elseif ($Filter) 
        {
            $URL += "?`$filter=$Filter"
        }
        elseif ($SelectProperties) 
        {
            $URL += "?`$select=$SelectProperties"
        }
        if ($Expand)
        {
            If ($Url -like "*?`$*")
            {
                $URL += "&`$expand=$Expand"
            }
            else
            {
                $URL += "?`$expand=$Expand"
            }
        }
    }
    $headers = @{'Authorization'="Bearer " + $GraphToken}
    $GraphRequest = Invoke-WebRequestPro -URL $URL -Headers $headers -Method GET
    return $GraphRequest  
}
#endregion --------------------------------------------------------------------------------------------------------


#region --------------------------------------------- Authentication ----------------------------------------------
# For manual testing, get an access token for MS Graph as the current user
# ref: https://gist.github.com/SMSAgentSoftware/664dc71350a6d926ea1ec7f41ad2ed77
# $script:GraphToken = Get-MicrosoftGraphAccessToken 

# For automation, use a service principal (Managed identity etc)
$AuthRetryCount = 5
$AuthRetries = 0
$AuthSuccess = $false
do {
    try 
    {
        $null = Connect-AzAccount -Identity -ErrorAction Stop
        $script:GraphToken = (Get-AzAccessToken -ResourceTypeName MSGraph -ErrorAction Stop).Token
        $AuthSuccess = $true
    }
    catch 
    {
        $AuthErrorMessage = $_.Exception.Message
        $AuthRetries ++
        Write-Warning "Failed to obtain access token: $AuthErrorMessage."
        Start-Sleep -Seconds 10  
    }
}
until ($AuthSuccess -eq $true -or $AuthRetries -ge $AuthRetryCount)
If ($AuthSuccess -eq $false)
{
    throw "Failed to authenticate as the service principal: $AuthErrorMessage"
}
#endregion --------------------------------------------------------------------------------------------------------


#region ---------------------------------------------- ETL Pipeline -----------------------------------------------
$Stopwatch = [System.Diagnostics.Stopwatch]::new()
foreach ($GraphEndpoint in $GraphEndpoints.Keys) 
{
    #############
    ## Extract ##
    #############
    Write-Output "Starting ETL pipeline for $GraphEndpoint"
    $Stopwatch.Reset()
    $Stopwatch.Start()
    # Prepare and pop the initial request with retry logic in case of a 429 status code
    $Params = @{
        "Endpoint" = $GraphEndpoints["$GraphEndpoint"].Endpoint
        "APIVersion" = $GraphEndpoints["$GraphEndpoint"].APIVersion
        "SelectProperties" = $GraphEndpoints["$GraphEndpoint"].SelectProperties
        "Filter" = $GraphEndpoints["$GraphEndpoint"].Filter
    }
    $429Count = 0
    do {
        $GraphRequest = Get-GraphData @Params
        If ($GraphRequest.StatusCode -eq 429)
        {
            $429Count++
            Write-Warning "429 status code received. Waiting 30 seconds before retrying"
            Start-Sleep -Seconds 30
        }
    }
    until ($GraphRequest.StatusCode -ne 429 -or $429Count -ge $429RetryCount)

    # If not a success code, log the error and continue to the next endpoint  
    If ($GraphRequest.StatusCode -ne 200)
    { 
        Write-Error "Failed to retrieve $GraphEndpoint from MS Graph`: $GraphRequest"
        continue 
    }

    # If no content is returned, log a warning and continue to the next endpoint
    If ($null -eq $GraphRequest.Content)
    {
        Write-Warning "No content returned for $GraphEndpoint"
        continue
    }

    # Extract the content from the request
    $GraphContent = [System.Collections.Generic.List[PSCustomObject]]::new()
    $GraphContentObject = $GraphRequest.Content | ConvertFrom-Json
    $GraphContent.AddRange([PSCustomObject[]]$GraphContentObject.Value) # System.Object[] is not iEnumerable and requires a specific cast

    # If there are more items, get them
    if ($GraphContentObject.'@odata.nextLink')
    {
        do {
            $429Count = 0
            do {
                $GraphRequest = Get-GraphData -Url $GraphContentObject.'@odata.nextLink'
                If ($GraphRequest.StatusCode -eq 429)
                {
                    $429Count++
                    Write-Warning "429 status code received. Waiting 30 seconds before retrying"
                    Start-Sleep -Seconds 30
                }
            }
            until ($GraphRequest.StatusCode -ne 429 -or $429Count -ge $429RetryCount)  
            If ($GraphRequest.StatusCode -ne 200)
            { 
                Write-Error "Failed to retrieve $GraphEndpoint from MS Graph`: $GraphRequest"
                $ODataFail = $true
                break 
            }
            $GraphContentObject = $GraphRequest.Content | ConvertFrom-Json
            $GraphContent.AddRange([PSCustomObject[]]$GraphContentObject.Value)
        }
        until ($null -eq $GraphContentObject.'@odata.nextLink')
    }
    # If any of the pagination requests fail, continue to the next endpoint
    if ($ODataFail -eq $true)
    {
        continue
    }
    Write-Output "Extracted $($GraphContent.Count) items in $($Stopwatch.Elapsed.TotalSeconds) seconds"
    
    
    ###############
    ## Transform ##
    ###############
    # Transform the data before loading to the destination
    switch ($GraphEndpoint) 
    {
        "ManagedDevices" 
        {
            $Stopwatch.Restart()
            foreach ($item in $GraphContent)
            {
                # Expand out the configurationManagerClientHealthState object
                $configurationManagerClientHealthState = $item.configurationManagerClientHealthState
                if ($null -ne $configurationManagerClientHealthState) 
                { 
                    $item.configurationManagerClientHealthState = $configurationManagerClientHealthState.state 
                    $item | Add-Member -MemberType NoteProperty -Name configurationManagerClientErrorCode -Value $configurationManagerClientHealthState.errorCode -TypeName long
                    $item | Add-Member -MemberType NoteProperty -Name configurationManagerClientlastSyncDateTime -Value $configurationManagerClientHealthState.lastSyncDateTime -TypeName datetime
                }

                # Expand out the usersLoggedOn object and select only the most recent
                $usersLoggedOn = $item.usersLoggedOn | Sort lastLogOnDateTime -Descending | Select -first 1
                if ($null -ne $usersLoggedOn) 
                { 
                    $item | Add-Member -MemberType NoteProperty -Name userLoggedOnUserId -Value $usersLoggedOn.userId -TypeName string
                    $item | Add-Member -MemberType NoteProperty -Name userLoggedOnLastLogOnDateTime -Value $usersLoggedOn.lastLogOnDateTime -TypeName datetime
                }
            }

            # Since we've expanded out the usersLoggedOn object, we can remove it from the final output.
            $GraphContentList = [System.Collections.Generic.List[PSCustomObject]]::new()
            foreach ($item in $GraphContent)
            {   
                $GraphContentList.Add(($item | Select-Object * -ExcludeProperty usersLoggedOn))
            }
            $GraphContent = $GraphContentList
            Write-Output "Transformed $($GraphContent.Count) items in $($Stopwatch.Elapsed.TotalSeconds) seconds"
        }
    }

    ##########
    ## Load ##
    ##########
    $Stopwatch.Restart()

    # Grab an access token for the Azure SQL Database
    If ($null -eq $SQLToken)
    {
        $SQLToken = (Get-AzAccessToken -ResourceUrl "https://database.windows.net/").Token
    }

    # Create a datatable to hold the data using the column names defined in the SQL database
    $ManagedDevicesTable = [System.Data.DataTable]::new()
    [void]$ManagedDevicesTable.Columns.Add("id")
    [void]$ManagedDevicesTable.Columns.Add("userId")
    [void]$ManagedDevicesTable.Columns.Add("deviceName")
    [void]$ManagedDevicesTable.Columns.Add("managedDeviceOwnerType")
    [void]$ManagedDevicesTable.Columns.Add("enrolledDateTime", [System.DateTime])
    [void]$ManagedDevicesTable.Columns.Add("lastSyncDateTime", [System.DateTime])
    [void]$ManagedDevicesTable.Columns.Add("complianceState")
    [void]$ManagedDevicesTable.Columns.Add("managementAgent")
    [void]$ManagedDevicesTable.Columns.Add("osVersion")
    [void]$ManagedDevicesTable.Columns.Add("azureADRegistered")
    [void]$ManagedDevicesTable.Columns.Add("deviceEnrollmentType")
    [void]$ManagedDevicesTable.Columns.Add("emailAddress")
    [void]$ManagedDevicesTable.Columns.Add("azureADDeviceId")
    [void]$ManagedDevicesTable.Columns.Add("deviceRegistrationState")
    [void]$ManagedDevicesTable.Columns.Add("isEncrypted")
    [void]$ManagedDevicesTable.Columns.Add("userPrincipalName")
    [void]$ManagedDevicesTable.Columns.Add("model")
    [void]$ManagedDevicesTable.Columns.Add("manufacturer")
    [void]$ManagedDevicesTable.Columns.Add("serialNumber")
    [void]$ManagedDevicesTable.Columns.Add("userDisplayName")
    [void]$ManagedDevicesTable.Columns.Add("configurationManagerClientHealthState"),
    [void]$ManagedDevicesTable.Columns.Add("configurationManagerClientErrorCode"),
    [void]$ManagedDevicesTable.Columns.Add("configurationManagerClientlastSyncDateTime", [System.DateTime]),
    [void]$ManagedDevicesTable.Columns.Add("managedDeviceName")
    [void]$ManagedDevicesTable.Columns.Add("managementCertificateExpirationDate", [System.DateTime])
    [void]$ManagedDevicesTable.Columns.Add("userLoggedOnUserId"),
    [void]$ManagedDevicesTable.Columns.Add("userLoggedOnLastLogOnDateTime", [System.DateTime]),
    [void]$ManagedDevicesTable.Columns.Add("joinType")
    [void]$ManagedDevicesTable.Columns.Add("skuFamily")
    [void]$ManagedDevicesTable.Columns.Add("autopilotEnrolled")


    # Populate the table
    $ColumnNames = $ManagedDevicesTable.Columns.ColumnName
    foreach ($Device in $GraphContent)
    {
        $Row = $ManagedDevicesTable.NewRow()
        foreach ($ColumnName in $ColumnNames)
        {
            # If the property is not null, add it to the row. Otherwise, add a DBNull value
            If ($null -ne $Device.$ColumnName)
            {
                $Row[$ColumnName] = $Device.$ColumnName
            }
            else
            {
                $Row[$ColumnName] = [System.DBNull]::Value
            }
        }

        [void]$ManagedDevicesTable.Rows.Add($Row)
    }

    # Load the data to the Azure SQL Database
    try 
    {
        $connection = New-Object -TypeName System.Data.SqlClient.SqlConnection
        $connection.ConnectionString = $AzSQLDBConnectionString
        $connection.AccessToken = $SQLToken
        $connection.Open()  
        $command = $connection.CreateCommand()
        $command.CommandType = [System.Data.CommandType]::StoredProcedure
        $command.CommandText = "sp_Update_ManagedDevices"

        $Parameter = [System.Data.SqlClient.SqlParameter]::new()
        $Parameter.ParameterName = "@ManagedDevicesType"
        $Parameter.SqlDbType = [System.Data.SqlDbType]::Structured
        $Parameter.TypeName = "ManagedDevices_Type"
        $Parameter.Value = $ManagedDevicesTable
        [void]$command.Parameters.Add($Parameter)
        $reader = $command.ExecuteNonQuery()
        $connection.Close() 
        Write-Output "$reader rows processed (additions and deletions) to SQL table in $($Stopwatch.Elapsed.TotalSeconds) seconds"
    }
    catch 
    {
        Write-Error "Failed to post data to SQL database: $($_.Exception.Message)"
    }
}
$Stopwatch.Stop()
#endregion --------------------------------------------------------------------------------------------------------
