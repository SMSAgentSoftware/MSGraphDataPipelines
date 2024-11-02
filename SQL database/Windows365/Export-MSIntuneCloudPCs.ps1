##############################################################################################
# Azure Automation Runbook to extract data from MS Graph and load it into Azure SQL Database #
##############################################################################################


#region ----------------------------------------------- Variables -------------------------------------------------
$ProgressPreference = 'SilentlyContinue' # Speeds up web requests
$429RetryCount = 5 # How many times to retry a request if a 429 status code is received
# Azure SQL database connection string
$AzSQLDBConnectionString = "Server=tcp:<SQLServerName>.database.windows.net,1433;Initial Catalog=<DatabaseName>;Persist Security Info=False;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;"
#endregion --------------------------------------------------------------------------------------------------------


#region -------------------------------------------- Graph Endpoints ----------------------------------------------
$GraphEndpoints = [ordered]@{
    "CloudPCs" = @{
        # Required
        Endpoint = "deviceManagement/virtualEndpoint/cloudPCs"
        # Required
        APIVersion= "v1.0"
        # Optional
        SelectProperties = "id,displayName,imageDisplayName,provisioningPolicyId,provisioningPolicyName,onPremisesConnectionName,servicePlanId,servicePlanName,userPrincipalName,lastModifiedDateTime,managedDeviceId,managedDeviceName,aadDeviceId,gracePeriodEndDateTime,provisioningType"
        # Optional
        #Filter = "operatingSystem eq 'Windows'"
        # Optional
        #Expand = "members"
    }
    "CloudPCAuditEvents" = @{
        # Required
        Endpoint = "deviceManagement/virtualEndpoint/auditEvents"
        # Required
        APIVersion= "v1.0"
        # Optional
        SelectProperties = "id,displayName,componentName,activityDateTime,activityType,activityResult,category,actor,resources"
        # Optional
        #Filter = "operatingSystem eq 'Windows'"
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
        $script:GraphToken = (Get-AzAccessToken -ResourceTypeName MSGraph -AsSecureString -ErrorAction Stop).Token | ConvertFrom-SecureString -AsPlainText
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


#region -------------------------------------------- Pipeline: Extract  -------------------------------------------
$Stopwatch = [System.Diagnostics.Stopwatch]::new()
foreach ($GraphEndpoint in $GraphEndpoints.Keys) 
{
    Write-Output "Starting data extraction for $GraphEndpoint"
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
#endregion --------------------------------------------------------------------------------------------------------


#region -------------------------------------------- Pipeline: Transform ------------------------------------------
    # Transform the data before loading to the destination
    switch ($GraphEndpoint) 
    {
        "CloudPCs" 
        {
        }
        "CloudPCAuditEvents" 
        {
            Write-Output "Starting data transformation for $GraphEndpoint"
            $Stopwatch.Restart()
            foreach ($item in $GraphContent)
            {
                # Expand out the actor object
                $actor = $item.actor
                if ($null -ne $actor) 
                { 
                    $item | Add-Member -MemberType NoteProperty -Name actorApplicationDisplayName -Value $actor.applicationDisplayName -TypeName string
                    $item | Add-Member -MemberType NoteProperty -Name actorUserPrincipalName -Value $actor.userPrincipalName -TypeName string
                }

                # Expand out the resources object
                $resources = $item.resources
                if ($null -ne $resources) 
                { 
                    $item | Add-Member -MemberType NoteProperty -Name resourcesDisplayName -Value $resources.displayName -TypeName string
                }
            }

            # Since we've expanded out the actor and resources objects, we can remove them from the final output.
            $GraphContentList = [System.Collections.Generic.List[PSCustomObject]]::new()
            foreach ($item in $GraphContent)
            {   
                $GraphContentList.Add(($item | Select-Object * -ExcludeProperty resources,actor))
            }
            $GraphContent = $GraphContentList
            Write-Output "Transformed $($GraphContent.Count) items in $($Stopwatch.Elapsed.TotalSeconds) seconds"
        }
    }
#endregion --------------------------------------------------------------------------------------------------------


#region -------------------------------------------- Pipeline: Load -----------------------------------------------
    switch ($GraphEndpoint) 
    {
        "CloudPCs"
        {
            Write-Output "Starting data load for $GraphEndpoint"
            $Stopwatch.Restart()

            # Grab an access token for the Azure SQL Database
            If ($null -eq $SQLToken)
            {
                $SQLToken = (Get-AzAccessToken -ResourceUrl "https://database.windows.net/" -AsSecureString -ErrorAction Stop).Token | ConvertFrom-SecureString -AsPlainText
            }

            # Create a datatable to hold the data using the column names defined in the SQL database
            $CloudPCTable = [System.Data.DataTable]::new()
            [void]$CloudPCTable.Columns.Add("id")
            [void]$CloudPCTable.Columns.Add("displayName")
            [void]$CloudPCTable.Columns.Add("imageDisplayName")
            [void]$CloudPCTable.Columns.Add("provisioningPolicyId")
            [void]$CloudPCTable.Columns.Add("provisioningPolicyName")
            [void]$CloudPCTable.Columns.Add("onPremisesConnectionName")
            [void]$CloudPCTable.Columns.Add("servicePlanId")
            [void]$CloudPCTable.Columns.Add("servicePlanName")
            [void]$CloudPCTable.Columns.Add("userPrincipalName")
            [void]$CloudPCTable.Columns.Add("lastModifiedDateTime", [System.DateTime])
            [void]$CloudPCTable.Columns.Add("managedDeviceId")
            [void]$CloudPCTable.Columns.Add("managedDeviceName")
            [void]$CloudPCTable.Columns.Add("aadDeviceId")
            [void]$CloudPCTable.Columns.Add("gracePeriodEndDateTime", [System.DateTime])
            [void]$CloudPCTable.Columns.Add("provisioningType")

            # Populate the table
            $ColumnNames = $CloudPCTable.Columns.ColumnName
            foreach ($Entry in $GraphContent)
            {
                $Row = $CloudPCTable.NewRow()
                foreach ($ColumnName in $ColumnNames)
                {
                    # If the property is not null, add it to the row. Otherwise, add a DBNull value
                    If ($null -ne $Entry.$ColumnName)
                    {
                        $Row[$ColumnName] = $Entry.$ColumnName
                    }
                    else
                    {
                        $Row[$ColumnName] = [System.DBNull]::Value
                    }
                }

                [void]$CloudPCTable.Rows.Add($Row)
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
                $command.CommandText = "sp_Update_CloudPCs"

                $Parameter = [System.Data.SqlClient.SqlParameter]::new()
                $Parameter.ParameterName = "@CloudPCsType"
                $Parameter.SqlDbType = [System.Data.SqlDbType]::Structured
                $Parameter.TypeName = "CloudPCs_Type"
                $Parameter.Value = $CloudPCTable
                [void]$command.Parameters.Add($Parameter)
                $reader = $command.ExecuteNonQuery()
                $connection.Close() 
                Write-Output "$reader rows processed (additions and deletions) to SQL table in $($Stopwatch.Elapsed.TotalSeconds) seconds"
            }
            catch 
            {
                throw "Failed to post data to SQL database: $($_.Exception.Message)"
            }
            $Stopwatch.Stop()
        }
        "CloudPCAuditEvents"
        {
            Write-Output "Starting data load for $GraphEndpoint"
            $Stopwatch.Restart()

            # Grab an access token for the Azure SQL Database
            If ($null -eq $SQLToken)
            {
                $SQLToken = (Get-AzAccessToken -ResourceUrl "https://database.windows.net/" -AsSecureString -ErrorAction Stop).Token | ConvertFrom-SecureString -AsPlainText
            }

            # Create a datatable to hold the data using the column names defined in the SQL database
            $CloudPCAuditEventsTable = [System.Data.DataTable]::new()
            [void]$CloudPCAuditEventsTable.Columns.Add("id")
            [void]$CloudPCAuditEventsTable.Columns.Add("displayName")
            [void]$CloudPCAuditEventsTable.Columns.Add("componentName")
            [void]$CloudPCAuditEventsTable.Columns.Add("activityDateTime", [System.DateTime])
            [void]$CloudPCAuditEventsTable.Columns.Add("activityType")
            [void]$CloudPCAuditEventsTable.Columns.Add("activityResult")
            [void]$CloudPCAuditEventsTable.Columns.Add("category")
            [void]$CloudPCAuditEventsTable.Columns.Add("actorApplicationDisplayName")
            [void]$CloudPCAuditEventsTable.Columns.Add("actorUserPrincipalName")
            [void]$CloudPCAuditEventsTable.Columns.Add("resourcesDisplayName")

            # Populate the table
            $ColumnNames = $CloudPCAuditEventsTable.Columns.ColumnName
            foreach ($Entry in $GraphContent)
            {
                $Row = $CloudPCAuditEventsTable.NewRow()
                foreach ($ColumnName in $ColumnNames)
                {
                    # If the property is not null, add it to the row. Otherwise, add a DBNull value
                    If ($null -ne $Entry.$ColumnName)
                    {
                        $Row[$ColumnName] = $Entry.$ColumnName
                    }
                    else
                    {
                        $Row[$ColumnName] = [System.DBNull]::Value
                    }
                }

                [void]$CloudPCAuditEventsTable.Rows.Add($Row)
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
                $command.CommandText = "sp_Update_CloudPCAuditEvents"

                $Parameter = [System.Data.SqlClient.SqlParameter]::new()
                $Parameter.ParameterName = "@CloudPCAuditEventsType"
                $Parameter.SqlDbType = [System.Data.SqlDbType]::Structured
                $Parameter.TypeName = "CloudPCAuditEvents_Type"
                $Parameter.Value = $CloudPCAuditEventsTable
                [void]$command.Parameters.Add($Parameter)
                $reader = $command.ExecuteNonQuery()
                $connection.Close() 
                Write-Output "$reader rows processed (additions and deletions) to SQL table in $($Stopwatch.Elapsed.TotalSeconds) seconds"
            }
            catch 
            {
                throw "Failed to post data to SQL database: $($_.Exception.Message)"
            }
            $Stopwatch.Stop()
        }
    }
}
#endregion --------------------------------------------------------------------------------------------------------