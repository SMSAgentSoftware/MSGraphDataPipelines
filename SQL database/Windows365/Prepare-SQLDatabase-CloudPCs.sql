-- Create the table 
-- Uses autogenerated ClusterID as the primary key for a clustered index and id as a key for a nonclustered index
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[CloudPCs](
	[ClusterID] [int] IDENTITY(1,1) NOT NULL,
	[id] [varchar](36) NOT NULL,
	[displayName] [varchar](250) NULL,
	[imageDisplayName] [varchar](250) NULL,
	[provisioningPolicyId] [varchar](36) NULL,
	[provisioningPolicyName] [varchar](250) NULL,
	[onPremisesConnectionName] [varchar](250) NULL,
	[servicePlanId] [varchar](36) NULL,
	[servicePlanName] [varchar](150) NULL,
	[userPrincipalName] [varchar](150) NULL,
	[lastModifiedDateTime] [datetime2](7) NULL,
	[managedDeviceId] [varchar](36) NULL,
	[managedDeviceName] [varchar](100) NULL,
	[aadDeviceId] [varchar](36) NULL,
	[gracePeriodEndDateTime] [datetime2](7) NULL,
	[servicePlanType] [varchar](100) NULL,
	[diskEncryptionState] [varchar](100) NULL,
	[provisioningType] [varchar](100) NULL,
	[statusDetails] [varchar](250) NULL,
	[statusDescription] [varchar](250) NULL,
	[timeGenerated] [datetime2](7) NULL
) ON [PRIMARY]
GO
ALTER TABLE [dbo].[CloudPCs] ADD CONSTRAINT [PK_CloudPCs_ClusterID] PRIMARY KEY CLUSTERED 
(
	[ClusterID] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
CREATE NONCLUSTERED INDEX [IX_CloudPCs_id] ON [dbo].[CloudPCs]
(
	[id] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, DROP_EXISTING = OFF, ONLINE = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO




-- Create the table type
-- Do not include the autogenerated ClusterID column or timeGenerated column
CREATE TYPE [dbo].[CloudPCs_Type] AS TABLE(
	[id] [varchar](36) NOT NULL,
	[displayName] [varchar](250) NULL,
	[imageDisplayName] [varchar](250) NULL,
	[provisioningPolicyId] [varchar](36) NULL,
	[provisioningPolicyName] [varchar](250) NULL,
	[onPremisesConnectionName] [varchar](250) NULL,
	[servicePlanId] [varchar](36) NULL,
	[servicePlanName] [varchar](150) NULL,
	[userPrincipalName] [varchar](150) NULL,
	[lastModifiedDateTime] [datetime2](7) NULL,
	[managedDeviceId] [varchar](36) NULL,
	[managedDeviceName] [varchar](100) NULL,
	[aadDeviceId] [varchar](36) NULL,
	[gracePeriodEndDateTime] [datetime2](7) NULL,
	[servicePlanType] [varchar](100) NULL,
	[diskEncryptionState] [varchar](100) NULL,
	[provisioningType] [varchar](100) NULL,
	[statusDetails] [varchar](250) NULL,
	[statusDescription] [varchar](250) NULL
)
GO


-- Create the stored procedure
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[sp_Update_CloudPCs]
@CloudPCsType as CloudPCs_Type READONLY --,
--@MaxInventoryAge AS INT=40 -- Number of days to keep historical data
AS
BEGIN

-- If you only want the latest extracted data, first delete all the current data in the table.
DELETE FROM CloudPCs;

-- If you want to keep historical data up to a certain number of days, remove the DELETE statement above and add the following DELETE statement
--DELETE from ManagedDevices
--where (
--	DATEDIFF(day,timeGenerated,GetDate()) > @MaxInventoryAge
--)

INSERT INTO CloudPCs (
	id,
    displayName,
    imageDisplayName,
    provisioningPolicyId,
    provisioningPolicyName,
    onPremisesConnectionName,
    servicePlanId,
    servicePlanName,
    userPrincipalName,
    lastModifiedDateTime,
    managedDeviceId,
    managedDeviceName,
    aadDeviceId,
    gracePeriodEndDateTime,
    servicePlanType,
    diskEncryptionState,
    provisioningType,
    statusDetails,
    statusDescription,
    timeGenerated
)
SELECT 
    id,
	displayName,
    imageDisplayName,
    provisioningPolicyId,
    provisioningPolicyName,
    onPremisesConnectionName,
    servicePlanId,
    servicePlanName,
    userPrincipalName,
    lastModifiedDateTime,
    managedDeviceId,
    managedDeviceName,
    aadDeviceId,
    gracePeriodEndDateTime,
    servicePlanType,
    diskEncryptionState,
    provisioningType,
    statusDetails,
    statusDescription,
    GETDATE()
FROM @CloudPCsType;

END
GO

-- Grant permissions to the Azure Automation account
-- Substitute the user name with the name of your Azure Automation account
-- On target database
CREATE USER [aa-endpointeng-reporting] FROM EXTERNAL PROVIDER;
ALTER ROLE db_datawriter ADD MEMBER [aa-endpointeng-reporting];
ALTER ROLE db_ddladmin ADD MEMBER [aa-endpointeng-reporting];
ALTER ROLE db_datareader ADD MEMBER [aa-endpointeng-reporting];
-- This is required to execute stored procedures
GRANT EXECUTE TO [aa-endpointeng-reporting];

-- On Master database
-- This is required for SQlBulkCopy
GRANT ADMINISTER DATABASE BULK OPERATIONS TO [aa-endpointeng-reporting];
