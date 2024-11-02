-- Create the table 
-- Uses autogenerated ClusterID as the primary key for a clustered index and id as a key for a nonclustered index
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[ManagedDevices](
	[ClusterID] [int] IDENTITY(1,1) NOT NULL,
	[id] [varchar](36) NOT NULL,
	[userId] [varchar](36) NULL,
	[deviceName] [varchar](50) NULL,
	[managedDeviceOwnerType] [varchar](50) NULL,
	[enrolledDateTime] [datetime2](7) NULL,
	[lastSyncDateTime] [datetime2](7) NULL,
	[complianceState] [varchar](50) NULL,
	[managementAgent] [varchar](100) NULL,
	[osVersion] [varchar](50) NULL,
	[azureADRegistered] [bit] NULL,
	[deviceEnrollmentType] [varchar](50) NULL,
	[emailAddress] [varchar](100) NULL,
	[azureADDeviceId] [varchar](36) NULL,
	[deviceRegistrationState] [varchar](50) NULL,
	[isEncrypted] [bit] NULL,
	[userPrincipalName] [varchar](150) NULL,
	[model] [varchar](150) NULL,
	[manufacturer] [varchar](50) NULL,
	[serialNumber] [varchar](100) NULL,
	[userDisplayName] [varchar](150) NULL,
    [configurationManagerClientHealthState] [varchar](100) NULL,
    [configurationManagerClientErrorCode] [int] NULL,
    [configurationManagerClientlastSyncDateTime] [datetime2](7) NULL,
	[managedDeviceName] [varchar](150) NULL,
	[managementCertificateExpirationDate] [datetime2](7) NULL,
    [userLoggedOnUserId] [varchar](36) NULL,
    [userLoggedOnLastLogOnDateTime] [datetime2](7) NULL,
	[joinType] [varchar](50) NULL,
	[skuFamily] [varchar](50) NULL,
	[autopilotEnrolled] [bit] NULL,
	[timeGenerated] [datetime2](7) NULL
) ON [PRIMARY]
GO
ALTER TABLE [dbo].[ManagedDevices] ADD CONSTRAINT [PK_ManagedDevices_ClusterID] PRIMARY KEY CLUSTERED 
(
	[ClusterID] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
CREATE NONCLUSTERED INDEX [IX_ManagedDevices_id] ON [dbo].[ManagedDevices]
(
	[id] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, DROP_EXISTING = OFF, ONLINE = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO




-- Create the table type
-- Do not include the autogenerated ClusterID column or timeGenerated column
CREATE TYPE [dbo].[ManagedDevices_Type] AS TABLE(
	[id] [varchar](36) NOT NULL,
	[userId] [varchar](36) NULL,
	[deviceName] [varchar](50) NULL,
	[managedDeviceOwnerType] [varchar](50) NULL,
	[enrolledDateTime] [datetime2](7) NULL,
	[lastSyncDateTime] [datetime2](7) NULL,
	[complianceState] [varchar](50) NULL,
	[managementAgent] [varchar](100) NULL,
	[osVersion] [varchar](50) NULL,
	[azureADRegistered] [bit] NULL,
	[deviceEnrollmentType] [varchar](50) NULL,
	[emailAddress] [varchar](100) NULL,
	[azureADDeviceId] [varchar](36) NULL,
	[deviceRegistrationState] [varchar](50) NULL,
	[isEncrypted] [bit] NULL,
	[userPrincipalName] [varchar](150) NULL,
	[model] [varchar](150) NULL,
	[manufacturer] [varchar](50) NULL,
	[serialNumber] [varchar](100) NULL,
	[userDisplayName] [varchar](150) NULL,
    [configurationManagerClientHealthState] [varchar](100) NULL,
    [configurationManagerClientErrorCode] [int] NULL,
    [configurationManagerClientlastSyncDateTime] [datetime2](7) NULL,
	[managedDeviceName] [varchar](150) NULL,
	[managementCertificateExpirationDate] [datetime2](7) NULL,
    [userLoggedOnUserId] [varchar](36) NULL,
    [userLoggedOnLastLogOnDateTime] [datetime2](7) NULL,
	[joinType] [varchar](50) NULL,
	[skuFamily] [varchar](50) NULL,
	[autopilotEnrolled] [bit] NULL
)
GO


-- Create the stored procedure
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[sp_Update_ManagedDevices]
@ManagedDevicesType as ManagedDevices_Type READONLY --,
--@MaxInventoryAge AS INT=40 -- Number of days to keep historical data
AS
BEGIN

-- If you only want the latest extracted data, first delete all the current data in the table.
DELETE FROM ManagedDevices;

-- If you want to keep historical data up to a certain number of days, remove the DELETE statement above and add the following DELETE statement
--DELETE from ManagedDevices
--where (
--	DATEDIFF(day,timeGenerated,GetDate()) > @MaxInventoryAge
--)

INSERT INTO ManagedDevices (
	id,
	userId,
    deviceName,
    managedDeviceOwnerType,
    enrolledDateTime,
    lastSyncDateTime,
    complianceState,
    managementAgent,
    osVersion,
    azureADRegistered,
    deviceEnrollmentType,
    emailAddress,
    azureADDeviceId,
    deviceRegistrationState,
    isEncrypted,
    userPrincipalName,
    model,
    manufacturer,
    serialNumber,
    userDisplayName,
    configurationManagerClientHealthState,
    configurationManagerClientErrorCode,
    configurationManagerClientlastSyncDateTime,
    managedDeviceName,
    managementCertificateExpirationDate,
    userLoggedOnUserId,
    userLoggedOnLastLogOnDateTime,
    joinType,
    skuFamily,
    autopilotEnrolled,
    timeGenerated
)
SELECT 
    id,
	userId,
    deviceName,
    managedDeviceOwnerType,
    enrolledDateTime,
    lastSyncDateTime,
    complianceState,
    managementAgent,
    osVersion,
    azureADRegistered,
    deviceEnrollmentType,
    emailAddress,
    azureADDeviceId,
    deviceRegistrationState,
    isEncrypted,
    userPrincipalName,
    model,
    manufacturer,
    serialNumber,
    userDisplayName,
    configurationManagerClientHealthState,
    configurationManagerClientErrorCode,
    configurationManagerClientlastSyncDateTime,
    managedDeviceName,
    managementCertificateExpirationDate,
    userLoggedOnUserId,
    userLoggedOnLastLogOnDateTime,
    joinType,
    skuFamily,
    autopilotEnrolled,
    GETDATE()
FROM @ManagedDevicesType;

END
GO

-- Grant permissions to the Azure Automation account
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