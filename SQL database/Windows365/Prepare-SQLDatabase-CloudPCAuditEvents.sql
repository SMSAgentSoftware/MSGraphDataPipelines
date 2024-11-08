-- Create the table 
-- Uses autogenerated ClusterID as the primary key for a clustered index and id as a key for a nonclustered index
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[CloudPCAuditEvents](
	[ClusterID] [int] IDENTITY(1,1) NOT NULL,
	[id] [varchar](36) NOT NULL,
	[displayName] [varchar](150) NULL,
	[componentName] [varchar](100) NULL,
	[activityDateTime] [datetime2](7) NULL,
	[activityType] [varchar](150) NULL,
	[activityResult] [varchar](50) NULL,
	[category] [varchar](50) NULL,
	[actorApplicationDisplayName] [varchar](150) NULL,
	[actorUserPrincipalName] [varchar](100) NULL,
	[resourcesDisplayName] [varchar](150) NULL,
	[timeGenerated] [datetime2](7) NULL
) ON [PRIMARY]
GO
ALTER TABLE [dbo].[CloudPCAuditEvents] ADD CONSTRAINT [PK_CloudPCAuditEvents_ClusterID] PRIMARY KEY CLUSTERED 
(
	[ClusterID] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
CREATE NONCLUSTERED INDEX [IX_CloudPCAuditEvents_id] ON [dbo].[CloudPCAuditEvents]
(
	[id] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, DROP_EXISTING = OFF, ONLINE = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO




-- Create the table type
-- Do not include the autogenerated ClusterID column or timeGenerated column
CREATE TYPE [dbo].[CloudPCAuditEvents_Type] AS TABLE(
	[id] [varchar](36) NOT NULL,
	[displayName] [varchar](150) NULL,
	[componentName] [varchar](100) NULL,
	[activityDateTime] [datetime2](7) NULL,
	[activityType] [varchar](150) NULL,
	[activityResult] [varchar](50) NULL,
	[category] [varchar](50) NULL,
	[actorApplicationDisplayName] [varchar](150) NULL,
	[actorUserPrincipalName] [varchar](100) NULL,
	[resourcesDisplayName] [varchar](150) NULL
)
GO


-- Create the stored procedure
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[sp_Update_CloudPCAuditEvents]
@CloudPCAuditEventsType as CloudPCAuditEvents_Type READONLY --,
--@MaxInventoryAge AS INT=40 -- Number of days to keep historical data
AS
BEGIN

-- If you only want the latest extracted data, first delete all the current data in the table.
DELETE FROM CloudPCAuditEvents;

-- If you want to keep historical data up to a certain number of days, remove the DELETE statement above and add the following DELETE statement
--DELETE from ManagedDevices
--where (
--	DATEDIFF(day,timeGenerated,GetDate()) > @MaxInventoryAge
--)

INSERT INTO CloudPCAuditEvents (
	id,
    displayName,
    componentName,
    activityDateTime,
    activityType,
    activityResult,
    category,
    actorApplicationDisplayName,
    actorUserPrincipalName,
    resourcesDisplayName,
    timeGenerated
)
SELECT 
    id,
	displayName,
    componentName,
    activityDateTime,
    activityType,
    activityResult,
    category,
    actorApplicationDisplayName,
    actorUserPrincipalName,
    resourcesDisplayName,
    GETDATE()
FROM @CloudPCAuditEventsType;

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
