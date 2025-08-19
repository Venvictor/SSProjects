USE [MMM_CI]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROCEDURE [Target].[SP_THE_Target_Load] 
AS
BEGIN
    SET NOCOUNT ON;

    /*********************************************************************************************************
    CREATED:        Data Integration & Analytics, 2/19/2024 
    DESCRIPTION:    Optimized procedure to load THE targets for SAI initiative
    *********************************************************************************************************/

    DECLARE @ProgramYear CHAR(4) = (
        SELECT TOP(1) [Program_Year_Char] 
        FROM [Target].[Target_Year_Dim] 
        WHERE [Active_Status] = 1
    );

    -- Reset any previously staged new targets
    UPDATE [Target].[THE_Target_Stg] 
    SET [New_Target_Flag] = 0
    WHERE [New_Target_Flag] = 1;

    -------------------------------------------------------------
    -- Stage new targets once and reuse for downstream inserts
    -------------------------------------------------------------
    ;WITH Candidate AS (
        SELECT DISTINCT 
            Program_Year   = @ProgramYear,
            Plan_State     = PAC.PlanState,
            MemID          = PAC.Q_MemID,
            MemID_Type     = 'QNXT',
            DOB            = CAST(PAC.Q_MemDOB AS DATE),
            PAT_ID         = PAT.PatID,
            EPIC_MRN       = PAT.ID14_MRN,
            LOB            = PAC.LOB,
            Target_Type    = 'THE',
            Target_Subtype = 'THE',
            Load_Type      = 'SAI',
            Intake_Period  = MIN(S.DOS) OVER (PARTITION BY PAC.PlanState, PAC.Q_MemID)
        FROM [ADT].[Primary_Active_Coverage]       PAC WITH (NOLOCK)
        INNER JOIN [ADT].[Epic_Patient_List]       PAT ON PAC.Q_MemID = PAT.ID164_QNXTMemID
                                                     AND PAC.Q_MemDOB = PAT.DOB
        INNER JOIN [Target].[Stg_THE_Event_Log]    S   WITH (NOLOCK) 
               ON PAC.Q_MemID = S.EPIC_QNXT_ID
              AND S.EventCode IN ('THE_Encounter','THE_THERAPY_EPISODE')
              AND GETDATE() > S.DOS
        WHERE NOT EXISTS (
            SELECT 1
            FROM [Target].[THE_Target_Log] AS D
            WHERE PAC.PlanState = LEFT(D.Plan_State,2)
              AND PAC.Q_MemID  = D.MemID
              AND D.Program_Year = @ProgramYear
              AND D.Target_Type  = 'THE'
              AND D.Target_TermDate >= CAST(GETDATE() AS DATE)
              AND D.Plan_State = PAC.PlanState
        )
    )
    SELECT
        Target_Unique_ID   = REPLACE(CONCAT(Plan_State,'-',MemID,'-',@ProgramYear,'-',Target_Type),' ','_'),
        Plan_Member_ID     = CONCAT(Plan_State,'-',MemID),
        Program_Year       = Program_Year,
        Plan_State         = Plan_State,
        LOB_From_Src       = LOB,
        MemID              = MemID,
        MemID_Type         = MemID_Type,
        DOB                = DOB,
        PAT_ID             = PAT_ID,
        EPIC_MRN           = EPIC_MRN,
        Target_Type        = Target_Type,
        Target_Subtype     = Target_Subtype,
        Load_Type          = Load_Type,
        Target_EffectiveDate = CASE WHEN Intake_Period < GETDATE() THEN CAST(Intake_Period AS DATE)
                                    ELSE CAST(GETDATE() AS DATE) END,
        Target_TermDate    = DATEFROMPARTS(@ProgramYear,12,31),
        Create_Date        = CAST(GETDATE() AS DATE),
        Last_Updated       = CAST(GETDATE() AS DATE),
        Target_Source      = 'Internal',
        New_Target_Flag    = 1,
        Insert_Timestamp   = CURRENT_TIMESTAMP,
        Update_Timestamp   = CURRENT_TIMESTAMP,
        Multi_Source       = 0,
        Intake_Date        = NULL
    INTO #NewTargets
    FROM Candidate;

    INSERT INTO [Target].[THE_Target_Stg](
          Target_Unique_ID, Plan_Member_ID, Program_Year, Plan_State,
          LOB_From_Src, MemID, MemID_Type, DOB, PAT_ID, EPIC_MRN,
          Target_Type, Target_Subtype, Load_Type,
          Target_EffectiveDate, Target_TermDate, Create_Date, Last_Updated,
          Target_Source, New_Target_Flag, Insert_Timestamp, Update_Timestamp,
          Multi_Source, Intake_Date
    )
    SELECT * FROM #NewTargets;

    INSERT INTO [Target].[THE_Target_Log](
          Target_Unique_ID, Plan_Member_ID, Program_Year, Plan_State,
          LOB_From_Src, MemID, MemID_Type, DOB, PAT_ID, EPIC_MRN,
          Target_Type, Target_Subtype, Load_Type,
          Target_EffectiveDate, Target_TermDate, Create_Date, Last_Updated,
          Target_Source, Insert_Timestamp, Update_Timestamp,
          Multi_Source, Intake_Date
    )
    SELECT
          Target_Unique_ID, Plan_Member_ID, Program_Year, Plan_State,
          LOB_From_Src, MemID, MemID_Type, DOB, PAT_ID, EPIC_MRN,
          Target_Type, Target_Subtype, Load_Type,
          Target_EffectiveDate, Target_TermDate, Create_Date, Last_Updated,
          Target_Source, Insert_Timestamp, Update_Timestamp,
          Multi_Source, Intake_Date
    FROM #NewTargets;

    INSERT INTO [Target].[THE_Target_Dim](
          Target_Unique_ID, Plan_Member_ID, Program_Year, Plan_State,
          LOB_From_Src, MemID, MemID_Type, DOB, PAT_ID, EPIC_MRN,
          Target_Type, Target_Subtype, Load_Type,
          Target_EffectiveDate, Target_TermDate, Create_Date, Last_Updated,
          Target_Source, Insert_Timestamp, Update_Timestamp,
          Multi_Source, Intake_Date
    )
    SELECT
          Target_Unique_ID, Plan_Member_ID, Program_Year, Plan_State,
          LOB_From_Src, MemID, MemID_Type, DOB, PAT_ID, EPIC_MRN,
          Target_Type, Target_Subtype, Load_Type,
          Target_EffectiveDate, Target_TermDate, Create_Date, Last_Updated,
          Target_Source, Insert_Timestamp, Update_Timestamp,
          Multi_Source, Intake_Date
    FROM #NewTargets
    WHERE GETDATE() BETWEEN Target_EffectiveDate AND Target_TermDate;

    -------------------------------------------------------------
    -- Existing TPL load logic retained below
    -------------------------------------------------------------

    -- Step 6a: Get 1 row per member from Target_Patient_Log_Demographics
    DROP TABLE IF EXISTS #DEM;
    SELECT * INTO #DEM
    FROM (
        SELECT DISTINCT D.*,
            Row_Num = ROW_NUMBER() OVER (PARTITION BY MemID, PlanState, DOB
                                         ORDER BY LastName DESC, FirstName DESC, MiddleName DESC)
        FROM (
            SELECT MemID, PlanState, DOB, LastName, FirstName, MiddleName,
                   Epic_MRN, PAT_ID, QNXTStateMemID
            FROM Target.Target_Patient_Log_Demographics
        ) D
    ) A
    WHERE Row_Num = 1;

    -- Step 6b: Remove current year THE records
    DECLARE @ProgramYearInt INT = (
        SELECT TOP(1) Program_Year FROM Target.Target_Year_Dim WHERE Active_Status = 1
    );

    ;WITH TPL_DELETE AS (
        SELECT *
        FROM Target.Target_Patient_Log
        WHERE Target_Type = 'THE' AND ProgramYear = @ProgramYearInt
    )
    DELETE FROM TPL_DELETE;

    -- Step 6c: Insert all current year THE records into TPL
    INSERT INTO Target.Target_Patient_Log (
        ProgramYear, PlanState, MemID, MemberName, DOB, Target_Type,
        Target_Subtype, LoadType, Target_EffectiveDate, Target_TermDate,
        LOB, SNF_Current, SNF_Addr1, SNF_Addr2, SNF_City, SNF_State, SNF_Zip,
        SNF_County, SNF_Phone, SNF_ProvID, SNF_NPI, SNF_Rendering_Prov,
        CreateDate, LastUpdated, Epic_MRN, PAT_ID, QNXTStateMemID,
        Living_Status, Calculated_MemID, Facility_Current, [Source]
    )
    SELECT DISTINCT
        D.Program_Year,
        D.Plan_State,
        D.MemID,
        MemberName = CASE
            WHEN DEM.LastName IS NOT NULL THEN CONCAT(DEM.LastName, ', ', DEM.FirstName, ' ', DEM.MiddleName)
            WHEN LOBS.Q_MemName IS NOT NULL THEN LOBS.Q_MemName
            ELSE 'Unknown'
        END,
        DOB = COALESCE(D.DOB, DEM.DOB),
        D.Target_Type,
        D.Target_Subtype,
        D.Load_Type,
        D.Target_EffectiveDate,
        D.Target_TermDate,
        LOB = COALESCE(LOBS.LOB, LG.LOB, 'Unknown'),
        SNF_Current = FAC.SNF_Flag,
        FAC.SNF_Addr1,
        FAC.SNF_Addr2,
        FAC.SNF_City,
        FAC.SNF_State,
        FAC.SNF_ZipCode,
        FAC.SNF_County,
        FAC.SNF_Phone,
        FAC.SNF_ProvID,
        FAC.SNF_NPI,
        FAC.SNF_Rendering_Prov,
        D.Create_Date,
        D.Last_Updated,
        Epic_MRN = COALESCE(EPL.ID14_MRN, DEM.Epic_MRN),
        PAT_ID = COALESCE(EPL.PatID, DEM.PAT_ID),
        QNXTStateMemID = D.MemID + '_' + D.Plan_State,
        Living_Status = EPL.LivingStatus,
        Calculated_MemID = COALESCE(HX.CURRENT_MEMID, D.MemID),
        FAC.Facility_Current,
        [Source] = D.Target_Source
    FROM Target.THE_Target_Dim AS D
    INNER JOIN Target.Target_Year_Dim AS Y
        ON D.Program_Year = Y.Program_Year AND Y.Active_Status = 1
    LEFT JOIN #DEM AS DEM
        ON D.Plan_State = DEM.PlanState AND D.MemID = DEM.MemID AND D.DOB = DEM.DOB
    LEFT JOIN ADT.Primary_Active_Coverage AS LOBS
        ON D.Plan_State = LOBS.PlanState AND D.MemID = LOBS.Q_MEMID AND D.DOB = LOBS.Q_MemDOB
    LEFT JOIN Target.Target_SNF_Members AS FAC
        ON D.MemID = FAC.MemID AND D.DOB = FAC.DOB
    LEFT JOIN ADT.Epic_Patient_List AS EPL
        ON ( (D.MemID = EPL.ID164_QNXTMemID AND D.MemID IS NOT NULL)
          OR (DEM.QNXTStateMemID = EPL.ID171_QNXTStateMemID AND DEM.QNXTStateMemID IS NOT NULL)
          OR (DEM.Epic_MRN = EPL.ID14_MRN AND DEM.Epic_MRN IS NOT NULL) )
       AND D.DOB = EPL.DOB
    LEFT JOIN Target.Epic_Hx_MemID AS HX
        ON (D.MemID = HX.HX_MEMID OR D.MemID = HX.CURRENT_MEMID) AND D.DOB = HX.Epic_DOB
    LEFT JOIN (
        SELECT MemID, DOB, Plan_State, LOB_From_SRC AS LOB,
               Rownum = ROW_NUMBER() OVER(PARTITION BY MemID, DOB, Plan_State ORDER BY Insert_Timestamp DESC)
        FROM Target.THE_Target_Dim
    ) LG
        ON D.MemID = LG.MemID AND D.DOB = LG.DOB AND D.Plan_State = LG.Plan_State AND LG.Rownum = 1;
END
GO
