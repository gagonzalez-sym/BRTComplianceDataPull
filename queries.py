"""
Function responsible for query checking reinduction success based on parameters
"""
def reinductQuery(siteShort: str, siteFull: str, timeDays: int) -> str:
    query = f"""
    SELECT
        BOTNUMBER,
        '{siteShort}' AS SITE_ID,  
        DATE_TRUNC('week', TIMESTAMP) AS week_start,
        SUM(CASE WHEN ACTION = 'Added' THEN 1 ELSE 0 END) AS inductions_per_week,
        SUM(CASE WHEN ACTION = 'Removed' THEN 1 ELSE 0 END) AS removals_per_week,
        MIN(CASE WHEN ACTION = 'Added' THEN TIMESTAMP END) AS first_induction_time,
        MAX(CASE WHEN ACTION = 'Removed' THEN TIMESTAMP END) AS last_removal_time,
        CASE 
            WHEN MAX(CASE WHEN ACTION = 'Removed' THEN TIMESTAMP END) IS NOT NULL
                 AND MIN(CASE WHEN ACTION = 'Added' THEN TIMESTAMP END) IS NOT NULL
                 AND DATE_TRUNC('week', MAX(CASE WHEN ACTION = 'Removed' THEN TIMESTAMP END))
                     = DATE_TRUNC('week', MIN(CASE WHEN ACTION = 'Added' THEN TIMESTAMP END))
                 AND MAX(CASE WHEN ACTION = 'Removed' THEN TIMESTAMP END) > MIN(CASE WHEN ACTION = 'Added' THEN TIMESTAMP END)
            THEN 'Fail'
            ELSE 'Pass'
        END AS pass_fail
    FROM {siteShort}_FIVETRAN_DB.VIEW_SCHEMA.BOTSINDUCTEDANDREMOVED
    WHERE TIMESTAMP >= CURRENT_DATE - INTERVAL '{timeDays} DAY'
      AND TIMESTAMP < CURRENT_DATE + INTERVAL '1 DAY'
    GROUP BY 
        BOTNUMBER,
        DATE_TRUNC('week', TIMESTAMP)
    ORDER BY week_start DESC, BOTNUMBER;
    """
    return query

"""
Function to generate dwell time query for a given site and time period
"""
def dwellTimeQuery(environment: str, start_date: str, end_date: str) -> str:
    query = f"""
WITH daily_snapshot AS (
  SELECT
    environment,
    DATE_TRUNC('DAY', timestamp) AS event_date,
    bot_id,
    MIN(timestamp) AS snapshot_time,
    MAX(bot_location_changed) AS entry_time
  FROM
    VIEW_DB.TABLEAU_VIEW.CC_BOT_HEALTH_EVENT_RAW
  WHERE
    health_state != 'Unknown'
    AND environment = '{environment}'
    AND EXTRACT(HOUR FROM timestamp) = 23
    AND EXTRACT(MINUTE FROM timestamp) BETWEEN 5 AND 55
  GROUP BY
    environment, bot_id, DATE_TRUNC('DAY', timestamp)
),
bot_dwell_times AS (
  SELECT
    environment,
    event_date,
    bot_id,
    DATEDIFF('day', entry_time, snapshot_time) AS dwell_days
  FROM
    daily_snapshot
  WHERE
    entry_time <= snapshot_time
)
SELECT
  environment,
  event_date,
  COUNT(DISTINCT bot_id) AS unique_bot_count,
  AVG(dwell_days) AS avg_dwell_days
FROM
  bot_dwell_times
WHERE event_date >= '{start_date}'
  AND event_date <= '{end_date}'
GROUP BY
  environment, event_date
ORDER BY
  event_date DESC;
"""
    return query



"""
    Function to generate compliance query for a given site and time period
"""
def complianceQuery(siteShort: str, siteFull: str, timeDays: int) -> str:
    query = f"""
WITH start_date AS (
    SELECT (CURRENT_TIMESTAMP() - INTERVAL '{timeDays} DAY')::TIMESTAMP_NTZ AS start_timestamp
),

BotEvents AS (
    SELECT
        timestamp AS removed_timestamp_est,
        botnumber,
        action
    FROM {siteShort}_FIVETRAN_DB.VIEW_SCHEMA.BOTSINDUCTEDANDREMOVED
    CROSS JOIN start_date
    WHERE timestamp >= start_date.start_timestamp
      AND action = 'Removed'
),

FilteredBotEvents AS (
    SELECT
        be.removed_timestamp_est,
        be.botnumber,
        be.action,
        LAG(be.removed_timestamp_est) OVER (PARTITION BY be.botnumber ORDER BY be.removed_timestamp_est) AS prev_removed_timestamp,
        CASE
            WHEN prev_removed_timestamp IS NULL OR DATEDIFF(MINUTE, prev_removed_timestamp, be.removed_timestamp_est) >= 30 THEN 1
            ELSE 0
        END AS is_new_event
    FROM BotEvents be
),

FlaggedBotEvents AS (
    SELECT
        r.bot_id,
        fbe.removed_timestamp_est,
        MAX(CASE WHEN r.case_handling_disabled = 1 THEN 1 ELSE 0 END) AS case_handling_disabled,
        MAX(CASE WHEN r.system_flagged = 1 THEN 1 ELSE 0 END) AS system_flagged,
        MAX(CASE WHEN r.drive_system_not_ok = 1 THEN 1 ELSE 0 END) AS drive_system_not_ok
    FROM VIEW_DB.TABLEAU_VIEW.CC_BOT_HEALTH_EVENT_RAW r
    JOIN FilteredBotEvents fbe
      ON r.bot_id = fbe.botnumber
     AND CONVERT_TIMEZONE('UTC','America/New_York', r.timestamp::TIMESTAMP_NTZ)
         BETWEEN fbe.removed_timestamp_est - INTERVAL '1 HOUR' AND fbe.removed_timestamp_est
    GROUP BY r.bot_id, fbe.removed_timestamp_est
),

LatestBotStates AS (
    SELECT
        fbe.botnumber AS bot_id,
        fbe.removed_timestamp_est,
        r.bot_state,
        r.health_state,
        r.safety_reason
    FROM FilteredBotEvents fbe
    JOIN VIEW_DB.TABLEAU_VIEW.CC_BOT_HEALTH_EVENT_RAW r
      ON r.bot_id = fbe.botnumber
     AND CONVERT_TIMEZONE('UTC','America/New_York', r.timestamp::TIMESTAMP_NTZ)
         BETWEEN fbe.removed_timestamp_est - INTERVAL '1 HOUR' AND fbe.removed_timestamp_est
    WHERE r.safety_reason IS NOT NULL AND r.safety_reason <> ''
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY fbe.botnumber, fbe.removed_timestamp_est
        ORDER BY r.timestamp DESC
    ) = 1
),

LatestBotStatesGeneral AS (
    SELECT
        fbe.botnumber AS bot_id,
        fbe.removed_timestamp_est,
        r.bot_state,
        r.health_state
    FROM FilteredBotEvents fbe
    JOIN VIEW_DB.TABLEAU_VIEW.CC_BOT_HEALTH_EVENT_RAW r
      ON r.bot_id = fbe.botnumber
     AND CONVERT_TIMEZONE('UTC','America/New_York', r.timestamp::TIMESTAMP_NTZ)
         BETWEEN fbe.removed_timestamp_est - INTERVAL '1 HOUR' AND fbe.removed_timestamp_est
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY fbe.botnumber, fbe.removed_timestamp_est
        ORDER BY r.timestamp DESC
    ) = 1
),

BotErrorEvents AS (
    SELECT
        bot_id,
        CONVERT_TIMEZONE('UTC', 'America/New_York', datetime) AS BDATETIME,
        CASE
            WHEN UPPER(EVENT) = 'ALARM' AND RECOVERFLAG != 'Y' AND (
                    UPPER(ALARMOPERATIONALSTATE) = 'OPERATING' OR
                    (UPPER(ALARMOPERATIONALSTATE) = 'DISABLED' AND SUBEVENT NOT IN ('estimation_error', 'under_voltage'))
                ) THEN 'WRANGLE'
            WHEN EVENT = 'caseHandlingDisabled' THEN 'CHD'
            WHEN EVENT = 'alarm' AND SUBEVENT = 'initialized_with_error' THEN 'IWE'
            WHEN EVENT = 'suspect' AND actiontype IN ('PICK', 'PLACE') AND transfertype IN ('BUFFER_TRANSFER', 'SHELF_TRANSFER') THEN 'SUSPECT'
        END AS ERROR_CATEGORY
    FROM view_db.tableau_view.bderrors
    WHERE site_id ='{siteFull}'
      AND CONVERT_TIMEZONE('UTC', 'America/New_York', datetime) > DATEADD(DAY, -10, CURRENT_TIMESTAMP())
      AND (event IS NULL OR event NOT IN ('logFileWritten', 'behaviorStackTrace', 'constraintViolation'))
      AND TO_NUMBER(level) IS NOT NULL
),

BotErrorCounts AS (
    SELECT
        fbe.botnumber,
        fbe.removed_timestamp_est,
        COUNT_IF(be.ERROR_CATEGORY = 'WRANGLE' AND be.BDATETIME BETWEEN fbe.removed_timestamp_est - INTERVAL '3 DAY' AND fbe.removed_timestamp_est) AS wrangle_count,
        COUNT_IF(be.ERROR_CATEGORY = 'CHD'     AND be.BDATETIME BETWEEN fbe.removed_timestamp_est - INTERVAL '3 DAY' AND fbe.removed_timestamp_est) AS chd_count,
        COUNT_IF(be.ERROR_CATEGORY = 'SUSPECT' AND be.BDATETIME BETWEEN fbe.removed_timestamp_est - INTERVAL '3 DAY' AND fbe.removed_timestamp_est) AS suspect_count,
        COUNT_IF(be.ERROR_CATEGORY = 'IWE'     AND be.BDATETIME BETWEEN fbe.removed_timestamp_est - INTERVAL '1 DAY' AND fbe.removed_timestamp_est) AS iwe_count
    FROM FilteredBotEvents fbe
    LEFT JOIN BotErrorEvents be
      ON fbe.botnumber = be.bot_id
    GROUP BY fbe.botnumber, fbe.removed_timestamp_est
),

DisconnectIssues AS (
    SELECT
        fbe.botnumber,
        fbe.removed_timestamp_est,
        MAX(
            CASE
                WHEN (d."Disconnect Type" = 'Flicker' AND d.events >= 500)
                     OR (d."Disconnect Type" = 'Disconnect' AND d.events >= 5 AND d.disconnect_duration >= 1800)
                THEN 1 ELSE 0
            END
        ) AS has_disconnect_issue
    FROM FilteredBotEvents fbe
    LEFT JOIN (
        SELECT
            "Bot Id",
            "Disconnect Type",
            COUNT(*) AS events,
            SUM("Disconnect Duration") AS disconnect_duration,
            MAX("Lost Time") AS last_lost_time
        FROM view_db.tableau_view.bot_disconnect
        WHERE connect_operational_status = 'Operating'
          AND lost_operational_status = 'Operating'
          AND "Site Id" = '{siteShort}'
        GROUP BY "Bot Id", "Disconnect Type"
    ) d
      ON d."Bot Id" = fbe.botnumber
     AND d.last_lost_time >= fbe.removed_timestamp_est - INTERVAL '72 HOURS'
     AND d.last_lost_time <= fbe.removed_timestamp_est
    GROUP BY fbe.botnumber, fbe.removed_timestamp_est
),

RedFlags AS (
    SELECT DISTINCT
        bot_id,
        CONVERT_TIMEZONE('UTC', 'America/New_York', last_updated::TIMESTAMP_NTZ) AS last_updated_est
    FROM ml_prod.results.bot_recs
    WHERE
        ISSUE_CLASSIFICATIONS ILIKE '%Drive Fault Alarm --> {{CAN1Bus Error%}}'
        OR ISSUE_CLASSIFICATIONS ILIKE '%Traction Feedback Error%'
        OR ISSUE_CLASSIFICATIONS ILIKE '%Power Fault%'
        OR ISSUE_CLASSIFICATIONS ILIKE '%Caster Motor Current%'
        OR ISSUE_CLASSIFICATIONS ILIKE '%Blade Motor Current%'
),

RedFlaggedRemovals AS (
    SELECT
        fbe.botnumber,
        fbe.removed_timestamp_est,
        MAX(CASE WHEN rf.bot_id IS NOT NULL THEN 1 ELSE 0 END) AS red_flag
    FROM FilteredBotEvents fbe
    LEFT JOIN RedFlags rf
      ON rf.bot_id = fbe.botnumber
     AND rf.last_updated_est BETWEEN fbe.removed_timestamp_est - INTERVAL '1 HOUR' AND fbe.removed_timestamp_est
    GROUP BY fbe.botnumber, fbe.removed_timestamp_est
), 

CasegroupStability AS (
    SELECT
        fbe.botnumber AS bot_id,
        fbe.removed_timestamp_est,
        dm.casegroupid,
        MIN(dm.datetime) AS first_seen,
        MAX(dm.datetime) AS last_seen,
        DATEDIFF(MINUTE, MIN(dm.datetime), MAX(dm.datetime)) AS duration_minutes
    FROM FilteredBotEvents fbe
    JOIN symbotic_kafka_db.log_tables.diagnosticmessage dm
      ON fbe.botnumber = dm.bot_id
     AND dm.datetime BETWEEN fbe.removed_timestamp_est - INTERVAL '2 HOUR' AND fbe.removed_timestamp_est
    WHERE dm.casegroupid IS NOT NULL
    GROUP BY fbe.botnumber, fbe.removed_timestamp_est, dm.casegroupid
    HAVING DATEDIFF(MINUTE, MIN(dm.datetime), MAX(dm.datetime)) >= 120
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY fbe.botnumber, fbe.removed_timestamp_est
        ORDER BY MAX(dm.datetime) DESC
    ) = 1
)

SELECT
    TO_CHAR(fbe.removed_timestamp_est, 'YYYY-MM-DD HH24:MI:SS') AS removed_timestamp_est,
    DATE(fbe.removed_timestamp_est) AS removed_date,
    fbe.botnumber,
    fbe.action,
    COALESCE(lbsg.bot_state, 'UNKNOWN') AS bot_state,
    COALESCE(lbsg.health_state, 'UNKNOWN') AS health_state,
    bs.safety_reason,
    SUM(fbe.is_new_event) OVER (PARTITION BY fbe.botnumber) AS "3_DAY_REMOVAL_COUNT",
    ROUND(br.pull_score, 1) AS pull_score,
    br.tcts_daily_avg,
    COALESCE(flg.case_handling_disabled, 0) AS case_handling_disabled,
    COALESCE(flg.system_flagged, 0) AS system_flagged,
    COALESCE(flg.drive_system_not_ok, 0) AS drive_system_not_ok,
    COALESCE(bec.wrangle_count, 0) AS wrangle_count,
    COALESCE(bec.chd_count, 0) AS chd_count,
    COALESCE(bec.suspect_count, 0) AS suspect_count,
    COALESCE(bec.iwe_count, 0) AS iwe_count,
    COALESCE(di.has_disconnect_issue, 0) AS has_disconnect_issue,
    COALESCE(rfr.red_flag, 0) AS red_flag,
    COALESCE(TO_VARCHAR(cg.casegroupid), '0') AS stale_case,

    CASE
        WHEN (br.pull_score > 50 AND br.tcts_daily_avg < 100) THEN 1
        WHEN COALESCE(flg.case_handling_disabled, 0) = 1 THEN 1
        WHEN COALESCE(flg.system_flagged, 0) = 1 THEN 1
        WHEN COALESCE(bec.wrangle_count, 0) >= 3 THEN 1
        WHEN COALESCE(bec.suspect_count, 0) >= 20 THEN 1
        WHEN COALESCE(di.has_disconnect_issue, 0) = 1 THEN 1
        WHEN COALESCE(rfr.red_flag, 0) = 1 THEN 1
        WHEN COALESCE(lbsg.bot_state, 'UNKNOWN') = 'Disabled' AND COALESCE(bec.chd_count, 0) >= 1 THEN 1
        WHEN COALESCE(lbsg.bot_state, 'UNKNOWN') = 'Disabled' AND COALESCE(bec.iwe_count, 0) >= 1 THEN 1
        WHEN cg.casegroupid IS NOT NULL AND bs.safety_reason IS NULL THEN 1
        ELSE 0
    END AS valid_removal,

    CASE
        WHEN (br.pull_score > 50 AND br.tcts_daily_avg < 100) THEN 'Pull Score + TCTS OK'
        WHEN COALESCE(flg.case_handling_disabled, 0) = 1 THEN 'Case Handling Disabled'
        WHEN COALESCE(flg.system_flagged, 0) = 1 THEN 'System Flagged'
        WHEN COALESCE(bec.wrangle_count, 0) >= 3 THEN 'WRANGLE Count >= 3'
        WHEN COALESCE(bec.suspect_count, 0) >= 20 THEN 'SUSPECT Count >= 20'
        WHEN COALESCE(di.has_disconnect_issue, 0) = 1 THEN 'Disconnect Issue'
        WHEN COALESCE(rfr.red_flag, 0) = 1 THEN 'Red Flag Trigger'
        WHEN COALESCE(lbsg.bot_state, 'UNKNOWN') = 'Disabled' AND COALESCE(bec.chd_count, 0) >= 1 THEN 'Disabled + CHD'
        WHEN COALESCE(lbsg.bot_state, 'UNKNOWN') = 'Disabled' AND COALESCE(bec.iwe_count, 0) >= 1 THEN 'Disabled + IWE'
        WHEN cg.casegroupid IS NOT NULL AND bs.safety_reason IS NULL THEN 'Stale Case'
        ELSE NULL
    END AS valid_removal_reason

FROM FilteredBotEvents fbe
LEFT JOIN ml_prod.results.bot_recs br
    ON br.bot_id = fbe.botnumber
    AND br.site_id ='{siteFull}'
    AND br.last_updated = (
        SELECT MAX(br2.last_updated)
        FROM ml_prod.results.bot_recs br2
        WHERE br2.bot_id = fbe.botnumber
          AND br2.site_id ='{siteFull}'
          AND CONVERT_TIMEZONE('UTC','America/New_York', br2.last_updated::TIMESTAMP_NTZ) <= fbe.removed_timestamp_est
    )
LEFT JOIN FlaggedBotEvents flg
    ON flg.bot_id = fbe.botnumber
   AND flg.removed_timestamp_est = fbe.removed_timestamp_est
LEFT JOIN LatestBotStates bs
    ON bs.bot_id = fbe.botnumber
   AND bs.removed_timestamp_est = fbe.removed_timestamp_est
LEFT JOIN LatestBotStatesGeneral lbsg
    ON lbsg.bot_id = fbe.botnumber
   AND lbsg.removed_timestamp_est = fbe.removed_timestamp_est
LEFT JOIN BotErrorCounts bec
    ON bec.botnumber = fbe.botnumber
   AND bec.removed_timestamp_est = fbe.removed_timestamp_est
LEFT JOIN DisconnectIssues di
    ON di.botnumber = fbe.botnumber
   AND di.removed_timestamp_est = fbe.removed_timestamp_est
LEFT JOIN RedFlaggedRemovals rfr
    ON rfr.botnumber = fbe.botnumber
   AND rfr.removed_timestamp_est = fbe.removed_timestamp_est
LEFT JOIN CasegroupStability cg
    ON cg.bot_id = fbe.botnumber
   AND cg.removed_timestamp_est = fbe.removed_timestamp_est
ORDER BY fbe.removed_timestamp_est ASC;
"""
    return query

