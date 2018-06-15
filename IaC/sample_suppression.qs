{
  "Ruleset" : "Snowflake User Login without MFA",
  "GUID" : "7ce9eee71fa5403e9d605343148ddd36",
  "Query" : "select * from snowalert.public.alerts where alert:AffectedObject = \'DESIGNATED_NOMFA_USER\' and suppressed is null",
  "SuppressionName" : "Sample Suppression"
}
