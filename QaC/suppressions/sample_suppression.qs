suppression_spec snowflake_login_without_mfa {
  Ruleset = "Kernel Module Modifications"
  GUID    = "7ce9eee71fa5403e9d605343148ddd36"

  Query = <<QUERY
select *
from snowalert.public.alerts 
where alert:AffectedObject = 'DESIGNATED_NOMFA_USER' 
and suppressed is null
QUERY
}