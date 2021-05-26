// params

var QUERIES_LIKE = QUERIES_LIKE || ''


// library

function exec(sqlText, binds=[]) {
  let retval = []
  const stmnt = snowflake.createStatement({sqlText, binds})
  try {
    var result = stmnt.execute()
  } catch (e) {
    return {sqlText, 'error': e}
  }

  const columnCount = stmnt.getColumnCount()
  const columnNames = []
  for (let i = 1 ; i < columnCount + 1 ; i++) {
    columnNames.push(stmnt.getColumnName(i))
  }

  while(result.next()) {
    let o = {}
    for (let c of columnNames) {
      o[c] = result.getColumnValue(c)
    }
    retval.push(o)
  }
  return retval
}

// code

const run_id = Math.random().toString(36).substring(2).toUpperCase()
const suppressions = exec(`SHOW VIEWS LIKE '%_ALERT_SUPPRESSION' IN SCHEMA rules`)

const select_all_suppressions_sql = suppressions.map(s => `
  SELECT id, '${s.name}' rule, TRUE suppressed FROM rules.${s.name}
  UNION ALL
  SELECT UUID_STRING() id, '${s.name}' rule, FALSE suppressed -- for count=0 records
`).join('  UNION ALL')

const all_ununsuppressed_rules = `
  SELECT alert:ALERT_ID id, NULL rule, FALSE suppressed
  FROM results.alerts
  WHERE suppressed IS NULL
`

const suppressions_updates = select_all_suppressions_sql.length == 0 ? (
  all_ununsuppressed_rules
) : (`
  ${select_all_suppressions_sql}
  UNION ALL (${all_ununsuppressed_rules})
`)

const NEW_SUPPRESSIONS_TBL = `results.ALERT_SUPPRESSIONS_${run_id}`

const CREATE_SUPPRESSIONS_SQL = `
CREATE TEMP TABLE ${NEW_SUPPRESSIONS_TBL} AS
SELECT id, rule, suppressed, CURRENT_TIMESTAMP ts
FROM (${suppressions_updates})
QUALIFY 1=ROW_NUMBER() OVER (
  PARTITION BY id -- one row per id
  ORDER BY suppressed DESC -- because TRUE > FALSE
)
`

const SUPPRESSION_COUNTS_SQL = `
SELECT rule, COUNT_IF(suppressed) count, ts
FROM ${NEW_SUPPRESSIONS_TBL}
WHERE rule IS NOT NULL
GROUP BY rule, ts
`

const QUERIES_LIKE_SQL = QUERIES_LIKE ? `  AND dst.alert:QUERY_NAME ILIKE ${QUERIES_LIKE}` : ''

const MERGE_SUPRESSIONS_SQL = `
MERGE INTO results.alerts dst
USING ${NEW_SUPPRESSIONS_TBL} s
ON (
  dst.alert:ALERT_ID = s.id
${QUERIES_LIKE_SQL})
WHEN MATCHED THEN UPDATE
SET dst.suppressed = s.suppressed
  , dst.suppression_rule = s.rule
`

return {
  'run_id': run_id,
  'queries_like': QUERIES_LIKE,
  'new_suppressions_tbl': NEW_SUPPRESSIONS_TBL,
  'create_result': exec(CREATE_SUPPRESSIONS_SQL)[0],
  'merge_result': exec(MERGE_SUPRESSIONS_SQL)[0],
  'suppression_counts': exec(SUPPRESSION_COUNTS_SQL),
}
