// params

var RUN_ID, ALERT_SQUELCH_POSTFIX;

RUN_ID = RUN_ID || Math.random().toString(36).substring(2).toUpperCase();
ALERT_SQUELCH_POSTFIX = ALERT_SQUELCH_POSTFIX || '_ALERT_SUPPRESSION';

// library

function exec(sqlText, binds=[]) {
  let retval = []
  const stmnt = snowflake.createStatement({sqlText, binds})
  try {
    var result = stmnt.execute()
  } catch (e) {
    return {sqlText, 'error': e}
  }

  const columnCount = stmnt.getColumnCount();
  const columnNames = []
  for (let i = 1 ; i < columnCount + 1 ; i++) {
    columnNames.push(stmnt.getColumnName(i))
  }

  while(result.next()) {
    let o = {};
    for (let c of columnNames) {
      o[c] = result.getColumnValue(c)
    }
    retval.push(o)
  }
  return retval
}

// code

const suppressions = exec(`SHOW VIEWS LIKE '%${ALERT_SQUELCH_POSTFIX}' IN SCHEMA rules;`);

const select_all_suppressions_sql = suppressions.map(s => `
  SELECT id, '${s.name}' rule, TRUE suppressed FROM rules.${s.name}
  UNION ALL
  SELECT UUID_STRING() id, '${s.name}' rule, FALSE suppressed -- for count=0 records
`).join('  UNION ALL')


const NEW_SUPPRESSIONS_TBL = `results.ALERT_SUPPRESSIONS_${RUN_ID}`;

const CREATE_SUPPRESSIONS_SQL = `
CREATE TRANSIENT TABLE ${NEW_SUPPRESSIONS_TBL} AS
SELECT id, rule, suppressed, CURRENT_TIMESTAMP ts
FROM (
${select_all_suppressions_sql}
  UNION ALL (
    SELECT alert:ALERT_ID id, NULL rule, FALSE suppressed
    FROM results.alerts
    WHERE suppressed IS NULL
  )
)
QUALIFY 1=ROW_NUMBER() OVER (
  PARTITION BY id -- one row per id
  ORDER BY suppressed DESC -- because TRUE > FALSE
)
;`;

const SUPPRESSION_COUNTS_SQL = `
SELECT rule, COUNT_IF(suppressed) count, ts
FROM ${NEW_SUPPRESSIONS_TBL}
GROUP BY rule, ts
;`;

const MERGE_SUPRESSIONS_SQL = `
MERGE INTO results.alerts dst
USING ${NEW_SUPPRESSIONS_TBL} s
ON (
  dst.alert:ALERT_ID = s.id
)
WHEN MATCHED THEN UPDATE
SET dst.suppressed = s.suppressed
  , dst.suppression_rule = s.rule
;`;

return {
  'run_id': RUN_ID,
  'new_suppressions_tbl': NEW_SUPPRESSIONS_TBL,
  'create_result': exec(CREATE_SUPPRESSIONS_SQL)[0],
  'merge_result': exec(MERGE_SUPRESSIONS_SQL)[0],
  'suppression_counts': exec(SUPPRESSION_COUNTS_SQL),
}
