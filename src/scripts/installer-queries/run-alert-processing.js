// args

var CORRELATION_PERIOD_MINUTES;

CORRELATION_PERIOD_MINUTES = CORRELATION_PERIOD_MINUTES || 60;

// library

function exec(sqlText, binds=[]) {
  let retval = []
  const stmnt = snowflake.createStatement({sqlText, binds})
  const result = stmnt.execute()
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

CORRELATE = `
MERGE INTO results.alerts dst
USING (
  SELECT
    d.id alert_id_to_update,
    COALESCE(
      p.correlation_id,
      d.correlation_id,
      UUID_STRING()
    ) correlation_id
  FROM data.alerts d -- destination
  LEFT OUTER JOIN data.alerts p -- potential chain
  ON (
    d.id != p.id
    AND (
      d.alert_time > p.alert_time
      OR (
        d.alert_time = p.alert_time
        AND d.id > p.id
      )
    )
    AND p.alert_time > d.alert_time - INTERVAL '1 hour'
    AND p.correlation_id IS NOT NULL
    AND p.actor = d.actor
    AND (
      p.object = d.object
      OR p.action = d.action
    )
  )
  WHERE d.suppressed = FALSE
    AND d.alert_time > CURRENT_TIMESTAMP - INTERVAL '${CORRELATION_PERIOD_MINUTES} minutes'
  QUALIFY 1=ROW_NUMBER() OVER (
      PARTITION BY d.id  -- one update per destination id
      ORDER BY -- most recent wins
        p.alert_time DESC, p.id DESC
    )
) src
ON (
  dst.alert:ALERT_ID = src.alert_id_to_update
  AND (
    dst.correlation_id IS NULL
    OR dst.correlation_id != src.correlation_id
  )
)
WHEN MATCHED THEN UPDATE SET
  correlation_id = src.correlation_id
`
var n, results = []
do {
  n = exec(CORRELATE)[0]['number of rows updated']
  results.push(n)
} while (n != 0)

return {
  'ROWS_UPDATED': results
}
