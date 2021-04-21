// args

var FROM_TIME_SQL, TO_TIME_SQL, CUTOFF_MINUTES, QUERY_NAME;

CUTOFF_MINUTES = CUTOFF_MINUTES || 60 * 24;
FROM_TIME_SQL = FROM_TIME_SQL || `DATE_TRUNC(HOURS, CURRENT_TIMESTAMP) - INTERVAL '${CUTOFF_MINUTES} MINUTES'`;
TO_TIME_SQL = TO_TIME_SQL || 'CURRENT_TIMESTAMP';

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

function fillArray(value, len) {
  const arr = []
  for (var i = 0; i < len; i++) {
    arr.push(value)
  }
  return arr
}

function unindent(s) {
  const min_indent = Math.min(...[...s.matchAll('\n *')].map(x => x[0].length));
  return s.replace('\n' + ' '.repeat(min_indent), '\n')
}

const RUN_ID = Math.random().toString(36).substring(2).toUpperCase();

const insert_violations = exec("SHOW VIEWS LIKE '%_VIOLATION_QUERY' IN SCHEMA rules")
  .map(v => v.name)
  .map(query_name => exec(unindent(`
    INSERT INTO results.violations (alert_time, id, result)
    SELECT CURRENT_TIMESTAMP()
      , MD5(TO_JSON(
          IFNULL(
            OBJECT_CONSTRUCT(*):IDENTITY,
            OBJECT_CONSTRUCT(
                'ENVIRONMENT', IFNULL(OBJECT_CONSTRUCT(*):ENVIRONMENT, PARSE_JSON('null')),
                'OBJECT', IFNULL(OBJECT_CONSTRUCT(*):OBJECT, PARSE_JSON('null')),
                'OWNER', IFNULL(OBJECT_CONSTRUCT(*):OWNER, PARSE_JSON('null')),
                'TITLE', IFNULL(OBJECT_CONSTRUCT(*):TITLE, PARSE_JSON('null')),
                'ALERT_TIME', IFNULL(OBJECT_CONSTRUCT(*):ALERT_TIME, PARSE_JSON('null')),
                'DESCRIPTION', IFNULL(OBJECT_CONSTRUCT(*):DESCRIPTION, PARSE_JSON('null')),
                'EVENT_DATA', IFNULL(OBJECT_CONSTRUCT(*):EVENT_DATA, PARSE_JSON('null')),
                'DETECTOR', IFNULL(OBJECT_CONSTRUCT(*):DETECTOR, PARSE_JSON('null')),
                'SEVERITY', IFNULL(OBJECT_CONSTRUCT(*):SEVERITY, PARSE_JSON('null')),
                'QUERY_ID', IFNULL(OBJECT_CONSTRUCT(*):QUERY_ID, PARSE_JSON('null')),
                'QUERY_NAME', '${query_name}'
            )
          )
        ))
      , data.object_assign(
          OBJECT_CONSTRUCT(*),
          OBJECT_CONSTRUCT('QUERY_NAME', '${query_name}')
        )
    FROM rules.${query_name}
    WHERE IFF(alert_time IS NOT NULL, alert_time > ${FROM_TIME_SQL}, TRUE)
  `)))

return {
  'run_id': RUN_ID,
  'insert_violations': insert_violations
}
