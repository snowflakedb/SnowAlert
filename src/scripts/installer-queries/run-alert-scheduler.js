// args

var WAREHOUSE

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

function unindent(s) {
  const min_indent = Math.min(...[...s.matchAll('\n *')].map(x => x[0].length));
  return s.replace('\n' + ' '.repeat(min_indent), '\n')
}

// logic

FIND_VIEWS = String.raw`-- find views with schedules
SELECT table_name AS "rule_name"
     , IFF(
         CONTAINS(view_definition, ' AS schedule'),
         REGEXP_REPLACE(view_definition, '[\\s\\S]*\'([^\']*)\' AS schedule[\\s\\S]*', '\\1'),
         NULL
       ) AS "schedule"
     , IFF(
         CONTAINS(view_definition, ' AS lookback'),
         REGEXP_REPLACE(view_definition, '[\\s\\S]*\'([^\']*)\' AS lookback[\\s\\S]*', '\\1'),
         NULL
       ) AS "lookback"
FROM information_schema.views
WHERE table_schema='RULES'
  AND "schedule" IS NOT NULL
`

return {
  'handled': exec(FIND_VIEWS).map(v => ({
      'run_alert_query': exec(unindent(`-- create alert query run task
        CREATE OR REPLACE TASK RUN_ALERT_QUERY_${v.rule_name}
        WAREHOUSE=${WAREHOUSE}
        SCHEDULE='${v.schedule}'
        AS
        CALL results.alert_queries_runner('${v.rule_name}', '${v.schedule}')
      `))[0]['status'],
      'run_alert_suppressions': exec(unindent(`-- create alert suppression run task
        CREATE OR REPLACE TASK RUN_ALERT_SUPPRESSIONS_ON_${v.rule_name}
        WAREHOUSE=${WAREHOUSE}
        AFTER RUN_ALERT_QUERY_${v.rule_name}
        AS
        CALL results.alert_suppressions_runner('${v.rule_name}')
      `))[0]['status'],
      'resume_alert_suppressions': exec(unindent(`
        ALTER TASK RUN_ALERT_SUPPRESSIONS_ON_${v.rule_name} RESUME
      `))[0]['status'],
      'resume_alert_query': exec(unindent(`
        ALTER TASK RUN_ALERT_QUERY_${v.rule_name} RESUME
      `))[0]['status'],
    }))
}
