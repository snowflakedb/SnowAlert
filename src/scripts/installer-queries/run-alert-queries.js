// args

var QUERY_NAME, FROM_TIME_SQL, TO_TIME_SQL, OFFSET;

OFFSET = OFFSET || '90 minutes';
FROM_TIME_SQL = FROM_TIME_SQL || `CURRENT_TIMESTAMP - INTERVAL '${OFFSET}'`;
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

const RUN_ID = Math.random().toString(36).substring(2).toUpperCase();
const TEMP_NEW_ALERTS_TBL = `results.RUN_${RUN_ID}_${QUERY_NAME}`;

const CREATE_ALERTS_SQL = `CREATE TEMP TABLE ${TEMP_NEW_ALERTS_TBL} AS
SELECT OBJECT_CONSTRUCT(
         'ALERT_ID', UUID_STRING(),
         'QUERY_NAME', '${QUERY_NAME}',
         'QUERY_ID', IFNULL(QUERY_ID::VARIANT, PARSE_JSON('null')),
         'ENVIRONMENT', IFNULL(ENVIRONMENT::VARIANT, PARSE_JSON('null')),
         'SOURCES', IFNULL(SOURCES::VARIANT, PARSE_JSON('null')),
         'ACTOR', IFNULL(ACTOR::VARIANT, PARSE_JSON('null')),
         'OBJECT', IFNULL(OBJECT::VARIANT, PARSE_JSON('null')),
         'ACTION', IFNULL(ACTION::VARIANT, PARSE_JSON('null')),
         'TITLE', IFNULL(TITLE::VARIANT, PARSE_JSON('null')),
         'EVENT_TIME', IFNULL(EVENT_TIME::VARIANT, PARSE_JSON('null')),
         'ALERT_TIME', IFNULL(ALERT_TIME::VARIANT, PARSE_JSON('null')),
         'DESCRIPTION', IFNULL(DESCRIPTION::VARIANT, PARSE_JSON('null')),
         'DETECTOR', IFNULL(DETECTOR::VARIANT, PARSE_JSON('null')),
         'EVENT_DATA', IFNULL(EVENT_DATA::VARIANT, PARSE_JSON('null')),
         'SEVERITY', IFNULL(SEVERITY::VARIANT, PARSE_JSON('null')),
         'HANDLERS', IFNULL(OBJECT_CONSTRUCT(*):HANDLERS::VARIANT, PARSE_JSON('null'))
       ) AS alert
     , alert_time
     , event_time
     , 1 AS counter
FROM rules.${QUERY_NAME}
WHERE event_time BETWEEN ${FROM_TIME_SQL} AND ${TO_TIME_SQL}
;`;

const ALERT_RUN_RESULT_COUNT = `SELECT COUNT(*) n FROM ${TEMP_NEW_ALERTS_TBL}`

const MERGE_ALERTS_SQL = `
MERGE INTO results.alerts AS alerts
USING (

  SELECT ANY_VALUE(alert) AS alert
       , SUM(counter) AS counter
       , MIN(alert_time) AS alert_time
       , MIN(event_time) AS event_time

  FROM ${TEMP_NEW_ALERTS_TBL}
  GROUP BY
    alert:OBJECT,
    alert:DESCRIPTION,
    alert:EVENT_DATA,
    alert:TITLE

) AS new_alerts

ON (
  alerts.alert:EVENT_TIME > ${FROM_TIME_SQL}
  AND alerts.alert:OBJECT = new_alerts.alert:OBJECT
  AND alerts.alert:DESCRIPTION = new_alerts.alert:DESCRIPTION
  AND alerts.alert:EVENT_DATA = new_alerts.alert:EVENT_DATA
  AND alerts.alert:TITLE = new_alerts.alert:TITLE
)

WHEN MATCHED
  THEN UPDATE
  SET counter = alerts.counter + new_alerts.counter

WHEN NOT MATCHED
  THEN INSERT (
    alert,
    alert_id,
    counter,
    alert_time,
    event_time
  )
  VALUES (
    new_alerts.alert,
    new_alerts.alert['ALERT_ID'],
    new_alerts.counter,
    new_alerts.alert_time,
    new_alerts.event_time
  )
;`;

const create_alerts_result = exec(CREATE_ALERTS_SQL)[0];
const merge_alerts_result = (
  exec(ALERT_RUN_RESULT_COUNT)[0]['N'] > 0
    ? exec(MERGE_ALERTS_SQL)[0]
    : {'number of rows updated': 0, 'number of rows inserted': 0}
)

return {
  'run_id': RUN_ID,
  'new_alerts_tbl': TEMP_NEW_ALERTS_TBL,
  'create_alerts_result': create_alerts_result,
  'merge_alerts_result': merge_alerts_result,
}
