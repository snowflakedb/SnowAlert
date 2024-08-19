// params

RESOURCES = RESOURCES || [
  'assessments',
  'audits',
  'issues',
  'requests',
  'controls',
  'people',
  'objectives',
  'programs',
  'systems',
  'risks',
]

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

function indent(n, s) {
  return s.replace(/\n/g, '\n' + ' '.repeat(4))
}

// code

const FIRST_PAGES_SQL = indent(4,
  RESOURCES
    .map(r => `SELECT '/api/v2/${r}' resource`)
    .join('\nUNION ALL\n')
)

// ZenGRC can have a lot of pages that take longer than API Gateway allows to
// iterate through, so we can't use the pagination in the external function

const GET_FIRST_PAGES_SQL = `
INSERT INTO zengrc_connection
SELECT
  CURRENT_TIMESTAMP,
  resource,
  result,
  result:links.next.href
FROM (
  SELECT
    resource,
    zengrc_snowflake_api(resource) result
  FROM (
    ${FIRST_PAGES_SQL}
  )
)
`

const GET_NEXT_PAGES_SQL = `
INSERT INTO zengrc_connection
SELECT
  CURRENT_TIMESTAMP,
  next_href,
  result,
  result:links.next.href
FROM (
  SELECT
    next_href,
    zengrc_snowflake_api(next_href) result
  FROM zengrc_connection
  WHERE next_href NOT IN (
    SELECT href
    FROM zengrc_connection
  )
)
`;

var page_depth = RESOURCES.length
var page_count = exec(GET_FIRST_PAGES_SQL)[0]['number of rows inserted']
var res = null

do {
  res = exec(GET_NEXT_PAGES_SQL)[0]
  page_depth += 1
  page_count += res['number of rows inserted']
} while (
  res['number of rows inserted'] > 0
)

return {
  // resource with largest page count
  'page_depth': page_depth,

  // number of pages total
  'page_count': page_count,
}
