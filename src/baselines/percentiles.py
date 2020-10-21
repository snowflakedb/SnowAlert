"""Basic Baseline

Compare the count of events in a window to percentiles of counts in prior windows.
"""

from typing import List, Optional

from runners.helpers import db
from runners.helpers.dbconfig import WAREHOUSE

OPTIONS = [
    {
        'name': 'history_size_days',
        'title': "Window Size",
        'prompt': "Plurality of Percentile Periods, in days (e.g. 90)",
        'type': 'text',
        'default': "90",
        'required': True,
    },
    {
        'name': 'sparcity_reduction',
        'title': "Sparcity Reduction (optional)",
        'prompt': "How to reduce the sparcity of event data",
        'type': 'select',
        'options': [
            {'value': 'none', 'label': "No Reduction"},
            {'value': 'crop', 'label': "Use top 100"},
            {'value': 'drop_zeros', 'label': "Drop Zeros"},
            {'value': 'window', 'label': "Use 24h sliding window"},
        ],
        'default': 'none',
        'required': True,
    },
    {
        'type': 'text',
        'name': 'groups',
        'title': "Group-by Columns (optional)",
        'prompt': "Columns by which to group data before baselining it",
        'placeholder': "username,PARSE_JSON(details):geo.country",
    },
]

COUNT_HOURLY_TABLE_SQL = """
CREATE OR REPLACE TABLE {base_table}_counts (
  slice_start timestamp_ltz,
  slice_end timestamp_ltz,
  groups variant,
  n number
)
;
"""

COUNT_HOURLY_MERGE_SQL = """
MERGE INTO {base_table}_counts stored
USING (
  -- calculate sums
  SELECT COUNT(*) n
       , slice_start
       , slice_end
       , groups
  FROM (
    -- make the slices
    SELECT slice_start, slice_end
    FROM TABLE(data.TIME_SLICES_BEFORE_T(
      {days}*24, 60*60, DATE_TRUNC(HOUR, CURRENT_TIMESTAMP)
    ))
  ) t
  JOIN (
    -- calculate sums in those slices
    SELECT {time_column} event_time
         , {groups} AS groups
    FROM {base_table}
  ) c
  ON c.event_time BETWEEN t.slice_start AND t.slice_end
  GROUP BY slice_start, slice_end, groups
) calcd
ON (
  stored.groups = calcd.groups
  AND stored.slice_start = calcd.slice_start
  AND stored.slice_end = calcd.slice_end
)
WHEN NOT MATCHED THEN INSERT (
  slice_start,
  slice_end,
  groups,
  n
)
VALUES (
  slice_start,
  slice_end,
  groups,
  n
)
"""

COUNT_HOURLY_TASK_SQL = f"""
CREATE OR REPLACE TASK {{base_table}}_count
  SCHEDULE='USING CRON 0 * * * * UTC'
  WAREHOUSE={WAREHOUSE}
AS
{COUNT_HOURLY_MERGE_SQL}
"""

BASIC_BASELINE_VIEW = """
CREATE OR REPLACE VIEW {base_table}_pct_baseline AS
SELECT * FROM (
  SELECT slice_start hour
       , groups
       , n
       , APPROX_PERCENTILE(n, .01) OVER (PARTITION BY groups) pct01
       , APPROX_PERCENTILE(n, .05) OVER (PARTITION BY groups) pct05
       , APPROX_PERCENTILE(n, .10) OVER (PARTITION BY groups) pct10
       , APPROX_PERCENTILE(n, .50) OVER (PARTITION BY groups) pct50
       , APPROX_PERCENTILE(n, .90) OVER (PARTITION BY groups) pct90
       , APPROX_PERCENTILE(n, .95) OVER (PARTITION BY groups) pct95
       , APPROX_PERCENTILE(n, .99) OVER (PARTITION BY groups) pct99
  FROM (
    -- zero-filled count table
    SELECT ZEROIFNULL(n) n
         , groups
         , slice_start
         , slice_end
    FROM {base_table}_counts
    RIGHT JOIN (
      -- zero filled matrix of (groups X slices)
      SELECT groups, slice_start, slice_end
      FROM (
        SELECT DISTINCT groups
        FROM {base_table}_counts
      ) g
      CROSS JOIN (
        SELECT slice_start, slice_end
        FROM TABLE(TIME_SLICES_BEFORE_T(
          {days} * 24, 60 * 60, DATE_TRUNC(HOUR, CURRENT_TIMESTAMP)
        ))
      ) t
    )
    USING (groups, slice_start, slice_end)
    ORDER BY n DESC
    {limit} -- optional limit
  )
  WHERE slice_start > DATEADD(HOUR, -24 * {days}, DATE_TRUNC(HOUR, CURRENT_TIMESTAMP))
)
WHERE hour = (
  SELECT MAX(slice_start)
  FROM {base_table}_counts
)
ORDER BY n DESC
"""

BASIC_BASELINE_VIEW_NO_ZEROS = """
CREATE OR REPLACE VIEW {base_table}_pct_baseline COPY GRANTS
AS
SELECT * FROM (
  SELECT slice
       , groups
       , n
       , APPROX_PERCENTILE_ESTIMATE(pct, .01) pct01
       , APPROX_PERCENTILE_ESTIMATE(pct, .05) pct05
       , APPROX_PERCENTILE_ESTIMATE(pct, .10) pct10
       , APPROX_PERCENTILE_ESTIMATE(pct, .50) pct50
       , APPROX_PERCENTILE_ESTIMATE(pct, .90) pct90
       , APPROX_PERCENTILE_ESTIMATE(pct, .95) pct95
       , APPROX_PERCENTILE_ESTIMATE(pct, .99) pct99
  FROM (
    SELECT slice_start slice
         , current_hour
         , ZEROIFNULL(n) n
         , groups
         , pct
    FROM {base_table}_counts
    RIGHT OUTER JOIN (
      SELECT groups
           , DATE_TRUNC(HOUR, CURRENT_TIMESTAMP) current_hour
           , APPROX_PERCENTILE_ACCUMULATE(n) OVER (PARTITION BY GROUPS) pct
      FROM data.{base_table}_counts
      WHERE slice_start BETWEEN DATEADD(HOUR, -24 * {days} - 1, current_hour)
                            AND DATEADD(HOUR, -1, current_hour)
    )
    USING (groups)
    WHERE slice = current_hour
  )
)
"""

BASIC_BASELINE_VIEW_WITH_WINDOW = """
CREATE OR REPLACE VIEW {base_table}_pct_baseline AS
SELECT * FROM (
  SELECT slice_start hour
       , groups
       , n
       , APPROX_PERCENTILE_ESTIMATE(pct, .01) pct01
       , APPROX_PERCENTILE_ESTIMATE(pct, .05) pct05
       , APPROX_PERCENTILE_ESTIMATE(pct, .10) pct10
       , APPROX_PERCENTILE_ESTIMATE(pct, .50) pct50
       , APPROX_PERCENTILE_ESTIMATE(pct, .90) pct90
       , APPROX_PERCENTILE_ESTIMATE(pct, .95) pct95
       , APPROX_PERCENTILE_ESTIMATE(pct, .99) pct99
  FROM (
    SELECT SUM(n) OVER (
             PARTITION BY groups
             ORDER BY slice_start
             ROWS BETWEEN 24 PRECEDING
                      AND 1 PRECEDING
           ) n
         , APPROX_PERCENTILE_ACCUMULATE(n) OVER (PARTITION BY GROUPS) pct
         , slice_start
         , groups
    FROM (
      -- zero-filled count table
      SELECT ZEROIFNULL(n) n
           , groups
           , slice_start
           , slice_end
      FROM {base_table}_counts
      RIGHT JOIN (
        -- zero filled matrix of (groups X slices)
        SELECT groups, slice_start, slice_end
        FROM (
          SELECT DISTINCT groups
          FROM {base_table}_counts
        ) g
        CROSS JOIN (
          SELECT slice_start, slice_end
          FROM TABLE(TIME_SLICES_BEFORE_T(
            {days} * 24, 60 * 60, DATE_TRUNC(HOUR, CURRENT_TIMESTAMP)
          ))
        ) t
      )
      WHERE slice_start > DATEADD(HOUR, -24 * ({days} + 1), DATE_TRUNC(HOUR, CURRENT_TIMESTAMP))
    )
    USING (groups, slice_start, slice_end)
  )
  WHERE slice_start > DATEADD(HOUR, -24 * {days}, DATE_TRUNC(HOUR, CURRENT_TIMESTAMP))
)
WHERE hour = (
  SELECT MAX(slice_start)
  FROM {base_table}_counts
)
ORDER BY n DESC
"""


def generate_baseline_sql(
    base_table: str,
    time_column: str,
    groups: Optional[List[str]],
    days: int = 30,
    sparcity_reduction: Optional[str] = None,
) -> List[str]:
    return [
        COUNT_HOURLY_TABLE_SQL.format(base_table=base_table),
        COUNT_HOURLY_TASK_SQL.format(
            base_table=base_table,
            time_column=time_column,
            groups=db.dict_to_sql({g: g for g in groups or []}, indent=11),
            days=days,
        ),
        f'ALTER TASK {base_table}_count RESUME',
        BASIC_BASELINE_VIEW_NO_ZEROS.format(base_table=base_table, days=days)
        if sparcity_reduction == 'drop_zeros'
        else BASIC_BASELINE_VIEW_WITH_WINDOW
        if sparcity_reduction == 'window'
        else BASIC_BASELINE_VIEW.format(
            base_table=base_table,
            days=days,
            limit=('LIMIT 100' if sparcity_reduction == 'crop' else ''),
        ),
    ]


def create(options):
    base_table_entry = options['base_table_and_timecol']
    if ':' not in base_table_entry:
        raise ValueError("Enter time column in format <table>:<time_column>")
    else:
        base_table, time_column = options['base_table_and_timecol'].split(':', 1)

    groups = list(
        filter(None, [g.strip() for g in options.get('groups', '').split(',')])
    )
    days = int(options.get('history_size_days', '30'))
    return [
        next(db.fetch(sql, fix_errors=False), {}).get('status')
        for sql in generate_baseline_sql(base_table, time_column, groups, days)
    ]
