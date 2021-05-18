#!/usr/bin/env python

import json
import uuid

from .helpers import db, log

import snowflake.connector

CORRELATION_PERIOD = -60


CORRELATE = f"""
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
    AND d.alert_time > CURRENT_TIMESTAMP - INTERVAL '2 hours'
  QUALIFY 1=ROW_NUMBER() OVER (
      PARTITION BY d.id  -- one update per destination id
      ORDER BY -- most recent wins
        p.alert_time DESC, p.id DESC
    )
) src
ON (
  dst.alert_id = src.alert_id_to_update
  AND (
    dst.correlation_id IS NULL
    OR dst.correlation_id != src.correlation_id
  )
)
WHEN MATCHED THEN UPDATE SET
  correlation_id = src.correlation_id
;
"""

def correlate():
  log.debug('correlating')
  result = next(db.fetch(CORRELATE))
  log.debug(f'result: {result}')
  return result.get('number of rows updated')


def main():
  while correlate() != 0:
      pass
