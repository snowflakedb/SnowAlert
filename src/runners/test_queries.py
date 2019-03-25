TEST_1_ALERT = """
CREATE OR REPLACE VIEW rules.test1_alert_query COPY GRANTS
  COMMENT = 'Test 1 Alert Query; should group with Test 3
  @tags test-tag'
AS
SELECT OBJECT_CONSTRUCT('account', 'account_test', 'cloud', 'cloud_test') AS ENVIRONMENT
    , ARRAY_CONSTRUCT('obj1', 'obj2') AS OBJECT
    , 'test1_alert_query' AS TITLE
    , 'This is a test alert query; this should be grouped with Test 3' AS DESCRIPTION
    , 'SnowAlert' AS DETECTOR
    , 'test_actor' AS ACTOR
    , 'test action 1' AS ACTION
    , 'test_1_query' AS QUERY_ID
    , 'TEST1_ALERT_QUERY' AS QUERY_NAME
    , 'low' AS SEVERITY
    , ARRAY_CONSTRUCT('source') AS SOURCES
    , OBJECT_CONSTRUCT('data', 'test data') AS EVENT_DATA
    , current_timestamp() as EVENT_TIME
    , current_timestamp() as ALERT_TIME
FROM (select 1 as test_data)
WHERE 1=1
    AND test_data=1
"""

TEST_1_ALERT_GRANT = 'GRANT SELECT ON rules.test1_alert_query to role snowalert_testing'

TEST_2_ALERT = """
CREATE OR REPLACE VIEW rules.test2_alert_query COPY GRANTS
  COMMENT = 'Test 2 Alert Query; should get suppressed
  @tags test-tag'
AS
SELECT OBJECT_CONSTRUCT('account', 'account_test', 'cloud', 'cloud_test') AS ENVIRONMENT
    , ARRAY_CONSTRUCT('obj1', 'obj2') AS OBJECT
    , 'test2_alert_query' AS TITLE
    , 'This query should get suppressed' AS DESCRIPTION
    , 'SnowAlert' AS DETECTOR
    , 'test_actor' AS ACTOR
    , 'test action 2' AS ACTION
    , 'test_2_query' AS QUERY_ID
    , 'test2_alert_query' AS QUERY_NAME
    , 'low' AS SEVERITY
    , ARRAY_CONSTRUCT('source') AS SOURCES
    , OBJECT_CONSTRUCT('data', 'test 2 data') AS EVENT_DATA
    , current_timestamp() as EVENT_TIME
    , current_timestamp() as ALERT_TIME
FROM (select 1 as test_data)
WHERE 1=1
    AND test_data=1
"""

TEST_2_ALERT_GRANT = 'GRANT SELECT ON rules.test2_alert_query to role snowalert_testing'


TEST_2_SUPPRESSION = """
CREATE OR REPLACE VIEW rules.test2_alert_suppression COPY GRANTS
    COMMENT = 'test suppression; should suppress test 2'
AS
SELECT ALERT FROM results.alerts
where
    SUPPRESSED IS NULL
    AND ALERT:ACTOR = 'test_actor'
    AND ALERT:ACTION = 'test action 2'
"""

TEST_2_SUPPRESSION_GRANT = 'GRANT SELECT ON rules.test2_alert_suppression to role snowalert_testing'

TEST_3_ALERT = """
CREATE OR REPLACE VIEW rules.test3_alert_query COPY GRANTS
  COMMENT = 'Test 3 Alert Query; should group with Test 1
  @tags test-tag'
AS
SELECT OBJECT_CONSTRUCT('account', 'account_test', 'cloud', 'cloud_test') AS ENVIRONMENT
    , ARRAY_CONSTRUCT('obj1', 'obj2') AS OBJECT
    , 'test3_alert_query' AS TITLE
    , 'This is a third test alert query; this should be grouped with Test 1' AS DESCRIPTION
    , 'SnowAlert' AS DETECTOR
    , 'test_actor' AS ACTOR
    , 'test action 3' AS ACTION
    , 'test_3_query' AS QUERY_ID
    , 'test3_alert_query' AS QUERY_NAME
    , 'low' AS SEVERITY
    , ARRAY_CONSTRUCT('source') AS SOURCES
    , OBJECT_CONSTRUCT('data', 'test 3 data') AS EVENT_DATA
    , current_timestamp() as EVENT_TIME
    , current_timestamp() as ALERT_TIME
FROM (select 1 as test_data)
WHERE 1=1
    AND test_data=1
"""

TEST_3_ALERT_GRANT = 'GRANT SELECT on rules.test3_alert_query to role snowalert_testing'

TEST_4_TICKET_QUERY = """
SELECT TICKET FROM results.alerts
WHERE ALERT:QUERY_ID = 'test_1_query'
"""
