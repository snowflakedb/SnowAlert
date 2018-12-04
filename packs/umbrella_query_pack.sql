CREATE OR REPLACE VIEW snowalert.rules.recurring_c2_activity_ratio_alert_query COPY GRANTS AS
  SELECT 'Recurring C2 Communication Allowed by Umbrella' AS title
       , array_construct('umbrella') AS sources
       , hostname AS object
       , 'SnowAlert' AS environment
       , event_timestamp AS event_time
       , CURRENT_TIMESTAMP() AS alert_time
       , 'Cisco Umbrella is reporting recurring unblocked C2 activity at ' || hostname AS description
       , hostname AS actor
       , 'DNS Lookup' AS action
       , 'SnowAlert' AS detector
       , OBJECT_CONSTRUCT(*) AS event_data
       , 'High' AS severity
       , 'recurring_c2_activity_ratio_alert_query' AS query_name
       , 'de27bf5e-ce9a-4906-bb5f-746278806993' AS query_id
  FROM (
    SELECT MAX(slice_end) AS event_timestamp
         , SUM(IFF(bin_count=0, 0, 1)) / COUNT(*) AS c2_activity_ratio
         , hostname
    FROM (
      SELECT slice.slice_start, slice.slice_end, slice.host AS hostname, COUNT(uuid) AS bin_count
      FROM (
        SELECT uuid, timestamp, host
        FROM snowalert.data.umbrella AS event_data
        WHERE event_data.categories = 'Malware' AND event_data.action = 'Allowed'
      ) AS event_data
      FULL JOIN snowalert.data.latest_umbrella_slices AS slice
      ON event_data.timestamp BETWEEN slice.slice_start AND slice.slice_end AND event_data.host=slice.host
      GROUP BY slice.slice_start, slice.slice_end, slice.host
      ORDER BY slice.slice_start DESC
    )
    GROUP BY hostname
  )
  WHERE c2_activity_ratio > 0.3
;
