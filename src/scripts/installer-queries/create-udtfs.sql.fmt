USE SCHEMA data;

CREATE FUNCTION IF NOT EXISTS time_slices (n NUMBER, s TIMESTAMP_NTZ, e TIMESTAMP_NTZ)
RETURNS TABLE ( slice_start TIMESTAMP_NTZ, slice_end TIMESTAMP_NTZ )
AS $$
  SELECT DATEADD(sec, DATEDIFF(sec, s, e) * ROW_NUMBER() OVER (ORDER BY SEQ4()) / n, s) AS slice_start
       , DATEADD(sec, DATEDIFF(sec, s, e) * 1 / n, slice_start) AS slice_end
  FROM TABLE(GENERATOR(ROWCOUNT => n))
$$
;

CREATE FUNCTION IF NOT EXISTS time_slices (n NUMBER, s TIMESTAMP_LTZ, e TIMESTAMP_LTZ)
RETURNS TABLE ( slice_start TIMESTAMP_LTZ, slice_end TIMESTAMP_LTZ )
AS $$
  SELECT DATEADD(sec, DATEDIFF(sec, s, e) * ROW_NUMBER() OVER (ORDER BY SEQ4()) / n, s) AS slice_start
       , DATEADD(sec, DATEDIFF(sec, s, e) * 1 / n, slice_start) AS slice_end
  FROM TABLE(GENERATOR(ROWCOUNT => n))
$$
;

CREATE FUNCTION IF NOT EXISTS time_slices_before_t (num_slices NUMBER, seconds_in_slice NUMBER, t TIMESTAMP_NTZ)
RETURNS TABLE ( slice_start TIMESTAMP_NTZ, slice_end TIMESTAMP_NTZ )
AS $$
SELECT slice_start
     , slice_end
FROM TABLE(
  time_slices(
    num_slices,
    DATEADD(sec, -seconds_in_slice * num_slices, t),
    t
  )
)
$$
;

CREATE FUNCTION IF NOT EXISTS time_slices_before_t (num_slices NUMBER, seconds_in_slice NUMBER, t TIMESTAMP_LTZ)
RETURNS TABLE ( slice_start TIMESTAMP_LTZ, slice_end TIMESTAMP_LTZ )
AS $$
SELECT slice_start
     , slice_end
FROM TABLE(
  time_slices(
    num_slices,
    DATEADD(sec, -seconds_in_slice * num_slices, t),
    t
  )
)
$$
;

CREATE FUNCTION IF NOT EXISTS time_slices_before_t (num_slices NUMBER, seconds_in_slice NUMBER)
RETURNS TABLE ( slice_start TIMESTAMP, slice_end TIMESTAMP )
AS $$
SELECT slice_start
     , slice_end
FROM TABLE(
  time_slices(
    num_slices,
    DATEADD(sec, -seconds_in_slice * num_slices, CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP)::TIMESTAMP),
    CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP)::TIMESTAMP
  )
)
$$
;

CREATE FUNCTION IF NOT EXISTS object_assign (o1 VARIANT, o2 VARIANT)
RETURNS VARIANT
LANGUAGE javascript
AS $$
  return Object.assign(O1, O2)
$$
;


CREATE OR REPLACE FUNCTION urlencode("obj" VARIANT) RETURNS STRING
LANGUAGE JAVASCRIPT
AS $$
var ret = []
for (var p in obj)
if (obj.hasOwnProperty(p)) {
  var v = obj[p]
  v = v instanceof Date ? v.toISOString() : v
  ret.push(encodeURIComponent(p) + "=" + encodeURIComponent(v))
}
return ret.join("&")
$$
;
