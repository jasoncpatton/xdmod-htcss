UPDATE
  modw_htcss.htcss_raw_logs AS raw
SET
  raw.start_day_id = YEAR(raw.start_date) * 100000 + DAYOFYEAR(raw.start_date),
  raw.end_day_id = YEAR(raw.end_date) * 100000 + DAYOFYEAR(raw.end_date),
  raw.aggregation_unit_id = CASE
    WHEN raw.aggregation_unit = 'day' THEN 1
    WHEN raw.aggregation_unit = 'month' THEN 2
    WHEN raw.aggregation_unit = 'quarter' THEN 3
    WHEN raw.aggregation_unit = 'year' THEN 4
  END
WHERE
  raw.last_modified >= ${LAST_MODIFIED}
//

UPDATE
  modw_htcss.htcss_raw_logs AS raw
JOIN
  mod_shredder.staging_union_user_pi as t1 on raw.project_pi = t1.union_user_pi_name
JOIN
  mod_shredder.staging_union_user_pi as t2 on raw.person = t2.union_user_pi_name
SET
  raw.person_id = t2.union_user_pi_id,
  raw.project_pi_person_id = t1.union_user_pi_id
WHERE
  raw.last_modified >= ${LAST_MODIFIED};
