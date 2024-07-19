UPDATE
  modw_htcss.htcss_raw_logs as raw
SET
  start_day_id = YEAR(raw.start_date) * 100000 + DAYOFYEAR(raw.start_date),
  end_day_id = YEAR(raw.end_date) * 100000 + DAYOFYEAR(raw.end_date)
