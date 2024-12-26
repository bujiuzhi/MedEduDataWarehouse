
SELECT grouped.hospital_id
     , grouped.hospital_name
     , grouped.month
     , sum(CASE WHEN grouped.activity_name = grouped.activity_type_name THEN grouped.activity_count ELSE 0 END) AS activity_name_type_same_count
     , sum(CASE WHEN grouped.activity_count > 1 THEN grouped.activity_count END)                                AS activity_name_type_same_total
FROM (SELECT d.hospital_id
           , d.hospital_name
           , date_format(d.start_time, 'yyyy-MM') AS month
           , d.activity_name
           , d.activity_type_name
           , count(*)                             AS activity_count
      FROM dwd_hainan_hospital_info.dwd_teaching_activity_detail_df d
      GROUP BY d.hospital_id, d.hospital_name, date_format(d.start_time, 'yyyy-MM'), d.activity_name, d.activity_type_name) AS grouped
GROUP BY grouped.hospital_id, grouped.hospital_name, grouped.month;
