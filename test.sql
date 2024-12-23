-- 每年开展过入院教育的医院
SELECT year(start_time) AS year
     , *
FROM dwd_hainan_hospital_info.dwd_teaching_activity_detail_df
WHERE year(start_time) >= 2019
    AND activity_type_id = 14 AND activity_type_name = '入院教育';
