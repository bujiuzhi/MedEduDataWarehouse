SELECT a.hospital_id
     , a.hospital_name
     , a.year
     , a.activity_count
     , b.student_count
     , c.evaluation_count
     , c.total_count
     , c.evaluation_rate
FROM dws_hainan_hospital_info.dws_teaching_activity_count_by_hospital_year a
         LEFT JOIN dws_hainan_hospital_info.dws_teaching_activity_student_count_by_hospital_year b
                   ON a.hospital_id = b.hospital_id
                       AND a.year = b.year
         LEFT JOIN dws_hainan_hospital_info.dws_teaching_activity_evaluation_count_by_hospital_year c
                   ON a.hospital_id = c.hospital_id
                       AND a.year = c.year;