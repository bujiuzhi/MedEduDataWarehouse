SELECT *
FROM dwd_hainan_hospital_info.dwd_teaching_activity_detail_df acd
         LEFT JOIN dwd_hainan_hospital_info.dwd_teaching_activity_student_sign_wide_df ss
                   ON acd.activity_id = ss.activity_id
