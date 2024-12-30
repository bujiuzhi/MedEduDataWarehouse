SELECT DISTINCT activity_type_id, activity_type_name
FROM dwd_hainan_hospital_info.dwd_teaching_activity_detail_df;


-- 2,技能培训
-- 3,教学阅片
-- 4,教学查房
-- 8,教学病例讨论
-- 9,临床小讲课
-- 10,入轮转科室教育
-- 13,临床文献研读会
-- 14,入院教育
-- 15,入专业基地教育
-- 16,手术操作指导
-- 17,影像诊断报告书写
-- 18,临床操作技能床旁教学
-- 22,门诊教学
-- 23,晨间报告
-- 99,其他

INSERT INTO dim_hainan_hospital_info.dim_teacher_activity_count_standard_by_hospital_major
VALUES (1, 1, '内科', 1, 2, 2, 2, NULL, NULL, NULL)
     , (1, '内科', 10, '入轮转科室教育', 1)
     , (1, '内科', 9, '临床小讲课', 2)
     , (1, '内科', 8, '教学病例讨论', 2)
     , (1, '内科', 3, '教学阅片', 2)



