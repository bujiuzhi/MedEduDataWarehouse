/*
    dws层报表数据（中文列名）
    目前已有表：
    教学活动入院教育覆盖率按年统计 dws_teaching_activity_hospital_education_coverage_by_year
    教学活动入专业基地教育覆盖率按年按医院统计表  dws_teaching_activity_major_education_coverage_by_year_hospital
    教学活动开展数量按医院按年统计表 dws_teaching_activity_count_by_hospital_year
    教学活动开展数量按医院按专业按月统计表 dws_teaching_activity_count_by_hospital_major_month
    教学活动临床小讲课开展数量按医院按专业按年统计表    dws_teaching_activity_clinical_lecture_by_hospital_major_year
    教学活动门诊教学开展数量按医院按专业按年统计表    dws_teaching_activity_outpatient_by_hospital_major_year
    教学活动评价次数统计按医院按年统计表 dws_teaching_activity_evaluation_count_by_hospital_year
    教学活动开展次数按类型按天统计表 dws_teaching_activity_count_by_type_day
*/

/*
    教学活动入院教育覆盖率按年统计表 dws_teaching_activity_hospital_education_coverage_by_year
*/

SELECT year                     AS `统计年份`
     , hospital_admitted_count  AS `该年实际招录学生的医院数`
     , hospital_education_count AS `该年开展过入院教育的医院数`
     , coverage_education       AS `入院教育覆盖率`
FROM dws_hainan_hospital_info.dws_teaching_activity_hospital_education_coverage_by_year;


/*
    教学活动入专业基地教育覆盖率按年按医院统计表 dws_teaching_activity_major_education_coverage_by_year_hospital
*/
SELECT year                  AS `统计年份`
     , hospital_id           AS `医院ID`
     , hospital_name         AS `医院名称`
     , major_admitted_count  AS `该年该医院实际招录的专业数`
     , major_education_count AS `该年该医院开展过入专业基地教育的专业数`
     , coverage_education    AS `入专业基地教育覆盖率`
FROM dws_hainan_hospital_info.dws_teaching_activity_major_education_coverage_by_year_hospital;


/*
    教学活动开展数量按医院按年统计表 dws_teaching_activity_count_by_hospital_year
*/
SELECT hospital_id    AS `医院ID`
     , hospital_name  AS `医院名称`
     , year           AS `统计年份`
     , activity_count AS `该年该医院该专业开展的教学活动数量`
FROM dws_hainan_hospital_info.dws_teaching_activity_count_by_hospital_year;


/*
    教学活动开展数量按医院按专业按月统计表 dws_teaching_activity_count_by_hospital_major_month
*/
SELECT hospital_id    AS `医院ID`
     , hospital_name  AS `医院名称`
     , major_id       AS `专业ID`
     , major_name     AS `专业名称`
     , month          AS `统计月份`
     , activity_count AS `该月该专业开展的教学活动数量`
FROM dws_hainan_hospital_info.dws_teaching_activity_count_by_hospital_major_month;


/*
    教学活动临床小讲课开展数量按医院按专业按年统计表 dws_teaching_activity_clinical_lecture_by_hospital_major_year
*/
SELECT hospital_id    AS `医院ID`
     , hospital_name  AS `医院名称`
     , major_id       AS `专业ID`
     , major_name     AS `专业名称`
     , year           AS `统计年份`
     , activity_count AS `该年该专业在该医院开展的临床小讲课数量`
FROM dws_hainan_hospital_info.dws_teaching_activity_clinical_lecture_by_hospital_major_year;


/*
    教学活动门诊教学开展数量按医院按专业按年统计表 dws_teaching_activity_outpatient_by_hospital_major_year
*/
SELECT hospital_id    AS `医院ID`
     , hospital_name  AS `医院名称`
     , major_id       AS `专业ID`
     , major_name     AS `专业名称`
     , year           AS `统计年份`
     , activity_count AS `该年该专业在该医院开展的门诊教学数量`
FROM dws_hainan_hospital_info.dws_teaching_activity_outpatient_by_hospital_major_year;


/*
    教学活动评价次数统计按医院按年统计表 dws_teaching_activity_evaluation_count_by_hospital_year
*/
SELECT hospital_id      AS `医院ID`
     , hospital_name    AS `医院名称`
     , year             AS `统计年份`
     , evaluation_count AS `实际评价总数`
     , total_count      AS `应评价总数`
     , evaluation_rate  AS `评价回收率`
FROM dws_hainan_hospital_info.dws_teaching_activity_evaluation_count_by_hospital_year;


/*
    教学活动开展次数按类型按天统计表 dws_teaching_activity_count_by_type_day
*/
SELECT activity_type_id   AS `教学活动类型ID`
     , activity_type_name AS `教学活动类型名称`
     , day                AS `统计日期`
     , activity_count     AS `该天该类型的教学活动数量`
FROM dws_hainan_hospital_info.dws_teaching_activity_count_by_type_day;
