/*===================================================================
dws层数据整理：
目前已有表：
    教学活动入院教育覆盖率按年统计 dws_teaching_activity_hospital_education_coverage_by_year
    教学活动入专业基地教育覆盖率按年按医院统计表  dws_teaching_activity_major_education_coverage_by_year_hospital
    教学活动开展数量按医院按年统计表 dws_teaching_activity_count_by_hospital_year
    教学活动开展数量按医院按专业按月统计表 dws_teaching_activity_count_by_hospital_major_month
    教学活动临床小讲课开展数量按医院按专业按年统计表    dws_teaching_activity_clinical_lecture_by_hospital_major_year
    教学活动门诊教学开展数量按医院按专业按年统计表    dws_teaching_activity_outpatient_by_hospital_major_year
    教学活动评价次数统计按医院按年统计表 dws_teaching_activity_evaluation_count_by_hospital_year
    教学活动开展次数按类型按天统计表 dws_teaching_activity_count_by_type_day
====================================================================*/

/*
    设置Hive执行引擎和MapReduce相关参数
    SET hive.execution.engine=mr;
    SET mapreduce.task.io.sort.mb = 1000;
*/

-- 确保目标数据库存在
CREATE DATABASE IF NOT EXISTS dws_hainan_hospital_info;

/*
    教学活动入院教育覆盖率按年统计 dws_teaching_activity_hospital_education_coverage_by_year
*/

-- 教学活动入院教育覆盖率按年统计表：删除旧的（如果存在）
DROP TABLE IF EXISTS dws_hainan_hospital_info.dws_teaching_activity_hospital_education_coverage_by_year;

-- 教学活动入院教育覆盖率按年统计表：创建
CREATE TABLE dws_hainan_hospital_info.dws_teaching_activity_hospital_education_coverage_by_year
(
    year                     INT COMMENT '统计年份',
    hospital_admitted_count  INT COMMENT '该年实际招录学生的医院数',
    hospital_education_count INT COMMENT '该年开展过入院教育的医院数',
    coverage_education       STRING COMMENT '入院教育覆盖率'
) COMMENT '教学活动入院教育覆盖率按年统计表';

-- 教学活动入院教育覆盖率按年统计表：清空数据
TRUNCATE TABLE dws_hainan_hospital_info.dws_teaching_activity_hospital_education_coverage_by_year;

-- 教学活动入院教育覆盖率按年统计表：插入数据
INSERT INTO dws_hainan_hospital_info.dws_teaching_activity_hospital_education_coverage_by_year
SELECT a.year
     , a.hospital_admitted_count
     , coalesce(b.hospital_education_count, 0) AS hospital_education_count
     , concat(round(coalesce(b.hospital_education_count, 0) * 100 / a.hospital_admitted_count, 2),
              '%')                             AS coverage_education
FROM (
         -- 分母：每年实际招录学生的医院数
         SELECT admission_year                   AS year
              , count(DISTINCT training_base_id) AS hospital_admitted_count
         FROM dwd_hainan_hospital_info.dwd_hainan_admission_data_view_students
         WHERE admission_year >= 2019
         GROUP BY admission_year) a
         LEFT JOIN
     (
         -- 分子：每年开展过入院教育的医院数
         SELECT year(start_time)            AS year
              , count(DISTINCT hospital_id) AS hospital_education_count
         FROM dwd_hainan_hospital_info.dwd_teaching_activity_detail_df
         WHERE year(start_time) >= 2019
           AND activity_type_id = 14
           AND activity_type_name = '入院教育'
         GROUP BY year(start_time)) b
     ON a.year = b.year;

/*
    教学活动入专业基地教育覆盖率按年按医院统计表  dws_teaching_activity_major_education_coverage_by_year_hospital
*/

-- 教学活动入专业基地教育覆盖率按年按医院统计表：删除旧的（如果存在）
DROP TABLE IF EXISTS dws_hainan_hospital_info.dws_teaching_activity_major_education_coverage_by_year_hospital;

-- 教学活动入专业基地教育覆盖率按年按医院统计表：创建
CREATE TABLE dws_hainan_hospital_info.dws_teaching_activity_major_education_coverage_by_year_hospital
(
    year                  INT COMMENT '统计年份',
    hospital_id           INT COMMENT '医院ID',
    hospital_name         STRING COMMENT '医院名称',
    major_admitted_count  INT COMMENT '该年该医院实际招录的专业数',
    major_education_count INT COMMENT '该年该医院开展过入专业基地教育的专业数',
    coverage_education    STRING COMMENT '入专业基地教育覆盖率'
) COMMENT '教学活动入专业基地教育覆盖率按年按医院统计表';

-- 教学活动入专业基地教育覆盖率按年按医院统计表：清空数据
TRUNCATE TABLE dws_hainan_hospital_info.dws_teaching_activity_major_education_coverage_by_year_hospital;

-- 教学活动入专业基地教育覆盖率按年按医院统计表：插入数据
INSERT INTO dws_hainan_hospital_info.dws_teaching_activity_major_education_coverage_by_year_hospital
SELECT a.year
     , a.hospital_id
     , a.hospital_name
     , a.major_admitted_count
     , coalesce(b.major_education_count, 0) AS major_education_count
     , concat(round(coalesce(b.major_education_count, 0) * 100 / a.major_admitted_count, 2),
              '%')                          AS coverage_education
FROM (
         -- 分母：每年每家医院实际招录的专业数
         SELECT admission_year                    AS year
              , training_base_id                  AS hospital_id
              , training_base                     AS hospital_name
              , count(DISTINCT specialty_base_id) AS major_admitted_count
         FROM dwd_hainan_hospital_info.dwd_hainan_admission_data_view_students
         WHERE admission_year >= 2019
         GROUP BY admission_year, training_base_id, training_base) a
         LEFT JOIN
     (
         -- 分子：每年每家医院开展过入专业基地教育的专业数
         SELECT year(start_time)         AS year
              , hospital_id
              , hospital_name
              , count(DISTINCT major_id) AS major_education_count
         FROM dwd_hainan_hospital_info.dwd_teaching_activity_detail_df
         WHERE year(start_time) >= 2019
           AND activity_type_id = 15
           AND activity_type_name = '入专业基地教育'
         GROUP BY year(start_time), hospital_id, hospital_name) b
     ON a.year = b.year
         AND a.hospital_id = b.hospital_id
         AND a.hospital_name = b.hospital_name;


/*
    教学活动开展数量按医院按年统计表 dws_teaching_activity_count_by_hospital_year
*/

-- 教学活动开展数量按医院按年统计表：删除旧的（如果存在）
DROP TABLE IF EXISTS dws_hainan_hospital_info.dws_teaching_activity_count_by_hospital_year;

-- 教学活动开展数量按医院按年统计表：创建
CREATE TABLE dws_hainan_hospital_info.dws_teaching_activity_count_by_hospital_year
(
    hospital_id    INT COMMENT '医院ID',
    hospital_name  STRING COMMENT '医院名称',
    year           INT COMMENT '统计年份',
    activity_count INT COMMENT '该年该医院该专业开展的教学活动数量'
) COMMENT '教学活动开展数量按医院按年统计表';

-- 教学活动开展数量按医院按年统计表：清空数据
TRUNCATE TABLE dws_hainan_hospital_info.dws_teaching_activity_count_by_hospital_year;

-- 教学活动开展数量按医院按年统计表：插入数据
INSERT INTO dws_hainan_hospital_info.dws_teaching_activity_count_by_hospital_year
SELECT hospital_id
     , hospital_name
     , year(start_time) AS year
     , count(*)         AS activity_count
FROM dwd_hainan_hospital_info.dwd_teaching_activity_detail_df
WHERE year(start_time) >= 2019
GROUP BY hospital_id, hospital_name, year(start_time);

/*
    教学活动开展数量按医院按专业按月统计表 dws_teaching_activity_count_by_hospital_major_month
*/

-- 教学活动开展数量按医院按专业按月统计表：删除旧的（如果存在）
DROP TABLE IF EXISTS dws_hainan_hospital_info.dws_teaching_activity_count_by_hospital_major_month;

-- 教学活动开展数量按医院按专业按月统计表：创建
CREATE TABLE dws_hainan_hospital_info.dws_teaching_activity_count_by_hospital_major_month
(
    hospital_id    INT COMMENT '医院ID',
    hospital_name  STRING COMMENT '医院名称',
    major_id       INT COMMENT '专业ID',
    major_name     STRING COMMENT '专业名称',
    month          STRING COMMENT '统计月份',
    activity_count INT COMMENT '该月该专业开展的教学活动数量'
) COMMENT '教学活动开展数量按医院按专业按月统计表';

-- 教学活动开展数量按医院按专业按月统计表：清空数据
TRUNCATE TABLE dws_hainan_hospital_info.dws_teaching_activity_count_by_hospital_major_month;

-- 教学活动开展数量按医院按专业按月统计表：插入数据
INSERT INTO dws_hainan_hospital_info.dws_teaching_activity_count_by_hospital_major_month
SELECT hospital_id
     , hospital_name
     , major_id
     , major_name
     , date_format(start_time, 'yyyy-MM') AS month
     , count(1)                           AS activity_count
FROM dwd_hainan_hospital_info.dwd_teaching_activity_detail_df
WHERE year(start_time) >= 2019
GROUP BY hospital_id, hospital_name, major_id, major_name, date_format(start_time, 'yyyy-MM');

/*
    教学活动临床小讲课开展数量按医院按专业按年统计表    dws_teaching_activity_clinical_lecture_by_hospital_major_year
*/

-- 教学活动临床小讲课开展数量按医院按专业按年统计表：删除旧的（如果存在）
DROP TABLE IF EXISTS dws_hainan_hospital_info.dws_teaching_activity_clinical_lecture_by_hospital_major_year;

-- 教学活动临床小讲课开展数量按医院按专业按年统计表：创建
CREATE TABLE dws_hainan_hospital_info.dws_teaching_activity_clinical_lecture_by_hospital_major_year
(
    hospital_id    INT COMMENT '医院ID',
    hospital_name  STRING COMMENT '医院名称',
    major_id       INT COMMENT '专业ID',
    major_name     STRING COMMENT '专业名称',
    year           INT COMMENT '统计年份',
    activity_count INT COMMENT '该年该专业在该医院开展的临床小讲课数量'
) COMMENT '教学活动临床小讲课开展数量按医院按专业按年统计表';

-- 教学活动临床小讲课开展数量按医院按专业按年统计表：清空数据
TRUNCATE TABLE dws_hainan_hospital_info.dws_teaching_activity_clinical_lecture_by_hospital_major_year;

-- 教学活动临床小讲课开展数量按医院按专业按年统计表：插入数据
INSERT INTO dws_hainan_hospital_info.dws_teaching_activity_clinical_lecture_by_hospital_major_year
SELECT hospital_id
     , hospital_name
     , major_id
     , major_name
     , year(start_time) AS year
     , count(1)         AS activity_count
FROM dwd_hainan_hospital_info.dwd_teaching_activity_detail_df
WHERE year(start_time) >= 2019
  AND activity_type_id = 9
  AND activity_type_name = '临床小讲课'
GROUP BY hospital_id, hospital_name, major_id, major_name, year(start_time);

/*
    教学活动门诊教学开展数量按医院按专业按年统计表    dws_teaching_activity_outpatient_by_hospital_major_year
*/

-- 教学活动门诊教学开展数量按医院按专业按年统计表：删除旧的（如果存在）
DROP TABLE IF EXISTS dws_hainan_hospital_info.dws_teaching_activity_outpatient_by_hospital_major_year;

-- 教学活动门诊教学开展数量按医院按专业按年统计表：创建
CREATE TABLE dws_hainan_hospital_info.dws_teaching_activity_outpatient_by_hospital_major_year
(
    hospital_id    INT COMMENT '医院ID',
    hospital_name  STRING COMMENT '医院名称',
    major_id       INT COMMENT '专业ID',
    major_name     STRING COMMENT '专业名称',
    year           INT COMMENT '统计年份',
    activity_count INT COMMENT '该年该专业在该医院开展的门诊教学数量'
) COMMENT '教学活动门诊教学开展数量按医院按专业按年统计表';

-- 教学活动门诊教学开展数量按医院按专业按年统计表：清空数据
TRUNCATE TABLE dws_hainan_hospital_info.dws_teaching_activity_outpatient_by_hospital_major_year;

-- 教学活动门诊教学开展数量按医院按专业按年统计表：插入数据
INSERT INTO dws_hainan_hospital_info.dws_teaching_activity_outpatient_by_hospital_major_year
SELECT hospital_id
     , hospital_name
     , major_id
     , major_name
     , year(start_time) AS year
     , count(1)         AS activity_count
FROM dwd_hainan_hospital_info.dwd_teaching_activity_detail_df
WHERE year(start_time) >= 2019
  AND activity_type_id = 22
  AND activity_type_name = '门诊教学'
GROUP BY hospital_id, hospital_name, major_id, major_name, year(start_time);

/*
  教学活动评价次数统计按医院按年统计表 dws_teaching_activity_evaluation_count_by_hospital_year
*/

-- 教学活动评价次数统计按医院按年统计表：删除旧的（如果存在）
DROP TABLE IF EXISTS dws_hainan_hospital_info.dws_teaching_activity_evaluation_count_by_hospital_year;

-- 教学活动评价次数统计按医院按年统计表：创建
CREATE TABLE dws_hainan_hospital_info.dws_teaching_activity_evaluation_count_by_hospital_year
(
    hospital_id      INT COMMENT '医院ID',
    hospital_name    STRING COMMENT '医院名称',
    year             INT COMMENT '统计年份',
    evaluation_count INT COMMENT '实际评价总数',
    total_count      INT COMMENT '应评价总数',
    evaluation_rate  STRING COMMENT '评价回收率'
) COMMENT '教学活动评价次数统计按医院按年统计表';

-- 教学活动评价次数统计按医院按年统计表：清空数据
TRUNCATE TABLE dws_hainan_hospital_info.dws_teaching_activity_evaluation_count_by_hospital_year;

-- 教学活动评价次数统计按医院按年统计表：插入数据
INSERT INTO dws_hainan_hospital_info.dws_teaching_activity_evaluation_count_by_hospital_year
SELECT hospital_id
     , hospital_name
     , year(start_time)                                   AS year
     , count(`if`(evaluation_score IS NOT NULL, 1, NULL)) AS evaluation_count
     , count(1)                                           AS total_count
     , concat(round(count(`if`(evaluation_score IS NOT NULL, 1, NULL)) * 100 / count(1), 2),
              '%')                                        AS evaluation_rate
FROM dwd_hainan_hospital_info.dwd_teaching_activity_student_evaluation_wide_df
WHERE year(start_time) >= 2019
GROUP BY hospital_id, hospital_name, year(start_time);

/*
    教学活动开展次数按类型按天统计表 dws_teaching_activity_count_by_type_day
*/

-- 教学活动开展次数按类型按天统计表：删除旧的（如果存在）
DROP TABLE IF EXISTS dws_hainan_hospital_info.dws_teaching_activity_count_by_type_day;

-- 教学活动开展次数按类型按天统计表：创建
CREATE TABLE dws_hainan_hospital_info.dws_teaching_activity_count_by_type_day
(
    activity_type_id   INT COMMENT '教学活动类型ID',
    activity_type_name STRING COMMENT '教学活动类型名称',
    day                STRING COMMENT '统计日期',
    activity_count     INT COMMENT '该天该类型的教学活动数量'
) COMMENT '教学活动开展次数按类型按天统计表';

-- 教学活动开展次数按类型按天统计表：清空数据
TRUNCATE TABLE dws_hainan_hospital_info.dws_teaching_activity_count_by_type_day;

-- 教学活动开展次数按类型按天统计表：插入数据
INSERT INTO dws_hainan_hospital_info.dws_teaching_activity_count_by_type_day
SELECT activity_type_id
     , activity_type_name
     , date_format(start_time, 'yyyy-MM-dd') AS day
     , count(1)                              AS activity_count
FROM dwd_hainan_hospital_info.dwd_teaching_activity_detail_df
WHERE year(start_time) >= 2019
GROUP BY activity_type_id, activity_type_name, date_format(start_time, 'yyyy-MM-dd');




