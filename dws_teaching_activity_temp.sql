/*===================================================================
dws层数据整理：
目前已有表：
    教学活动_入院教育覆盖率按年统计表	dws_teaching_activity_hospital_education_coverage_by_year
    教学活动_入专业基地教育覆盖率按医院按年统计表	dws_teaching_activity_major_education_coverage_by_hospital_year
    教学活动_参与学员数按医院按年统计表	dws_teaching_activity_student_count_by_hospital_year
    教学活动_开展数量按医院按年统计表	dws_teaching_activity_count_by_hospital_year
    教学活动_开展数量按医院按专业按月统计表	dws_teaching_activity_count_by_hospital_major_month
    教学活动_临床小讲课开展数量按医院按专业按年统计表	dws_teaching_activity_clinical_lecture_by_hospital_major_year
    教学活动_门诊教学开展数量按医院按专业按年统计表	dws_teaching_activity_outpatient_by_hospital_major_year
    教学活动_评价次数统计按医院按年统计表	dws_teaching_activity_evaluation_count_by_hospital_year
    教学活动_开展次数按类型按月统计表	dws_teaching_activity_count_by_type_month
    教学活动_已招录学生但无活动记录按医院按年统计表	dws_teaching_activity_admission_students_no_activity_by_hospital_year
    教学活动_轮转学员教学活动数按医院按月统计表	dws_teaching_activity_round_student_activity_count_by_hospital_month
    教学活动_轮转学员在当月没有教学活动记录按医院按月统计表	dws_teaching_activity_round_student_no_activity_by_hospital_month
    教学活动_日均数据按医院按年统计表	dws_teaching_activity_daily_data_by_hospital_year
====================================================================*/

/*
    设置Hive执行引擎和MapReduce相关参数
    SET hive.execution.engine=mr;
    SET mapreduce.task.io.sort.mb = 1000;
*/

-- 确保目标数据库存在
CREATE DATABASE IF NOT EXISTS dws_hainan_hospital_info;

/*
中文表名：教学活动_入院教育覆盖率按年统计表
数据库表名：dws_teaching_activity_hospital_education_coverage_by_year
源数据表：
    - dwd_hainan_hospital_info.dwd_hainan_admission_data_view_students
    - dwd_hainan_hospital_info.dwd_teaching_activity_detail_df
*/

-- 教学活动_入院教育覆盖率按年统计表：删除旧的（如果存在）
DROP TABLE IF EXISTS dws_hainan_hospital_info.dws_teaching_activity_hospital_education_coverage_by_year;

-- 教学活动_入院教育覆盖率按年统计表：创建
CREATE TABLE dws_hainan_hospital_info.dws_teaching_activity_hospital_education_coverage_by_year
(
    year                     INT COMMENT '统计年份',
    hospital_admitted_count  INT COMMENT '该年实际招录学生的医院数',
    hospital_education_count INT COMMENT '该年开展过入院教育的医院数',
    coverage_education       STRING COMMENT '入院教育覆盖率'
) COMMENT '教学活动_入院教育覆盖率按年统计表';

-- 教学活动_入院教育覆盖率按年统计表：清空数据
TRUNCATE TABLE dws_hainan_hospital_info.dws_teaching_activity_hospital_education_coverage_by_year;

-- 教学活动_入院教育覆盖率按年统计表：插入数据
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
中文表名：教学活动_入专业基地教育覆盖率按医院按年统计表
数据库表名：dws_teaching_activity_major_education_coverage_by_hospital_year
源数据表：
    - dwd_hainan_hospital_info.dwd_hainan_admission_data_view_students
    - dwd_hainan_hospital_info.dwd_teaching_activity_detail_df
*/

-- 教学活动_入专业基地教育覆盖率按医院按年统计表：删除旧的（如果存在）
DROP TABLE IF EXISTS dws_hainan_hospital_info.dws_teaching_activity_major_education_coverage_by_hospital_year;

-- 教学活动_入专业基地教育覆盖率按医院按年统计表：创建
CREATE TABLE dws_hainan_hospital_info.dws_teaching_activity_major_education_coverage_by_hospital_year
(
    hospital_id           INT COMMENT '医院ID',
    hospital_name         STRING COMMENT '医院名称',
    year                  INT COMMENT '统计年份',
    major_admitted_count  INT COMMENT '该年该医院实际招录的专业数',
    major_education_count INT COMMENT '该年该医院开展过入专业基地教育的专业数',
    coverage_education    STRING COMMENT '入专业基地教育覆盖率'
) COMMENT '教学活动_入专业基地教育覆盖率按医院按年统计表';

-- 教学活动_入专业基地教育覆盖率按医院按年统计表：清空数据
TRUNCATE TABLE dws_hainan_hospital_info.dws_teaching_activity_major_education_coverage_by_hospital_year;

-- 教学活动_入专业基地教育覆盖率按医院按年统计表：插入数据
INSERT INTO dws_hainan_hospital_info.dws_teaching_activity_major_education_coverage_by_hospital_year
SELECT a.hospital_id
     , a.hospital_name
     , a.year
     , a.major_admitted_count
     , coalesce(b.major_education_count, 0) AS major_education_count
     , concat(round(coalesce(b.major_education_count, 0) * 100 / a.major_admitted_count, 2),
              '%')                          AS coverage_education
FROM (
         -- 分母：每年每家医院实际招录的专业数
         SELECT training_base_id                  AS hospital_id
              , training_base                     AS hospital_name
              , admission_year                    AS year
              , count(DISTINCT specialty_base_id) AS major_admitted_count
         FROM dwd_hainan_hospital_info.dwd_hainan_admission_data_view_students
         WHERE admission_year >= 2019
         GROUP BY admission_year, training_base_id, training_base) a
         LEFT JOIN
     (
         -- 分子：每年每家医院开展过入专业基地教育的专业数
         SELECT hospital_id
              , hospital_name
              , year(start_time)         AS year
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
中文表名：教学活动_参与学员数按医院按年统计表
数据库表名：dws_teaching_activity_student_count_by_hospital_year
源数据表：
    - dwd_hainan_hospital_info.dwd_teaching_activity_student_sign_wide_df
*/

-- 教学活动_参与学员数按医院按年统计表：删除旧的（如果存在）
DROP TABLE IF EXISTS dws_hainan_hospital_info.dws_teaching_activity_student_count_by_hospital_year;

-- 教学活动_参与学员数按医院按年统计表：创建
CREATE TABLE dws_hainan_hospital_info.dws_teaching_activity_student_count_by_hospital_year
(
    hospital_id   INT COMMENT '医院ID',
    hospital_name STRING COMMENT '医院名称',
    year          INT COMMENT '统计年份',
    student_count INT COMMENT '该年该医院参与的学员数'
) COMMENT '教学活动_参与学员数按医院按年统计表';

-- 教学活动_参与学员数按医院按年统计表：清空数据
TRUNCATE TABLE dws_hainan_hospital_info.dws_teaching_activity_student_count_by_hospital_year;

-- 教学活动_参与学员数按医院按年统计表：插入数据
INSERT INTO dws_hainan_hospital_info.dws_teaching_activity_student_count_by_hospital_year
SELECT hospital_id
     , hospital_name
     , year(start_time)             AS year
     , count(DISTINCT student_name) AS student_count
FROM dwd_hainan_hospital_info.dwd_teaching_activity_student_sign_wide_df
WHERE year(start_time) >= 2019
GROUP BY hospital_id, hospital_name, year(start_time);

/*
中文表名：教学活动_开展数量按医院按年统计表
数据库表名：dws_teaching_activity_count_by_hospital_year
源数据表：
    - dwd_hainan_hospital_info.dwd_teaching_activity_detail_df
*/

-- 教学活动_开展数量按医院按年统计表：删除旧的（如果存在）
DROP TABLE IF EXISTS dws_hainan_hospital_info.dws_teaching_activity_count_by_hospital_year;

-- 教学活动_开展数量按医院按年统计表：创建
CREATE TABLE dws_hainan_hospital_info.dws_teaching_activity_count_by_hospital_year
(
    hospital_id    INT COMMENT '医院ID',
    hospital_name  STRING COMMENT '医院名称',
    year           INT COMMENT '统计年份',
    activity_count INT COMMENT '该年该医院开展的教学活动数量'
) COMMENT '教学活动_开展数量按医院按年统计表';

-- 教学活动_开展数量按医院按年统计表：清空数据
TRUNCATE TABLE dws_hainan_hospital_info.dws_teaching_activity_count_by_hospital_year;

-- 教学活动_开展数量按医院按年统计表：插入数据
INSERT INTO dws_hainan_hospital_info.dws_teaching_activity_count_by_hospital_year
SELECT hospital_id
     , hospital_name
     , year(start_time) AS year
     , count(*)         AS activity_count
FROM dwd_hainan_hospital_info.dwd_teaching_activity_detail_df
WHERE year(start_time) >= 2019
GROUP BY hospital_id, hospital_name, year(start_time);

/*
中文表名：教学活动_开展数量按医院按专业按月统计表
数据库表名：dws_teaching_activity_count_by_hospital_major_month
源数据表：
    - dwd_hainan_hospital_info.dwd_teaching_activity_detail_df
*/

-- 教学活动_开展数量按医院按专业按月统计表：删除旧的（如果存在）
DROP TABLE IF EXISTS dws_hainan_hospital_info.dws_teaching_activity_count_by_hospital_major_month;

-- 教学活动_开展数量按医院按专业按月统计表：创建
CREATE TABLE dws_hainan_hospital_info.dws_teaching_activity_count_by_hospital_major_month
(
    hospital_id    INT COMMENT '医院ID',
    hospital_name  STRING COMMENT '医院名称',
    major_id       INT COMMENT '专业ID',
    major_name     STRING COMMENT '专业名称',
    month          STRING COMMENT '统计月份',
    activity_count INT COMMENT '该月该专业开展的教学活动数量'
) COMMENT '教学活动_开展数量按医院按专业按月统计表';

-- 教学活动_开展数量按医院按专业按月统计表：清空数据
TRUNCATE TABLE dws_hainan_hospital_info.dws_teaching_activity_count_by_hospital_major_month;

-- 教学活动_开展数量按医院按专业按月统计表：插入数据
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
中文表名：教学活动_临床小讲课开展数量按医院按专业按年统计表
数据库表名：dws_teaching_activity_clinical_lecture_by_hospital_major_year
源数据表：
    - dwd_hainan_hospital_info.dwd_teaching_activity_detail_df
*/

-- 教学活动_临床小讲课开展数量按医院按专业按年统计表：删除旧的（如果存在）
DROP TABLE IF EXISTS dws_hainan_hospital_info.dws_teaching_activity_clinical_lecture_by_hospital_major_year;

-- 教学活动_临床小讲课开展数量按医院按专业按年统计表：创建
CREATE TABLE dws_hainan_hospital_info.dws_teaching_activity_clinical_lecture_by_hospital_major_year
(
    hospital_id    INT COMMENT '医院ID',
    hospital_name  STRING COMMENT '医院名称',
    major_id       INT COMMENT '专业ID',
    major_name     STRING COMMENT '专业名称',
    year           INT COMMENT '统计年份',
    activity_count INT COMMENT '该年该专业在该医院开展的临床小讲课数量'
) COMMENT '教学活动_临床小讲课开展数量按医院按专业按年统计表';

-- 教学活动_临床小讲课开展数量按医院按专业按年统计表：清空数据
TRUNCATE TABLE dws_hainan_hospital_info.dws_teaching_activity_clinical_lecture_by_hospital_major_year;

-- 教学活动_临床小讲课开展数量按医院按专业按年统计表：插入数据
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
中文表名：教学活动_门诊教学开展数量按医院按专业按年统计表
数据库表名：dws_teaching_activity_outpatient_by_hospital_major_year
源数据表：
    - dwd_hainan_hospital_info.dwd_teaching_activity_detail_df
*/

-- 教学活动_门诊教学开展数量按医院按专业按年统计表：删除旧的（如果存在）
DROP TABLE IF EXISTS dws_hainan_hospital_info.dws_teaching_activity_outpatient_by_hospital_major_year;

-- 教学活动_门诊教学开展数量按医院按专业按年统计表：创建
CREATE TABLE dws_hainan_hospital_info.dws_teaching_activity_outpatient_by_hospital_major_year
(
    hospital_id    INT COMMENT '医院ID',
    hospital_name  STRING COMMENT '医院名称',
    major_id       INT COMMENT '专业ID',
    major_name     STRING COMMENT '专业名称',
    year           INT COMMENT '统计年份',
    activity_count INT COMMENT '该年该专业在该医院开展的门诊教学数量'
) COMMENT '教学活动_门诊教学开展数量按医院按专业按年统计表';

-- 教学活动_门诊教学开展数量按医院按专业按年统计表：清空数据
TRUNCATE TABLE dws_hainan_hospital_info.dws_teaching_activity_outpatient_by_hospital_major_year;

-- 教学活动_门诊教学开展数量按医院按专业按年统计表：插入数据
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
中文表名：教学活动_评价次数统计按医院按年统计表
数据库表名：dws_teaching_activity_evaluation_count_by_hospital_year
源数据表：
    - dwd_hainan_hospital_info.dwd_teaching_activity_student_evaluation_wide_df
*/

-- 教学活动_评价次数统计按医院按年统计表：删除旧的（如果存在）
DROP TABLE IF EXISTS dws_hainan_hospital_info.dws_teaching_activity_evaluation_count_by_hospital_year;

-- 教学活动_评价次数统计按医院按年统计表：创建
CREATE TABLE dws_hainan_hospital_info.dws_teaching_activity_evaluation_count_by_hospital_year
(
    hospital_id      INT COMMENT '医院ID',
    hospital_name    STRING COMMENT '医院名称',
    year             INT COMMENT '统计年份',
    evaluation_count INT COMMENT '实际评价总数',
    total_count      INT COMMENT '应评价总数',
    evaluation_rate  STRING COMMENT '评价回收率'
) COMMENT '教学活动_评价次数统计按医院按年统计表';

-- 教学活动_评价次数统计按医院按年统计表：清空数据
TRUNCATE TABLE dws_hainan_hospital_info.dws_teaching_activity_evaluation_count_by_hospital_year;

-- 教学活动_评价次数统计按医院按年统计表：插入数据
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
中文表名：教学活动_开展次数按类型按月统计表
数据库表名：dws_teaching_activity_count_by_type_month
源数据表：
    - dwd_hainan_hospital_info.dwd_teaching_activity_detail_df
*/

-- 教学活动_开展次数按类型按月统计表：删除旧的（如果存在）
DROP TABLE IF EXISTS dws_hainan_hospital_info.dws_teaching_activity_count_by_type_month;

-- 教学活动_开展次数按类型按月统计表：创建
CREATE TABLE dws_hainan_hospital_info.dws_teaching_activity_count_by_type_month
(
    activity_type_id   INT COMMENT '教学活动类型ID',
    activity_type_name STRING COMMENT '教学活动类型名称',
    month              STRING COMMENT '统计月份',
    activity_count     INT COMMENT '该天该类型的教学活动数量'
) COMMENT '教学活动_开展次数按类型按月统计表';

-- 教学活动_开展次数按类型按月统计表：清空数据
TRUNCATE TABLE dws_hainan_hospital_info.dws_teaching_activity_count_by_type_month;

-- 教学活动_开展次数按类型按月统计表：插入数据
INSERT INTO dws_hainan_hospital_info.dws_teaching_activity_count_by_type_month
SELECT activity_type_id
     , activity_type_name
     , date_format(start_time, 'yyyy-MM') AS month
     , count(1)                           AS activity_count
FROM dwd_hainan_hospital_info.dwd_teaching_activity_detail_df
WHERE year(start_time) >= 2019
GROUP BY activity_type_id, activity_type_name, date_format(start_time, 'yyyy-MM');


/*
中文表名：教学活动_已招录学生但无活动记录按医院按年统计表
数据库表名：dws_teaching_activity_admission_students_no_activity_by_hospital_year
源数据表：
    - dwd_hainan_hospital_info.dwd_hainan_admission_data_view_students
    - dwd_hainan_hospital_info.dwd_teaching_activity_student_sign_wide_df
*/

-- 教学活动_已招录学生但无活动记录按医院按年统计表：删除旧的（如果存在）
DROP TABLE IF EXISTS dws_hainan_hospital_info.dws_teaching_activity_admission_students_no_activity_by_hospital_year;

-- 教学活动_已招录学生但无活动记录按医院按年统计表：创建
CREATE TABLE dws_hainan_hospital_info.dws_teaching_activity_admission_students_no_activity_by_hospital_year
(
    hospital_id                  INT COMMENT '医院ID',
    hospital_name                STRING COMMENT '医院名称',
    year                         STRING COMMENT '统计年份',
    total_admitted_student_count INT COMMENT '总招录学生数',
    no_activity_student_count    INT COMMENT '未参加教学活动的学生数量',
    no_activity_student_rate     STRING COMMENT '未参加教学活动的学生比例'
) COMMENT '教学活动_已招录学生但无活动记录按医院按年统计表';

-- 教学活动_已招录学生但无活动记录按医院按年统计表：清空数据
TRUNCATE TABLE dws_hainan_hospital_info.dws_teaching_activity_admission_students_no_activity_by_hospital_year;

-- 教学活动_已招录学生但无活动记录按医院按年统计表：插入数据
INSERT INTO dws_hainan_hospital_info.dws_teaching_activity_admission_students_no_activity_by_hospital_year
SELECT s.training_base_id                                                                                                                                   AS hospital_id
     , s.training_base                                                                                                                                      AS hospital_name
     , s.admission_year                                                                                                                                     AS year
     , count(*)                                                                                                                                             AS total_admitted_student_count
     , count(CASE WHEN p.student_name IS NULL AND p.hospital_id IS NULL AND p.hospital_name IS NULL THEN 1 END)                                             AS no_activity_student_count
     , concat(round((count(CASE WHEN p.student_name IS NULL AND p.hospital_id IS NULL AND p.hospital_name IS NULL THEN 1 END) * 100.0) / count(*), 2), '%') AS no_activity_student_rate
FROM dwd_hainan_hospital_info.dwd_hainan_admission_data_view_students s
         LEFT JOIN(
    -- 获取有教学活动的学生数据（不能用id，因为两张表的ID不一样，后面无法关联）
    SELECT student_name
         , hospital_id
         , hospital_name
         , min(year(start_time)) AS min_sign_date
         , max(year(start_time)) AS max_sign_date
    FROM dwd_hainan_hospital_info.dwd_teaching_activity_student_sign_wide_df
    GROUP BY student_name, hospital_id, hospital_name) p
                  ON s.student_name = p.student_name
                      AND s.training_base_id = p.hospital_id
                      AND s.training_base = p.hospital_name
                      AND s.admission_year <= p.min_sign_date
                      AND s.admission_year + 3 >= p.max_sign_date
WHERE s.admission_year >= 2019
GROUP BY s.training_base_id, s.training_base, s.admission_year;

/*
中文表名：教学活动_轮转学员教学活动数按医院按月统计表
数据库表名：dws_teaching_activity_round_student_activity_count_by_hospital_month
源数据表：
    - dwd_hainan_hospital_info.dwd_teaching_activity_round_student_detail_wide_df
*/

-- 教学活动_轮转学员教学活动数按医院按月统计表：删除旧的（如果存在）
DROP TABLE IF EXISTS dws_hainan_hospital_info.dws_teaching_activity_round_student_activity_count_by_hospital_month;

-- 教学活动_轮转学员教学活动数按医院按月统计表：创建
CREATE TABLE dws_hainan_hospital_info.dws_teaching_activity_round_student_activity_count_by_hospital_month
(
    hospital_id    INT COMMENT '医院ID',
    hospital_name  STRING COMMENT '医院名称',
    month          STRING COMMENT '月份',
    student_name   STRING COMMENT '学生姓名',
    activity_count INT COMMENT '教学活动数量'
) COMMENT '教学活动_轮转学员教学活动数按医院按月统计表';

-- 教学活动_轮转学员教学活动数按人按医院按月统计表：清空数据
TRUNCATE TABLE dws_hainan_hospital_info.dws_teaching_activity_round_student_activity_count_by_hospital_month;

-- 教学活动_轮转学员教学活动数按人按医院按月统计表：插入数据
INSERT INTO dws_hainan_hospital_info.dws_teaching_activity_round_student_activity_count_by_hospital_month
SELECT hospital_id
     , hospital_name
     , rotation_month              AS month
     , student_name
     , count(DISTINCT activity_id) AS activity_count
FROM dwd_hainan_hospital_info.dwd_teaching_activity_round_student_detail_wide_df
WHERE rotation_month = activity_month
   OR activity_month IS NULL
GROUP BY hospital_id
       , hospital_name
       , rotation_month
       , student_name;

/*
中文表名：教学活动_轮转学员在当月没有教学活动记录按医院按月统计表
数据库表名：dws_teaching_activity_round_student_no_activity_by_hospital_month
源数据表：
    - dws_hainan_hospital_info.dws_teaching_activity_round_student_activity_count_by_hospital_month
*/

-- 教学活动_轮转学员在当月没有教学活动记录按医院按月统计表：删除旧的（如果存在）
DROP TABLE IF EXISTS dws_hainan_hospital_info.dws_teaching_activity_round_student_no_activity_by_hospital_month;

-- 教学活动_轮转学员在当月没有教学活动记录按医院按月统计表：创建
CREATE TABLE dws_hainan_hospital_info.dws_teaching_activity_round_student_no_activity_by_hospital_month
(

    hospital_id               INT COMMENT '医院ID',
    hospital_name             STRING COMMENT '医院名称',
    month                     STRING COMMENT '月份',
    total_round_student_count INT COMMENT '该月总轮转学员数',
    no_activity_student_count INT COMMENT '该月没有教学活动记录的学员数',
    no_activity_student_rate  STRING COMMENT '该月没有教学活动记录的学员比例'
) COMMENT '教学活动_轮转学员在当月没有教学活动记录按医院按月统计表';

-- 教学活动_轮转学员在当月没有教学活动记录按医院按月统计表：清空数据
TRUNCATE TABLE dws_hainan_hospital_info.dws_teaching_activity_round_student_no_activity_by_hospital_month;

-- 教学活动_轮转学员在当月没有教学活动记录按医院按月统计表：插入数据
INSERT INTO dws_hainan_hospital_info.dws_teaching_activity_round_student_no_activity_by_hospital_month
SELECT hospital_id
     , hospital_name
     , month
     , count(DISTINCT student_name)                                                                                            AS total_round_student_count
     , count(CASE WHEN activity_count = 0 THEN student_name END)                                                               AS no_activity_student_count
     , concat(round(count(CASE WHEN activity_count = 0 THEN student_name END) * 100.0 / count(DISTINCT student_name), 2), '%') AS no_activity_student_rate
FROM dws_hainan_hospital_info.dws_teaching_activity_round_student_activity_count_by_hospital_month
GROUP BY month, hospital_id, hospital_name;

/*
中文表名：教学活动_日均数据按医院按年统计表
数据库表名：dws_teaching_activity_daily_data_by_hospital_year
源数据表：
    - dwd_hainan_hospital_info.dws_teaching_activity_count_by_hospital_year
    - dwd_hainan_hospital_info.dws_teaching_activity_student_count_by_hospital_year
    - dwd_hainan_hospital_info.dws_teaching_activity_evaluation_count_by_hospital_year
*/

-- 教学活动_日均数据按医院按年统计表：删除旧的（如果存在）
DROP TABLE IF EXISTS dws_hainan_hospital_info.dws_teaching_activity_daily_data_by_hospital_year;

-- 教学活动_日均数据按医院按年统计表：创建
CREATE TABLE dws_hainan_hospital_info.dws_teaching_activity_daily_data_by_hospital_year
(
    hospital_id      INT COMMENT '医院ID',
    hospital_name    STRING COMMENT '医院名称',
    year             INT COMMENT '统计年份',
    daily_activity   DOUBLE COMMENT '日均教学活动数量',
    daily_student    DOUBLE COMMENT '日均参与学员数',
    daily_evaluation DOUBLE COMMENT '日均评价次数'
) COMMENT '教学活动_日均数据按医院按年统计表';

-- 教学活动_日均数据按医院按年统计表：清空数据
TRUNCATE TABLE dws_hainan_hospital_info.dws_teaching_activity_daily_data_by_hospital_year;

-- 教学活动_日均数据按医院按年统计表：插入数据
INSERT INTO dws_hainan_hospital_info.dws_teaching_activity_daily_data_by_hospital_year
SELECT a.hospital_id
     , a.hospital_name
     , a.year
     , round(sum(activity_count) / 365, 2)   AS daily_activity
     , round(sum(student_count) / 365, 2)    AS daily_student
     , round(sum(evaluation_count) / 365, 2) AS daily_evaluation
FROM dws_hainan_hospital_info.dws_teaching_activity_count_by_hospital_year a
         JOIN dws_hainan_hospital_info.dws_teaching_activity_student_count_by_hospital_year b
              ON a.hospital_id = b.hospital_id
                  AND a.year = b.year
         JOIN dws_hainan_hospital_info.dws_teaching_activity_evaluation_count_by_hospital_year c
              ON a.hospital_id = c.hospital_id
                  AND a.year = c.year
GROUP BY a.year, a.hospital_id, a.hospital_name;

