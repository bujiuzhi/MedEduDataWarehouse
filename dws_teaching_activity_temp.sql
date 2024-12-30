/*===================================================================
dws层数据整理：
目前已有表：
    教学活动_数据总览按医院按年统计表   dws_teaching_activity_data_overview_by_hospital_year
    教学活动_开展数量按医院按专业按类型按月统计表   dws_teaching_activity_count_by_hospital_major_type_month
    教学活动_入院教育覆盖率按年统计表   dws_teaching_activity_hospital_education_coverage_by_year
    教学活动_入专业基地教育覆盖率按医院按年统计表   dws_teaching_activity_major_education_coverage_by_hospital_year
    教学活动_已招录学生教学活动记录和轮转情况按医院按年统计表   dws_teaching_activity_admission_students_activity_round_count_by_hospital_year
    教学活动_轮转学员教学活动情况按医院按月统计表   dws_teaching_activity_round_student_activity_count_by_hospital_month
    教学活动_轮转学员没有教学活动记录的人数按医院按月统计表   dws_teaching_activity_round_no_activity_student_count_by_hospital_month
    教学活动_轮转学员不在本科室上课的学员数按医院按月统计表   dws_teaching_activity_round_no_class_student_count_by_hospital_month
    教学活动_全科专业和门诊教学相关指标按医院按月统计表   dws_teaching_activity_general_practice_outpatient_education_by_hospital_month
    教学活动_名称不规范相关指标按医院按专业按月统计表   dws_teaching_activity_name_irregular_count_by_hospital_major_month
====================================================================*/

/*
    设置Hive执行引擎和MapReduce相关参数
    SET hive.execution.engine=mr;
    SET mapreduce.task.io.sort.mb = 1000;
*/

-- 确保目标数据库存在
CREATE DATABASE IF NOT EXISTS dws_hainan_hospital_info;

/*
中文表名：教学活动_数据总览按医院按年统计表
数据库表名：dws_teaching_activity_data_overview_by_hospital_year
源数据表：
    - dwd_hainan_hospital_info.dwd_teaching_activity_detail_df
    - dwd_hainan_hospital_info.dwd_teaching_activity_student_sign_wide_df
    - dwd_hainan_hospital_info.dwd_teaching_activity_teacher_detail_wide_df
*/

-- 教学活动_数据总览按医院按年统计表：删除旧的（如果存在）
DROP TABLE IF EXISTS dws_hainan_hospital_info.dws_teaching_activity_data_overview_by_hospital_year;

-- 教学活动_数据总览按医院按年统计表：创建
CREATE TABLE dws_hainan_hospital_info.dws_teaching_activity_data_overview_by_hospital_year
(
    hospital_id                    INT COMMENT '医院ID',
    hospital_name                  STRING COMMENT '医院名称',
    year                           INT COMMENT '统计年份',
    activity_count                 INT COMMENT '该年该医院开展的教学活动数量',
    total_person_times             INT COMMENT '该年该医院总的参与人次',
    student_count                  INT COMMENT '该年该医院参与的学员数',
    avg_activity_times_per_student DOUBLE COMMENT '该年该医院平均每个学员参与的活动次数',
    sign_count                     INT COMMENT '该年该医院签到次数',
    no_sign_count                  INT COMMENT '该年该医院未签到次数',
    sign_rate                      STRING COMMENT '签到率',
    evaluation_count               INT COMMENT '该年该医院评价次数',
    total_evaluation_count         INT COMMENT '该年该医院应评价总数',
    evaluation_rate                STRING COMMENT '评价回收率'
) COMMENT '教学活动_数据总览按医院按年统计表';

-- 教学活动_数据总览按医院按年统计表：清空数据
TRUNCATE TABLE dws_hainan_hospital_info.dws_teaching_activity_data_overview_by_hospital_year;

-- 教学活动_数据总览按医院按年统计表：插入数据
INSERT INTO dws_hainan_hospital_info.dws_teaching_activity_data_overview_by_hospital_year
SELECT ac.hospital_id
     , ac.hospital_name
     , ac.year
     , activity_count
     , total_person_times
     , student_count
     , round(total_person_times / student_count, 2) AS avg_activity_times_per_student
     , sign_count
     , no_sign_count
     , sign_rate
     , evaluation_count
     , total_evaluation_count
     , evaluation_rate
FROM (
         -- 教学活动明细的数据总览
         SELECT hospital_id
              , hospital_name
              , year(start_time)            AS year
              , count(DISTINCT activity_id) AS activity_count
              , sum(participant_count)      AS total_person_times
         FROM dwd_hainan_hospital_info.dwd_teaching_activity_detail_df
         WHERE year(start_time) >= 2019
         GROUP BY hospital_id, hospital_name, year(start_time)) ac
         LEFT JOIN (
    -- 教学活动的学员数据总览
    SELECT hospital_id
         , hospital_name
         , year(start_time)                                                          AS year
         , count(DISTINCT student_name)                                              AS student_count
         , count(CASE WHEN is_signed = '已签到' THEN 1 END)                          AS sign_count
         , count(CASE WHEN is_signed = '未签到' THEN 1 END)                          AS no_sign_count
         , concat(round(count(CASE WHEN is_signed = '已签到' THEN 1 END) * 100 /
                        (count(CASE WHEN is_signed = '已签到' THEN 1 END) +
                         count(CASE WHEN is_signed = '未签到' THEN 1 END)), 2), '%') AS sign_rate
    FROM dwd_hainan_hospital_info.dwd_teaching_activity_student_sign_wide_df
    WHERE year(start_time) >= 2019
    GROUP BY hospital_id, hospital_name, year(start_time)) sc
                   ON ac.hospital_id = sc.hospital_id
                       AND ac.hospital_name = sc.hospital_name AND ac.year = sc.year
         LEFT JOIN (
    -- 教学活动评价数据总览
    SELECT hospital_id
         , hospital_name
         , year(start_time)                                   AS year
         , count(`if`(evaluation_score IS NOT NULL, 1, NULL)) AS evaluation_count
         , count(1)                                           AS total_evaluation_count
         , concat(round(count(`if`(evaluation_score IS NOT NULL, 1, NULL)) * 100 / count(1), 2),
                  '%')                                        AS evaluation_rate
    FROM dwd_hainan_hospital_info.dwd_teaching_activity_student_evaluation_wide_df
    WHERE year(start_time) >= 2019
    GROUP BY hospital_id, hospital_name, year(start_time)) ec
                   ON ac.hospital_id = ec.hospital_id
                       AND ac.hospital_name = ec.hospital_name
                       AND ac.year = ec.year;

/*
中文表名：教学活动_开展数量按医院按专业按类型按月统计表
数据库表名：dws_teaching_activity_count_by_hospital_major_type_month
源数据表：
    - dwd_hainan_hospital_info.dwd_teaching_activity_detail_df
    - dim_hainan_hospital_info.dim_teacher_activity_count_standard_by_hospital_major
*/

-- 教学活动_开展数量按医院按专业按类型按月统计表：删除旧的（如果存在）
DROP TABLE IF EXISTS dws_hainan_hospital_info.dws_teaching_activity_count_by_hospital_major_type_month;

-- 教学活动_开展数量按医院按专业按类型按月统计表：创建
CREATE TABLE dws_hainan_hospital_info.dws_teaching_activity_count_by_hospital_major_type_month
(
    hospital_id        INT COMMENT '医院ID',
    hospital_name      STRING COMMENT '医院名称',
    major_id           INT COMMENT '专业ID',
    major_name         STRING COMMENT '专业名称',
    activity_type_id   INT COMMENT '活动类型ID',
    activity_type_name STRING COMMENT '活动类型名称',
    month              STRING COMMENT '统计月份',
    activity_count     INT COMMENT '该月该专业该类型开展的教学活动数量',
    standard_count     INT COMMENT '该类型开展数量标准',
    is_standard        INT COMMENT '是否达到标准'
) COMMENT '教学活动_开展数量按医院按专业按类型按月统计表';

-- 教学活动_开展数量按医院按专业按类型按月统计表：清空数据
TRUNCATE TABLE dws_hainan_hospital_info.dws_teaching_activity_count_by_hospital_major_type_month;

-- 教学活动_开展数量按医院按专业按类型按月统计表：插入数据
INSERT INTO dws_hainan_hospital_info.dws_teaching_activity_count_by_hospital_major_type_month
SELECT ad.hospital_id
     , ad.hospital_name
     , ad.major_id
     , ad.major_name
     , ad.activity_type_id
     , ad.activity_type_name
     , ad.month
     , ad.activity_count
     , st.standard_count
     , `if`(activity_count >= st.standard_count OR st.standard_count IS NULL, 1, 0) AS is_standard
FROM (SELECT hospital_id
           , hospital_name
           , major_id
           , major_name
           , activity_type_id
           , activity_type_name
           , date_format(start_time, 'yyyy-MM') AS month
           , count(DISTINCT activity_id)        AS activity_count
      FROM dwd_hainan_hospital_info.dwd_teaching_activity_detail_df
      WHERE year(start_time) >= 2019
      GROUP BY hospital_id, hospital_name, major_id, major_name, activity_type_id, activity_type_name, date_format(start_time, 'yyyy-MM')) ad
         LEFT JOIN dim_hainan_hospital_info.dim_teacher_activity_count_standard_by_hospital_major st
                   ON ad.major_id = st.major_id
                       AND ad.major_name = st.major_name
                       AND ad.activity_type_id = st.activity_type_id
                       AND ad.activity_type_name = st.activity_type_name;

/*
中文表名：教学活动_入院教育覆盖率按年统计表
数据库表名：dws_teaching_activity_hospital_education_coverage_by_year
源数据表：
    - dwd_hainan_hospital_info.dwd_teaching_activity_admission_hospital_education_detail_wide_df
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
SELECT year
     , count(DISTINCT hospital_id)                                                                                                 AS hospital_admitted_count
     , count(DISTINCT `if`(activity_id IS NOT NULL, hospital_id, NULL))                                                            AS hospital_education_count
     , concat(round(count(DISTINCT `if`(activity_id IS NOT NULL, hospital_id, NULL)) * 100 / count(DISTINCT hospital_id), 2), '%') AS coverage_education
FROM dwd_hainan_hospital_info.dwd_teaching_activity_admission_hospital_education_detail_wide_df
GROUP BY year;

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
SELECT hospital_id
     , hospital_name
     , year
     , count(DISTINCT major_id)                                                                                              AS hospital_admitted_count
     , count(DISTINCT `if`(activity_id IS NOT NULL, major_id, NULL))                                                         AS hospital_education_count
     , concat(round(count(DISTINCT `if`(activity_id IS NOT NULL, major_id, NULL)) * 100 / count(DISTINCT major_id), 2), '%') AS coverage_education
FROM dwd_hainan_hospital_info.dwd_teaching_activity_admission_hospital_major_education_detail_wide_df
GROUP BY hospital_id, hospital_name, year;

/*
中文表名：教学活动_已招录学生教学活动记录和轮转情况按医院按年统计表
数据库表名：dws_teaching_activity_admission_students_activity_round_count_by_hospital_year
源数据表：
    - dwd_hainan_hospital_info.dwd_hainan_admission_data_view_students
    - dwd_hainan_hospital_info.dwd_teaching_activity_student_sign_wide_df
    - dwd_hainan_hospital_info.dwd_hainan_round_spt_round_total_info
*/

-- 教学活动_已招录学生教学活动记录和轮转情况按医院按年统计表：删除旧的（如果存在）
DROP TABLE IF EXISTS dws_hainan_hospital_info.dws_teaching_activity_admission_students_activity_round_count_by_hospital_year;

-- 教学活动_已招录学生教学活动记录和轮转情况按医院按年统计表：创建
CREATE TABLE dws_hainan_hospital_info.dws_teaching_activity_admission_students_activity_round_count_by_hospital_year
(
    hospital_id                        INT COMMENT '医院ID',
    hospital_name                      STRING COMMENT '医院名称',
    year                               STRING COMMENT '统计年份',
    total_admitted_student_count       INT COMMENT '总招录学生数',
    round_student_count                INT COMMENT '有轮转的学生数',
    no_round_student_count             INT COMMENT '无轮转的学生数',
    activity_student_count             INT COMMENT '有教学活动的学生数',
    no_activity_student_count          INT COMMENT '无教学活动的学生数',
    round_no_activity_student_count    INT COMMENT '有轮转无教学活动的学生数',
    round_activity_student_count       INT COMMENT '有轮转有教学活动的学生数',
    no_round_no_activity_student_count INT COMMENT '无轮转无教学活动的学生数'
) COMMENT '教学活动_已招录学生教学活动记录和轮转情况按医院按年统计表';

-- 教学活动_已招录学生教学活动记录和轮转情况按医院按年统计表：清空数据
TRUNCATE TABLE dws_hainan_hospital_info.dws_teaching_activity_admission_students_activity_round_count_by_hospital_year;

-- 教学活动_已招录学生教学活动记录和轮转情况按医院按年统计表：插入数据
INSERT INTO dws_hainan_hospital_info.dws_teaching_activity_admission_students_activity_round_count_by_hospital_year
SELECT ad.hospital_id                                                                                                 AS hospital_id
     , ad.hospital_name                                                                                               AS hospital_name
     , ad.admission_year                                                                                              AS year
     , count(DISTINCT ad.student_name)                                                                                AS total_admitted_student_count
     , count(DISTINCT CASE WHEN ro.student_name IS NOT NULL THEN ad.student_name END)                                 AS round_student_count
     , count(DISTINCT CASE WHEN ro.student_name IS NULL THEN ad.student_name END)                                     AS no_round_student_count
     , count(DISTINCT CASE WHEN ac.student_name IS NOT NULL THEN ad.student_name END)                                 AS activity_student_count
     , count(DISTINCT CASE WHEN ac.student_name IS NULL THEN ad.student_name END)                                     AS no_activity_student_count
     , count(DISTINCT CASE WHEN ro.student_name IS NOT NULL AND ac.student_name IS NULL THEN ad.student_name END)     AS round_no_activity_student_count
     , count(DISTINCT CASE WHEN ro.student_name IS NOT NULL AND ac.student_name IS NOT NULL THEN ad.student_name END) AS round_activity_student_count
     , count(DISTINCT CASE WHEN ro.student_name IS NULL AND ac.student_name IS NULL THEN ad.student_name END)         AS no_round_no_activity_student_count
FROM (
         -- 招录的学生数据
         SELECT DISTINCT training_base_id  AS hospital_id
                       , training_base     AS hospital_name
                       , specialty_base_id AS major_id
                       , specialty_base    AS major_name
                       , admission_year
                       , student_name
         FROM dwd_hainan_hospital_info.dwd_hainan_admission_data_view_students) ad
         LEFT JOIN (
    -- 轮转的学生数据
    SELECT DISTINCT CASE
                        WHEN hospital = '301海南医院' THEN 130
                        WHEN hospital = '海南医学院第一附属医院' THEN 110
                        WHEN hospital = '三亚中心医院（海南省第三人民医院）' THEN 111
                        ELSE -1
        END                        AS hospital_id
                  , CASE
                        WHEN hospital = '301海南医院' THEN '解放军总医院海南医院'
                        WHEN hospital = '海南医学院第一附属医院' THEN '海南医学院第一附属医院'
                        WHEN hospital = '三亚中心医院（海南省第三人民医院）' THEN '三亚中心医院（海南省第三人民医院）'
                        ELSE '其他'
        END                        AS hospital_name
                  , spt_major_id   AS major_id
                  , spt_major_name AS major_name
                  , personname     AS student_name
    FROM dwd_hainan_hospital_info.dwd_hainan_round_spt_round_total_info) ro
                   ON ad.hospital_id = ro.hospital_id
                       AND ad.hospital_name = ro.hospital_name
                       AND ad.major_id = ro.major_id
                       AND ad.major_name = ro.major_name
                       AND ad.student_name = ro.student_name
         LEFT JOIN(
    -- 获取有教学活动的学生数据（不能用id，因为两张表的ID不一样，后面无法关联）
    SELECT DISTINCT hospital_id
                  , hospital_name
                  , major_id
                  , major_name
                  , student_name
    FROM dwd_hainan_hospital_info.dwd_teaching_activity_student_sign_wide_df) ac
                  ON ad.hospital_id = ac.hospital_id
                      AND ad.hospital_name = ac.hospital_name
                      AND ad.major_id = ac.major_id
                      AND ad.major_name = ac.major_name
                      AND ad.student_name = ac.student_name
WHERE ad.admission_year >= 2019
GROUP BY ad.hospital_id, ad.hospital_name, ad.admission_year;

/*
中文表名：教学活动_轮转学员教学活动情况按医院按月统计表
数据库表名：dws_teaching_activity_round_student_activity_count_by_hospital_month
源数据表：
    - dwd_hainan_hospital_info.dwd_teaching_activity_round_student_detail_wide_df
*/

-- 教学活动_轮转学员教学活动情况按医院按月统计表：删除旧的（如果存在）

DROP TABLE IF EXISTS dws_hainan_hospital_info.dws_teaching_activity_round_student_activity_count_by_hospital_month;

-- 教学活动_轮转学员教学活动情况按医院按月统计表：创建
CREATE TABLE dws_hainan_hospital_info.dws_teaching_activity_round_student_activity_count_by_hospital_month
(
    hospital_id       INT COMMENT '医院ID',
    hospital_name     STRING COMMENT '医院名称',
    month             STRING COMMENT '月份',
    student_name      STRING COMMENT '学生姓名',
    rotation_office   STRING COMMENT '轮转科室',
    activity_count    INT COMMENT '教学活动次数',
    class_count       INT COMMENT '本科室上课次数',
    other_class_count INT COMMENT '其他科室上课次数'
) COMMENT '教学活动_轮转学员教学活动情况按医院按月统计表';

-- 教学活动_轮转学员教学活动情况按医院按月统计表：清空数据
TRUNCATE TABLE dws_hainan_hospital_info.dws_teaching_activity_round_student_activity_count_by_hospital_month;

-- 教学活动_轮转学员教学活动情况按医院按月统计表：插入数据
INSERT INTO dws_hainan_hospital_info.dws_teaching_activity_round_student_activity_count_by_hospital_month
SELECT hospital_id
     , hospital_name
     , rotation_month                                                                    AS month
     , student_name
     , rotation_office
     , count(DISTINCT activity_id)                                                       AS activity_count
     , count(DISTINCT CASE WHEN rotation_office = activity_office THEN activity_id END)  AS class_count
     , count(DISTINCT CASE WHEN rotation_office != activity_office THEN activity_id END) AS other_class_count
FROM dwd_hainan_hospital_info.dwd_teaching_activity_round_student_detail_wide_df
WHERE rotation_month >= '2019-01'
GROUP BY hospital_id, hospital_name, rotation_month, student_name, rotation_office;

/*
中文表名：教学活动_轮转学员没有教学活动记录的人数按医院按月统计表
数据库表名：dws_teaching_activity_round_no_activity_student_count_by_hospital_month
源数据表：
    - dws_hainan_hospital_info.dws_teaching_activity_round_student_activity_count_by_hospital_month
*/

-- 教学活动_轮转学员没有教学活动记录的人数按医院按月统计表：删除旧的（如果存在）
DROP TABLE IF EXISTS dws_hainan_hospital_info.dws_teaching_activity_round_no_activity_student_count_by_hospital_month;

-- 教学活动_轮转学员没有教学活动记录的人数按医院按月统计表：创建
CREATE TABLE dws_hainan_hospital_info.dws_teaching_activity_round_no_activity_student_count_by_hospital_month
(

    hospital_id               INT COMMENT '医院ID',
    hospital_name             STRING COMMENT '医院名称',
    month                     STRING COMMENT '月份',
    total_round_student_count INT COMMENT '该月总轮转学员数',
    no_activity_student_count INT COMMENT '该月没有教学活动记录的学员数',
    no_activity_student_rate  STRING COMMENT '该月没有教学活动记录的学员比例'
) COMMENT '教学活动_轮转学员没有教学活动记录的人数按医院按月统计表';

-- 教学活动_轮转学员没有教学活动记录的人数按医院按月统计表：清空数据
TRUNCATE TABLE dws_hainan_hospital_info.dws_teaching_activity_round_no_activity_student_count_by_hospital_month;

-- 教学活动_轮转学员没有教学活动记录的人数按医院按月统计表：插入数据
INSERT INTO dws_hainan_hospital_info.dws_teaching_activity_round_no_activity_student_count_by_hospital_month
SELECT hospital_id
     , hospital_name
     , month
     , count(DISTINCT student_name)                                                                                                     AS total_round_student_count
     , count(DISTINCT CASE WHEN activity_count = 0 THEN student_name END)                                                               AS no_activity_student_count
     , concat(round(count(DISTINCT CASE WHEN activity_count = 0 THEN student_name END) * 100.0 / count(DISTINCT student_name), 2), '%') AS no_activity_student_rate
FROM dws_hainan_hospital_info.dws_teaching_activity_round_student_activity_count_by_hospital_month
WHERE month >= '2019-01'
GROUP BY hospital_id, hospital_name, month;

/*
中文表名：教学活动_轮转学员不在本科室上课的学员数按医院按月统计表
数据库表名：dws_teaching_activity_round_no_class_student_count_by_hospital_month
源数据表：
    - dwd_hainan_hospital_info.dwd_teaching_activity_round_student_detail_wide_df
*/

-- 教学活动_轮转学员不在本科室上课的学员数按医院按月统计表：删除旧的（如果存在）
DROP TABLE IF EXISTS dws_hainan_hospital_info.dws_teaching_activity_round_no_class_student_count_by_hospital_month;

-- 教学活动_轮转学员不在本科室上课的学员数按医院按月统计表：创建
CREATE TABLE dws_hainan_hospital_info.dws_teaching_activity_round_no_class_student_count_by_hospital_month
(
    hospital_id               INT COMMENT '医院ID',
    hospital_name             STRING COMMENT '医院名称',
    month                     STRING COMMENT '月份',
    total_round_student_count INT COMMENT '该月总轮转学员数',
    no_class_student_count    INT COMMENT '该月不在本科室上课的学员数',
    no_class_student_rate     STRING COMMENT '该月不在本科室上课的学员比例'
) COMMENT '教学活动_轮转学员不在本科室上课的学员数按医院按月统计表';

-- 教学活动_轮转学员不在本科室上课的学员数按医院按月统计表：清空数据
TRUNCATE TABLE dws_hainan_hospital_info.dws_teaching_activity_round_no_class_student_count_by_hospital_month;

-- 教学活动_轮转学员不在本科室上课的学员数按医院按月统计表：插入数据
INSERT INTO dws_hainan_hospital_info.dws_teaching_activity_round_no_class_student_count_by_hospital_month
SELECT hospital_id
     , hospital_name
     , rotation_month                                                                     AS month
     , count(DISTINCT student_name)                                                       AS total_round_student_count
     , count(DISTINCT CASE WHEN rotation_office != activity_office THEN student_name END) AS no_class_student_count
     , concat(round(count(DISTINCT CASE WHEN rotation_office != activity_office THEN student_name END) * 100.0 /
                    count(DISTINCT student_name), 2), '%')                                AS no_class_student_rate
FROM dwd_hainan_hospital_info.dwd_teaching_activity_round_student_detail_wide_df
WHERE rotation_month >= '2019-01'
GROUP BY hospital_id, hospital_name, rotation_month;

/*
中文表名：教学活动_全科专业和门诊教学相关指标按医院按月统计表
数据库表名：dws_teaching_activity_general_practice_outpatient_education_by_hospital_month
源数据表：
    - dws_hainan_hospital_info.dws_teaching_activity_count_by_hospital_major_type_month
*/

-- 教学活动_全科专业和门诊教学相关指标按医院按月统计表：删除旧的（如果存在）
DROP TABLE IF EXISTS dws_hainan_hospital_info.dws_teaching_activity_general_practice_outpatient_education_by_hospital_month;

-- 教学活动_全科专业和门诊教学相关指标按医院按月统计表：创建
CREATE TABLE dws_hainan_hospital_info.dws_teaching_activity_general_practice_outpatient_education_by_hospital_month
(
    hospital_id                              INT COMMENT '医院ID',
    hospital_name                            STRING COMMENT '医院名称',
    month                                    STRING COMMENT '月份',
    general_practice_outpatient_count        INT COMMENT '全科专业开展门诊教学数量',
    is_general_practice_outpatient           INT COMMENT '全科专业是否开了门诊教学',
    outpatient_teaching_specialties          INT COMMENT '开展门诊教学专业数（除全科）',
    is_outpatient_teaching_specialties_valid INT COMMENT '开展门诊教学专业数（除全科）是否合格'
) COMMENT '教学活动_全科专业和门诊教学相关指标按医院按月统计表';

-- 教学活动_全科专业和门诊教学相关指标按医院按月统计表：清空数据
TRUNCATE TABLE dws_hainan_hospital_info.dws_teaching_activity_general_practice_outpatient_education_by_hospital_month;

-- 教学活动_全科专业和门诊教学相关指标按医院按月统计表：插入数据
INSERT INTO dws_hainan_hospital_info.dws_teaching_activity_general_practice_outpatient_education_by_hospital_month
SELECT hospital_id
     , hospital_name
     , month
     , sum(CASE WHEN major_id = 18 AND major_name = '全科医学科' AND activity_type_id = 22 AND activity_type_name = '门诊教学' THEN activity_count ELSE 0 END) AS general_practice_outpatient_count
     , CASE
           WHEN sum(CASE WHEN major_id = 18 AND major_name = '全科医学科' AND activity_type_id = 22 AND activity_type_name = '门诊教学' THEN activity_count ELSE 0 END) > 0 THEN 1
           ELSE 0 END                                                                                                                                          AS is_general_practice_outpatient
     , sum(CASE
               WHEN major_id != 18 AND major_name != '全科医学科' AND activity_type_id = 22 AND activity_type_name = '门诊教学' THEN major_id
               ELSE 0 END)                                                                                                                                     AS outpatient_teaching_specialties
     , CASE
           WHEN sum(CASE WHEN major_id != 18 AND major_name != '全科医学科' AND activity_type_id = 22 AND activity_type_name = '门诊教学' THEN major_id ELSE 0 END) >= 2 THEN 1
           ELSE 0 END                                                                                                                                          AS is_outpatient_teaching_specialties_valid
FROM dws_hainan_hospital_info.dws_teaching_activity_count_by_hospital_major_type_month
WHERE month >= '2019-01'
GROUP BY hospital_id, hospital_name, month;

/*
中文表名：教学活动_名称不规范相关指标按医院按专业按月统计表
数据库表名：dws_teaching_activity_name_irregular_count_by_hospital_major_month
源数据表：
    - dwd_hainan_hospital_info.dwd_teaching_activity_detail_df
*/

-- 教学活动_名称不规范相关指标按医院按专业按月统计表：删除旧的（如果存在）
DROP TABLE IF EXISTS dws_hainan_hospital_info.dws_teaching_activity_name_irregular_count_by_hospital_major_month;

-- 教学活动_名称不规范相关指标按医院按专业按月统计表：创建
CREATE TABLE dws_hainan_hospital_info.dws_teaching_activity_name_irregular_count_by_hospital_major_month
(
    hospital_id                   INT COMMENT '医院ID',
    hospital_name                 STRING COMMENT '医院名称',
    major_id                      INT COMMENT '专业ID',
    major_name                    STRING COMMENT '专业名称',
    month                         STRING COMMENT '月份',
    activity_name_type_same_count INT COMMENT '本月内单个教学活动的活动名称和类型名称重名的教学活动数',
    activity_name_type_same_total INT COMMENT '本月内不同教学活动但同类型同名称的教学活动数'
) COMMENT '教学活动_名称不规范相关指标按医院按专业按月统计表';

-- 教学活动_名称不规范相关指标按医院按专业按月统计表：清空数据
TRUNCATE TABLE dws_hainan_hospital_info.dws_teaching_activity_name_irregular_count_by_hospital_major_month;

-- 教学活动_名称不规范相关指标按医院按专业按月统计表：插入数据
INSERT INTO dws_hainan_hospital_info.dws_teaching_activity_name_irregular_count_by_hospital_major_month
SELECT grouped.hospital_id
     , grouped.hospital_name
     , grouped.major_id
     , grouped.major_name
     , grouped.month
     , sum(CASE WHEN grouped.activity_name = grouped.activity_type_name THEN grouped.activity_count ELSE 0 END) AS activity_name_type_same_count
     , sum(CASE WHEN grouped.activity_count > 1 THEN grouped.activity_count ELSE 0 END)                         AS activity_name_type_same_total
FROM (SELECT hospital_id
           , hospital_name
           , major_id
           , major_name
           , date_format(start_time, 'yyyy-MM') AS month
           , activity_name
           , activity_type_name
           , count(DISTINCT activity_id)        AS activity_count
      FROM dwd_hainan_hospital_info.dwd_teaching_activity_detail_df
      GROUP BY hospital_id, hospital_name, major_id, major_name, date_format(start_time, 'yyyy-MM'), activity_name, activity_type_name) AS grouped
WHERE grouped.month >= '2019-01'
GROUP BY grouped.hospital_id, grouped.hospital_name, grouped.major_id, grouped.major_name, grouped.month;
