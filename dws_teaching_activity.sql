/*===================================================================
dws层数据整理：
目前已有表：
    教学活动_数据总览按医院按年统计表	dws_teaching_activity_data_overview_by_hospital_year
    教学活动_开展数量按医院按专业按类型按月统计表	dws_teaching_activity_count_by_hospital_major_type_month
    教学活动_入院教育覆盖率按年统计表	dws_teaching_activity_hospital_education_coverage_by_year
    教学活动_入专业基地教育覆盖率按医院按年统计表	dws_teaching_activity_major_education_coverage_by_hospital_year
    教学活动_已招录学生教学活动记录和轮转情况按医院按年统计表	dws_teaching_activity_admission_students_activity_round_count_by_hospital_year
    教学活动_轮转学员教学活动情况按医院按月统计表	dws_teaching_activity_round_student_activity_count_by_hospital_month
    教学活动_轮转学员教学活动记录异常的人数按医院按月统计表	dws_teaching_activity_round_abnormal_student_count_by_hospital_month
    教学活动_全科专业和门诊教学相关指标按医院按月统计表	dws_teaching_activity_general_practice_outpatient_education_by_hospital_month
    教学活动_名称不规范相关指标按医院按专业按月统计表	dws_teaching_activity_name_irregular_count_by_hospital_major_month
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
    hospital_id            INT COMMENT '医院ID',
    hospital_name          STRING COMMENT '医院名称',
    year                   INT COMMENT '统计年份',
    activity_count         INT COMMENT '该年该医院开展的教学活动数量',
    total_person_times     INT COMMENT '该年该医院总的参与人次',
    student_count          INT COMMENT '该年该医院参与的学员数',
    sign_count             INT COMMENT '该年该医院签到次数',
    total_sign_count       INT COMMENT '该年该医院应签到次数',
    evaluation_count       INT COMMENT '该年该医院评价次数',
    total_evaluation_count INT COMMENT '该年该医院应评价总数'
) COMMENT '教学活动_数据总览按医院按年统计表';

-- 教学活动_数据总览按医院按年统计表：清空数据
TRUNCATE TABLE dws_hainan_hospital_info.dws_teaching_activity_data_overview_by_hospital_year;

-- 教学活动_数据总览按医院按年统计表：插入数据
INSERT INTO dws_hainan_hospital_info.dws_teaching_activity_data_overview_by_hospital_year
SELECT ac.activity_hospital_id   AS hospital_id
     , ac.activity_hospital_name AS hospital_name
     , ac.activity_year          AS year
     , activity_count
     , total_person_times
     , student_count
     , sign_count
     , total_sign_count
     , evaluation_count
     , total_evaluation_count
FROM (
         -- 教学活动明细的数据总览
         SELECT activity_hospital_id
              , activity_hospital_name
              , year(activity_start_time)       AS activity_year
              , count(DISTINCT activity_id)     AS activity_count
              , sum(activity_participant_count) AS total_person_times
         FROM dwd_hainan_hospital_info.dwd_teaching_activity_detail_df
         WHERE year(activity_start_time) >= 2019
         GROUP BY activity_hospital_id, activity_hospital_name, year(activity_start_time)) ac
         LEFT JOIN (
    -- 教学活动的学员数据总览
    SELECT student_hospital_id
         , student_hospital_name
         , year(activity_start_time)                                        AS activity_year
         , count(DISTINCT student_name)                                     AS student_count
         , count(`if`(activity_is_signed = '已签到', 1, NULL))              AS sign_count
         , count(activity_is_signed)                                        AS total_sign_count
         , count(`if`(activity_evaluation_score_rate IS NOT NULL, 1, NULL)) AS evaluation_count
         , count(1)                                                         AS total_evaluation_count
    FROM dwd_hainan_hospital_info.dwd_teaching_activity_student_log_wide_df
    WHERE year(activity_start_time) >= 2019
    GROUP BY student_hospital_id, student_hospital_name, year(activity_start_time)) sc
                   ON ac.activity_hospital_id = sc.student_hospital_id
                       AND ac.activity_hospital_name = sc.student_hospital_name AND ac.activity_year = sc.activity_year;

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
    is_standard        INT COMMENT '是否达到标准（1-达到标准；0-未达到标准）'
) COMMENT '教学活动_开展数量按医院按专业按类型按月统计表';

-- 教学活动_开展数量按医院按专业按类型按月统计表：清空数据
TRUNCATE TABLE dws_hainan_hospital_info.dws_teaching_activity_count_by_hospital_major_type_month;

-- 教学活动_开展数量按医院按专业按类型按月统计表：插入数据
INSERT INTO dws_hainan_hospital_info.dws_teaching_activity_count_by_hospital_major_type_month
SELECT ad.activity_hospital_id                                                      AS hospital_id
     , ad.activity_hospital_name                                                    AS hospital_name
     , ad.activity_major_id                                                         AS major_id
     , ad.activity_major_name                                                       AS major_name
     , ad.activity_type_id
     , ad.activity_type_name
     , ad.month
     , ad.activity_count
     , st.standard_count
     -- 1-达到标准；0-未达到标准
     , `if`(activity_count >= st.standard_count OR st.standard_count IS NULL, 1, 0) AS is_standard
FROM (SELECT activity_hospital_id
           , activity_hospital_name
           , activity_major_id
           , activity_major_name
           , activity_type_id
           , activity_type_name
           , date_format(activity_start_time, 'yyyy-MM') AS month
           , count(DISTINCT activity_id)                 AS activity_count
      FROM dwd_hainan_hospital_info.dwd_teaching_activity_detail_df
      WHERE year(activity_start_time) >= 2019
      GROUP BY activity_hospital_id, activity_hospital_name, activity_major_id, activity_major_name, activity_type_id, activity_type_name
             , date_format(activity_start_time, 'yyyy-MM')) ad
         LEFT JOIN dim_hainan_hospital_info.dim_teacher_activity_count_standard_by_hospital_major st
                   ON ad.activity_major_id = st.major_id
                       AND ad.activity_major_name = st.major_name
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
SELECT admission_year                                                             AS year
     , count(DISTINCT admission_hospital_id)                                      AS hospital_admitted_count
     , count(DISTINCT `if`(activity_id IS NOT NULL, admission_hospital_id, NULL)) AS hospital_education_count
     , concat(round(count(DISTINCT `if`(activity_id IS NOT NULL, admission_hospital_id, NULL)) * 100 / count(DISTINCT admission_hospital_id), 2),
              '%')                                                                AS coverage_education
FROM dwd_hainan_hospital_info.dwd_teaching_activity_admission_hospital_education_detail_wide_df
WHERE admission_year >= 2019
GROUP BY admission_year;

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
SELECT admission_hospital_id                                                   AS hospital_id
     , admission_hospital_name                                                 AS hospital_name
     , admission_year                                                          AS year
     , count(DISTINCT admission_major_id)                                      AS hospital_admitted_count
     , count(DISTINCT `if`(activity_id IS NOT NULL, admission_major_id, NULL)) AS hospital_education_count
     , concat(round(count(DISTINCT `if`(activity_id IS NOT NULL, admission_major_id, NULL)) * 100 / count(DISTINCT admission_major_id), 2),
              '%')                                                             AS coverage_education
FROM dwd_hainan_hospital_info.dwd_teaching_activity_admission_hospital_major_education_detail_wide_df
WHERE admission_year >= 2019
GROUP BY admission_hospital_id, admission_hospital_name, admission_year;

/*
中文表名：教学活动_已招录学生教学活动记录和轮转情况按医院按年统计表
数据库表名：dws_teaching_activity_admission_students_activity_round_count_by_hospital_year
源数据表：
    - dwd_hainan_hospital_info.dwd_hainan_admission_data_view_students
    - dwd_hainan_hospital_info.dwd_teaching_activity_student_log_wide_df
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
    round_student_count                INT COMMENT '有轮转的学生数（在招录名单）',
    no_round_student_count             INT COMMENT '无轮转的学生数（在招录名单）',
    activity_student_count             INT COMMENT '有教学活动的学生数（在招录名单）',
    no_activity_student_count          INT COMMENT '无教学活动的学生数（在招录名单）',
    round_no_activity_student_count    INT COMMENT '有轮转无教学活动的学生数（在招录名单）',
    round_activity_student_count       INT COMMENT '有轮转有教学活动的学生数（在招录名单）',
    no_round_no_activity_student_count INT COMMENT '无轮转无教学活动的学生数（在招录名单）'
) COMMENT '教学活动_已招录学生教学活动记录和轮转情况按医院按年统计表';

-- 教学活动_已招录学生教学活动记录和轮转情况按医院按年统计表：清空数据
TRUNCATE TABLE dws_hainan_hospital_info.dws_teaching_activity_admission_students_activity_round_count_by_hospital_year;

-- 教学活动_已招录学生教学活动记录和轮转情况按医院按年统计表：插入数据
INSERT INTO dws_hainan_hospital_info.dws_teaching_activity_admission_students_activity_round_count_by_hospital_year
SELECT ad.admission_hospital_id                                   AS hospital_id
     , ad.admission_hospital_name                                 AS hospital_name
     , ad.admission_year                                          AS year
     , count(DISTINCT ad.admission_student_name)                  AS total_admitted_student_count
     , count(DISTINCT CASE
                          WHEN ro.round_student_name IS NOT NULL
                              THEN ad.admission_student_name END) AS round_student_count
     , count(DISTINCT CASE
                          WHEN ro.round_student_name IS NULL
                              THEN ad.admission_student_name END) AS no_round_student_count
     , count(DISTINCT CASE
                          WHEN ac.activity_student_name IS NOT NULL
                              THEN ad.admission_student_name END) AS activity_student_count
     , count(DISTINCT CASE
                          WHEN ac.activity_student_name IS NULL
                              THEN ad.admission_student_name END) AS no_activity_student_count
     , count(DISTINCT CASE
                          WHEN ro.round_student_name IS NOT NULL AND ac.activity_student_name IS NULL
                              THEN ad.admission_student_name END) AS round_no_activity_student_count
     , count(DISTINCT CASE
                          WHEN ro.round_student_name IS NOT NULL AND ac.activity_student_name IS NOT NULL
                              THEN ad.admission_student_name END) AS round_activity_student_count
     , count(DISTINCT CASE
                          WHEN ro.round_student_name IS NULL AND ac.activity_student_name IS NULL
                              THEN ad.admission_student_name END) AS no_round_no_activity_student_count
FROM (
         -- 招录的学生数据(不能用id，因为两张表的ID不一样，后面无法关联)
         SELECT DISTINCT training_base_id  AS admission_hospital_id
                       , training_base     AS admission_hospital_name
                       , specialty_base_id AS admission_major_id
                       , specialty_base    AS admission_major_name
                       , admission_year
                       , student_name      AS admission_student_name
         FROM dwd_hainan_hospital_info.dwd_hainan_admission_data_view_students) ad
         LEFT JOIN (
    -- 轮转的学生数据(有哪些学生，因为没有student_id所有用其它字段判断）
    SELECT DISTINCT CASE
                        WHEN hospital = '301海南医院' THEN 130
                        WHEN hospital = '海南医学院第一附属医院' THEN 110
                        WHEN hospital = '三亚中心医院（海南省第三人民医院）' THEN 111
                        ELSE -1
        END                        AS round_hospital_id
                  , CASE
                        WHEN hospital = '301海南医院' THEN '解放军总医院海南医院'
                        WHEN hospital = '海南医学院第一附属医院' THEN '海南医学院第一附属医院'
                        WHEN hospital = '三亚中心医院（海南省第三人民医院）' THEN '三亚中心医院（海南省第三人民医院）'
                        ELSE '其他'
        END                        AS round_hospital_name
                  , spt_major_id   AS round_student_major_id
                  , spt_major_name AS round_student_major_name
                  , personname     AS round_student_name
                  , gradeyear      AS round_admission_year
    FROM dwd_hainan_hospital_info.dwd_hainan_round_spt_round_total_info) ro
                   ON ad.admission_hospital_id = ro.round_hospital_id
                       AND ad.admission_hospital_name = ro.round_hospital_name
                       AND ad.admission_major_id = ro.round_student_major_id
                       AND ad.admission_major_name = ro.round_student_major_name
                       AND ad.admission_student_name = ro.round_student_name
                       AND ad.admission_year = ro.round_admission_year
         LEFT JOIN(
    -- 获取有教学活动的学生数据（保留教学活动这边ID为主）
    SELECT DISTINCT student_hospital_id    AS activity_student_hospital_id
                  , student_hospital_name  AS activity_student_hospital_name
                  , student_major_id       AS activity_student_major_id
                  , student_major_name     AS activity_student_major_name
                  , student_id             AS activity_student_id
                  , student_name           AS activity_student_name
                  , student_admission_year AS activity_student_admission_year
    FROM dwd_hainan_hospital_info.dwd_teaching_activity_student_log_wide_df) ac
                  ON ad.admission_hospital_id = ac.activity_student_hospital_id
                      AND ad.admission_hospital_name = ac.activity_student_hospital_name
                      AND ad.admission_major_id = ac.activity_student_major_id
                      AND ad.admission_major_name = ac.activity_student_major_name
                      AND ad.admission_student_name = ac.activity_student_name
                      AND ad.admission_year = ac.activity_student_admission_year
WHERE ad.admission_year >= 2019
GROUP BY ad.admission_hospital_id, ad.admission_hospital_name, ad.admission_year;

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
    hospital_id                    INT COMMENT '医院ID',
    hospital_name                  STRING COMMENT '医院名称',
    student_major_id               INT COMMENT '学生专业ID',
    student_major_name             STRING COMMENT '学生专业名称',
    student_id                     INT COMMENT '学生ID',
    student_name                   STRING COMMENT '学生姓名',
    student_admission_year         INT COMMENT '学生入学年份',
    rotation_office_name           STRING COMMENT '轮转科室名称',
    rotation_office_major_id       INT COMMENT '轮转科室专业ID',
    rotation_office_major_name     STRING COMMENT '轮转科室专业名称',
    rotation_teacher_name          STRING COMMENT '轮转老师姓名',
    rotation_start_time            TIMESTAMP COMMENT '轮转开始时间',
    rotation_end_time              TIMESTAMP COMMENT '轮转结束时间',
    rotation_month                 STRING COMMENT '轮转月份',
    activity_count                 INT COMMENT '教学活动数量',
    same_major_activity_count      INT COMMENT '当期轮转与教学活动同专业教学活动数量',
    different_major_activity_count INT COMMENT '当期轮转与教学活动不同专业教学活动数量'
) COMMENT '教学活动_轮转学员教学活动情况按医院按月统计表';

-- 教学活动_轮转学员教学活动情况按医院按月统计表：清空数据
TRUNCATE TABLE dws_hainan_hospital_info.dws_teaching_activity_round_student_activity_count_by_hospital_month;

-- 教学活动_轮转学员教学活动情况按医院按月统计表：插入数据
INSERT INTO dws_hainan_hospital_info.dws_teaching_activity_round_student_activity_count_by_hospital_month
SELECT rotation_hospital_id                                                                              AS hospital_id
     , rotation_hospital_name                                                                            AS hospital_name
     , rotation_student_major_id                                                                         AS student_major_id
     , rotation_student_major_name                                                                       AS student_major_name
     , rotation_student_id                                                                               AS student_id
     , rotation_student_name                                                                             AS student_name
     , rotation_student_admission_year                                                                   AS student_admission_year
     , rotation_office_name
     , rotation_office_major_id
     , rotation_office_major_name
     , rotation_teacher_name
     , rotation_start_time
     , rotation_end_time
     , rotation_month
     , count(DISTINCT activity_id)                                                                       AS activity_count
     , count(DISTINCT CASE
                          WHEN rotation_office_major_id = activity_major_id
                              AND rotation_office_major_name = activity_major_name THEN activity_id END) AS same_major_activity_count
     , count(DISTINCT CASE
                          WHEN rotation_office_major_id != activity_major_id
                              OR rotation_office_major_name != activity_major_name THEN activity_id END) AS different_major_activity_count
FROM dwd_hainan_hospital_info.dwd_teaching_activity_round_student_detail_wide_df
WHERE rotation_month >= '2019-01'
GROUP BY rotation_hospital_id
       , rotation_hospital_name
       , rotation_student_major_id
       , rotation_student_major_name
       , rotation_student_id
       , rotation_student_name
       , rotation_student_admission_year
       , rotation_office_name
       , rotation_office_major_id
       , rotation_office_major_name
       , rotation_teacher_name
       , rotation_start_time
       , rotation_end_time
       , rotation_month;

/*
中文表名：教学活动_轮转学员教学活动记录异常的人数按医院按月统计表
数据库表名：dws_teaching_activity_round_abnormal_student_count_by_hospital_month
源数据表：
    - dws_hainan_hospital_info.dws_teaching_activity_round_student_activity_count_by_hospital_month
*/

-- 教学活动_轮转学员教学活动记录异常的人数按医院按月统计表：删除旧的（如果存在）
DROP TABLE IF EXISTS dws_hainan_hospital_info.dws_teaching_activity_round_abnormal_student_count_by_hospital_month;

-- 教学活动_轮转学员教学活动记录异常的人数按医院按月统计表：创建
CREATE TABLE dws_hainan_hospital_info.dws_teaching_activity_round_abnormal_student_count_by_hospital_month
(
    hospital_id               INT COMMENT '医院ID',
    hospital_name             STRING COMMENT '医院名称',
    month                     STRING COMMENT '月份',
    total_round_student_count INT COMMENT '该月总轮转学员数',
    no_activity_student_count INT COMMENT '该月没有教学活动记录的学员数',
    no_major_student_count    INT COMMENT '该月不在本轮转专业上课的学员数'
) COMMENT '教学活动_轮转学员教学活动记录异常的人数按医院按月统计表';

-- 教学活动_轮转学员教学活动记录异常的人数按医院按月统计表：清空数据
TRUNCATE TABLE dws_hainan_hospital_info.dws_teaching_activity_round_abnormal_student_count_by_hospital_month;

-- 教学活动_轮转学员教学活动记录异常的人数按医院按月统计表：插入数据
INSERT INTO dws_hainan_hospital_info.dws_teaching_activity_round_abnormal_student_count_by_hospital_month
SELECT hospital_id
     , hospital_name
     , rotation_month                                                               AS month
     -- 这里用name，因为原表在补充ID后还有一些人依旧为null
     , count(DISTINCT student_name)                                                 AS total_round_student_count
     , count(DISTINCT CASE
                          WHEN activity_count = 0 THEN student_name END)            AS no_activity_student_count
     , count(DISTINCT CASE
                          WHEN same_major_activity_count = 0 THEN student_name END) AS no_major_student_count
FROM dws_hainan_hospital_info.dws_teaching_activity_round_student_activity_count_by_hospital_month
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
     , sum(CASE
               WHEN major_id = 18 AND major_name = '全科医学科' AND activity_type_id = 22 AND activity_type_name = '门诊教学' THEN activity_count
               ELSE 0 END) AS general_practice_outpatient_count
     , CASE
           WHEN sum(CASE
                        WHEN major_id = 18 AND major_name = '全科医学科' AND activity_type_id = 22 AND activity_type_name = '门诊教学'
                            THEN activity_count
                        ELSE 0 END) > 0 THEN 1
           ELSE 0 END      AS is_general_practice_outpatient
     , sum(CASE
               WHEN major_id != 18 AND major_name != '全科医学科' AND activity_type_id = 22 AND activity_type_name = '门诊教学' THEN major_id
               ELSE 0 END) AS outpatient_teaching_specialties
     , CASE
           WHEN sum(CASE
                        WHEN major_id != 18 AND major_name != '全科医学科' AND activity_type_id = 22 AND activity_type_name = '门诊教学' THEN major_id
                        ELSE 0 END) >= 2 THEN 1
           ELSE 0 END      AS is_outpatient_teaching_specialties_valid
FROM dws_hainan_hospital_info.dws_teaching_activity_count_by_hospital_major_type_month
WHERE month >= '2019-01'
GROUP BY hospital_id, hospital_name, month;

/*
中文表名：教学活动_名称不规范相关指标按医院按专业按月统计表
数据库表名：dws_teaching_activity_name_irregular_count_by_hospital_major_month
源数据表：
    - dwd_hainan_hospital_info.dwd_teaching_activity_name_abnormal_detail_wide_df
*/

-- 教学活动_名称不规范相关指标按医院按专业按月统计表：删除旧的（如果存在）
DROP TABLE IF EXISTS dws_hainan_hospital_info.dws_teaching_activity_name_irregular_count_by_hospital_major_month;

-- 教学活动_名称不规范相关指标按医院按专业按月统计表：创建
CREATE TABLE dws_hainan_hospital_info.dws_teaching_activity_name_irregular_count_by_hospital_major_month
(
    hospital_id                          INT COMMENT '医院ID',
    hospital_name                        STRING COMMENT '医院名称',
    major_id                             INT COMMENT '专业ID',
    major_name                           STRING COMMENT '专业名称',
    month                                STRING COMMENT '月份',
    activity_count                       INT COMMENT '该月该专业开展的教学活动数量',
    standard_count                       INT COMMENT '该月该专业满足规范的教学活动数量',
    single_activity_same_type_name_count INT COMMENT '该月该专业单教学活动内名称与类型同命名数量（命名重复）',
    different_activity_same_name_count   INT COMMENT '该月该专业不同教学活动名称相同相同数量（短期内相似主题）'
) COMMENT '教学活动_名称不规范相关指标按医院按专业按月统计表';

-- 教学活动_名称不规范相关指标按医院按专业按月统计表：清空数据
TRUNCATE TABLE dws_hainan_hospital_info.dws_teaching_activity_name_irregular_count_by_hospital_major_month;

-- 教学活动_名称不规范相关指标按医院按专业按月统计表：插入数据
INSERT INTO dws_hainan_hospital_info.dws_teaching_activity_name_irregular_count_by_hospital_major_month
SELECT activity_hospital_id                                          AS hospital_id
     , activity_hospital_name                                        AS hospital_name
     , activity_major_id                                             AS major_id
     , activity_major_name                                           AS major_name
     , activity_month                                                AS month
     , count(DISTINCT activity_id)                                   AS activity_count
     -- 规范类型
     -- 0-规范，1-单教学活动内名称与类型同命名（命名重复），2-不同教学活动名称相同相同（短期内相似主题）
     , count(DISTINCT if(name_abnormal_type = 0, activity_id, NULL)) AS standard_count
     , count(DISTINCT if(name_abnormal_type = 1, activity_id, NULL)) AS single_activity_same_type_name_count
     , count(DISTINCT if(name_abnormal_type = 2, activity_id, NULL)) AS different_activity_same_name_count
FROM dwd_hainan_hospital_info.dwd_teaching_activity_name_abnormal_detail_wide_df
WHERE activity_month >= '2019-01'
GROUP BY activity_hospital_id, activity_hospital_name, activity_major_id, activity_major_name, activity_month;