/*===================================================================
    dwd层数据整理：
        将多个医院的教学活动信息汇总到统一的事实表中，并进行清洗。
        拉几张宽表，方便后续的数据分析。

    目前已有医院数据：
        hainan_301              130     解放军总医院海南医院
        hainan_first_affiliated   110     海南医学院第一附属医院
        hainan_sanya_central    111     三亚中心医院（海南省第三人民医院）

    目前已有表：
        -- 从ods拉取原数据并清洗 --
		教学活动_活动明细表	dwd_teaching_activity_detail_df
		教学活动_参与人员明细表	dwd_teaching_activity_person_detail_df
		教学活动_附件明细表	dwd_teaching_activity_file_detail_df

        -- 基于清洗后的数据拉的宽表 --
		教学活动_参与学生明细宽表	dwd_teaching_activity_student_detail_wide_df
		教学活动_学员参与日志宽表	dwd_teaching_activity_student_log_wide_df
		教学活动_参与老师明细宽表	dwd_teaching_activity_teacher_detail_wide_df
		教学活动_教师参与日志宽表	dwd_teaching_activity_teacher_log_wide_df
		教学活动_有招录医院的入院教育明细宽表	dwd_teaching_activity_admission_hospital_education_detail_wide_df
		教学活动_有招录医院专业基地的入专业教育明细宽表	dwd_teaching_activity_admission_hospital_major_education_detail_wide_df
		教学活动_轮转学员当月教学活动明细宽表	dwd_teaching_activity_round_student_detail_wide_df
		教学活动_名称不规范明细宽表	dwd_teaching_activity_name_abnormal_detail_wide_df
===================================================================*/

/*
设置Hive执行引擎和MapReduce相关参数
SET hive.execution.engine=mr;
SET mapreduce.task.io.sort.mb = 1000;
*/

-- 确保目标数据库存在
CREATE DATABASE IF NOT EXISTS dwd_hainan_hospital_info;

/*
中文表名：教学活动_活动明细表
数据库表名：dwd_teaching_activity_detail_df
源数据表：
    - ods_hainan_hospital_info.ods_hainan_teaching_activity_spt_train_sheet_data_301
    - ods_hainan_hospital_info.ods_hainan_teaching_activity_spt_train_sheet_data_first_affiliated
    - ods_hainan_hospital_info.ods_hainan_teaching_activity_spt_train_sheet_data_sanya_central
    - dim_hainan_hospital_info.dim_teacher_activity_count_standard_by_hospital_major
*/

-- 教学活动_活动明细表：删除旧的（如果存在）
DROP TABLE IF EXISTS dwd_hainan_hospital_info.dwd_teaching_activity_detail_df;

-- 教学活动_活动明细表：创建
CREATE TABLE dwd_hainan_hospital_info.dwd_teaching_activity_detail_df
(
    activity_id                STRING COMMENT '教学活动ID',
    activity_name              STRING COMMENT '教学活动名称',
    activity_type_id           INT COMMENT '教学活动类型ID',
    activity_type_name         STRING COMMENT '教学活动类型名称',
    activity_hospital_id       INT COMMENT '教学活动医院ID',
    activity_hospital_name     STRING COMMENT '教学活动医院名称',
    activity_major_id          INT COMMENT '教学活动专业ID',
    activity_major_name        STRING COMMENT '教学活动专业名称',
    activity_office_name       STRING COMMENT '教学活动所属科室',
    activity_start_time        TIMESTAMP COMMENT '教学活动开始时间',
    activity_end_time          TIMESTAMP COMMENT '教学活动结束时间',
    activity_duration_minutes  INT COMMENT '教学活动时长（分钟）',
    activity_participant_count INT COMMENT '教学活动参与人数'
) COMMENT '教学活动_活动明细表';

-- 教学活动_活动明细表：清空数据
TRUNCATE TABLE dwd_hainan_hospital_info.dwd_teaching_activity_detail_df;

-- 教学活动_活动明细表：插入数据
INSERT INTO dwd_hainan_hospital_info.dwd_teaching_activity_detail_df
SELECT DISTINCT concat(activity_hospital_id, '_', trainid)                                                                       AS activity_id
              , activity_name
              , activity_type_id
              , activity_type_name
              , activity_hospital_id
              , activity_hospital_name
              , if(activity_major_id = -1 AND major_dic.major_id IS NOT NULL, major_dic.major_id, activity_major_id)             AS activity_major_id
              , if(activity_major_name = '未知' AND major_dic.major_name IS NOT NULL, major_dic.major_name,
                   activity_major_name)                                                                                          AS activity_major_name
              , activity_office_name
              , activity_start_time
              , activity_end_time
              , activity_duration_minutes
              , activity_participant_count
FROM (
         -- 数据源 1: 130-解放军总医院海南医院
         SELECT 130                                                                                    AS activity_hospital_id
              , '解放军总医院海南医院'                                                                 AS activity_hospital_name
              , trainid
              , regexp_replace(title, '[\r\n]', ' ')                                                   AS activity_name
              , coalesce(spt_traintype_id, -1)                                                         AS activity_type_id
              , coalesce(nullif(trim(regexp_replace(spt_traintype_name, '[\r\n"]', ' ')), ''), '未知') AS activity_type_name
              , coalesce(spt_major_id, -1)                                                             AS activity_major_id
              , coalesce(nullif(trim(regexp_replace(spt_major_name, '[\r\n"]', ' ')), ''), '未知')     AS activity_major_name
              , coalesce(nullif(trim(regexp_replace(officename, '[\r\n"]', ' ')), ''), '未知')         AS activity_office_name
              , starttime                                                                              AS activity_start_time
              , endtime                                                                                AS activity_end_time
              , teaching_activity_duration                                                             AS activity_duration_minutes
              , participant_count                                                                      AS activity_participant_count
         FROM ods_hainan_hospital_info.ods_hainan_teaching_activity_spt_train_sheet_data_301
         WHERE trainid IS NOT NULL
           AND trainid >= 0
           AND (spt_traintype_id IS NOT NULL OR spt_major_id IS NOT NULL)
         UNION ALL
         -- 数据源 2: 110-海南医学院第一附属医院
         SELECT 110                                                                                    AS activity_hospital_id
              , '海南医学院第一附属医院'                                                               AS activity_hospital_name
              , trainid
              , regexp_replace(title, '[\r\n]', ' ')                                                   AS activity_name
              , coalesce(spt_traintype_id, -1)                                                         AS activity_type_id
              , coalesce(nullif(trim(regexp_replace(spt_traintype_name, '[\r\n"]', ' ')), ''), '未知') AS activity_type_name
              , coalesce(spt_major_id, -1)                                                             AS activity_major_id
              , coalesce(nullif(trim(regexp_replace(spt_major_name, '[\r\n"]', ' ')), ''), '未知')     AS activity_major_name
              , coalesce(nullif(trim(regexp_replace(officename, '[\r\n"]', ' ')), ''), '未知')         AS activity_office_name
              , starttime                                                                              AS activity_start_time
              , endtime                                                                                AS activity_end_time
              , teaching_activity_duration                                                             AS activity_duration_minutes
              , participant_count                                                                      AS activity_participant_count
         FROM ods_hainan_hospital_info.ods_hainan_teaching_activity_spt_train_sheet_data_first_affiliated
         WHERE trainid IS NOT NULL
           AND trainid >= 0
         UNION ALL
         -- 数据源 3: 111-三亚中心医院（海南省第三人民医院）
         SELECT 111                                                                                    AS activity_hospital_id
              , '三亚中心医院（海南省第三人民医院）'                                                     AS activity_hospital_name
              , trainid
              , regexp_replace(title, '[\r\n]', ' ')                                                   AS activity_name
              , coalesce(spt_traintype_id, -1)                                                         AS activity_type_id
              , coalesce(nullif(trim(regexp_replace(spt_traintype_name, '[\r\n"]', ' ')), ''), '未知') AS activity_type_name
              , coalesce(spt_major_id, -1)                                                             AS activity_major_id
              , coalesce(nullif(trim(regexp_replace(spt_major_name, '[\r\n"]', ' ')), ''), '未知')     AS activity_major_name
              , coalesce(nullif(trim(regexp_replace(officename, '[\r\n"]', ' ')), ''), '未知')         AS activity_office_name
              , starttime                                                                              AS activity_start_time
              , endtime                                                                                AS activity_end_time
              , teaching_activity_duration                                                             AS activity_duration_minutes
              , participant_count                                                                      AS activity_participant_count
         FROM ods_hainan_hospital_info.ods_hainan_teaching_activity_spt_train_sheet_data_sanya_central
         WHERE trainid IS NOT NULL
           AND trainid >= 0) AS combined_data
         -- 初步解决没有专业信息的问题
         LEFT JOIN (SELECT major_id, major_name
                    FROM dim_hainan_hospital_info.dim_teacher_activity_count_standard_by_hospital_major
                    GROUP BY major_id, major_name) major_dic
                   ON combined_data.activity_office_name = major_dic.major_name;

/*
中文表名：教学活动_参与人员明细表
数据库表名：dwd_teaching_activity_person_detail_df
源数据表：
    - ods_hainan_hospital_info.ods_hainan_teaching_activity_spt_train_person_data_301
    - ods_hainan_hospital_info.ods_hainan_teaching_activity_spt_train_person_data_first_affiliated
    - ods_hainan_hospital_info.ods_hainan_teaching_activity_spt_train_person_data_sanya_central
*/

-- 教学活动_参与人员明细表：删除旧的（如果存在）
DROP TABLE IF EXISTS dwd_hainan_hospital_info.dwd_teaching_activity_person_detail_df;

-- 教学活动_参与人员明细表：创建
CREATE TABLE dwd_hainan_hospital_info.dwd_teaching_activity_person_detail_df
(
    activity_id            STRING COMMENT '教学活动ID',
    activity_hospital_id   INT COMMENT '所属医院ID',
    activity_hospital_name STRING COMMENT '所属医院',
    person_id              INT COMMENT '人员ID',
    person_name            STRING COMMENT '人员名字',
    person_type            STRING COMMENT '人员类型',
    professional_title     STRING COMMENT '专业职称',
    evaluation_score_rate  DOUBLE COMMENT '评价得分率',
    is_signed              STRING COMMENT '是否签到'
) COMMENT '教学活动_参与人员明细表';

-- 教学活动_参与人员明细表：清空数据
TRUNCATE TABLE dwd_hainan_hospital_info.dwd_teaching_activity_person_detail_df;

-- 教学活动_参与人员明细表：插入数据
INSERT INTO dwd_hainan_hospital_info.dwd_teaching_activity_person_detail_df
SELECT DISTINCT concat(activity_hospital_id, '_', trainid) AS activity_id
              , activity_hospital_id
              , activity_hospital_name
              , person_id
              , person_name
              , person_type
              , professional_title
              , evaluation_score_rate
              , is_signed
FROM (
         -- 数据源 1: 130-解放军总医院海南医院
         SELECT 130                                                                                   AS activity_hospital_id
              , '解放军总医院海南医院'                                                                AS activity_hospital_name
              , trainid
              , coalesce(personid, -1)                                                                AS person_id
              , coalesce(nullif(trim(regexp_replace(personname, '[\r\n"]', ' ')), ''), '未知')        AS person_name
              , coalesce(nullif(trim(regexp_replace(persontype, '[\r\n"]', ' ')), ''), '未知')        AS person_type
              , coalesce(nullif(trim(regexp_replace(professionaltitle, '[\r\n"]', ' ')), ''), '未知') AS professional_title
              , evalua_score_rate                                                                     AS evaluation_score_rate
              , coalesce(nullif(trim(regexp_replace(is_sign, '[\r\n"]', ' ')), ''), '未知')           AS is_signed
         FROM ods_hainan_hospital_info.ods_hainan_teaching_activity_spt_train_person_data_301
         WHERE trainid IS NOT NULL
           AND trainid >= 0
         UNION ALL
         -- 数据源 2: 110-海南医学院第一附属医院
         SELECT 110                                                                                   AS activity_hospital_id
              , '海南医学院第一附属医院'                                                              AS activity_hospital_name
              , trainid
              , coalesce(personid, -1)                                                                AS person_id
              , coalesce(nullif(trim(regexp_replace(personname, '[\r\n"]', ' ')), ''), '未知')        AS person_name
              , coalesce(nullif(trim(regexp_replace(persontype, '[\r\n"]', ' ')), ''), '未知')        AS person_type
              , coalesce(nullif(trim(regexp_replace(professionaltitle, '[\r\n"]', ' ')), ''), '未知') AS professional_title
              , evalua_score_rate                                                                     AS evaluation_score_rate
              , coalesce(nullif(trim(regexp_replace(is_sign, '[\r\n"]', ' ')), ''), '未知')           AS is_signed
         FROM ods_hainan_hospital_info.ods_hainan_teaching_activity_spt_train_person_data_first_affiliated
         WHERE trainid IS NOT NULL
           AND trainid >= 0
         UNION ALL
         -- 数据源 3: 111-三亚中心医院（海南省第三人民医院）
         SELECT 111                                                                                   AS activity_hospital_id
              , '三亚中心医院（海南省第三人民医院）'                                                    AS activity_hospital_name
              , trainid
              , coalesce(personid, -1)                                                                AS person_id
              , coalesce(nullif(trim(regexp_replace(personname, '[\r\n"]', ' ')), ''), '未知')        AS person_name
              , coalesce(nullif(trim(regexp_replace(persontype, '[\r\n"]', ' ')), ''), '未知')        AS person_type
              , coalesce(nullif(trim(regexp_replace(professionaltitle, '[\r\n"]', ' ')), ''), '未知') AS professional_title
              , evalua_score_rate                                                                     AS evaluation_score_rate
              , coalesce(nullif(trim(regexp_replace(is_sign, '[\r\n"]', ' ')), ''), '未知')           AS is_signed
         FROM ods_hainan_hospital_info.ods_hainan_teaching_activity_spt_train_person_data_sanya_central
         WHERE trainid IS NOT NULL
           AND trainid >= 0) AS combined_data;

/*
中文表名：教学活动_附件明细表
数据库表名：dwd_teaching_activity_file_detail_df
源数据表：
    - ods_hainan_hospital_info.ods_hainan_teaching_activity_spt_train_file_info_301
    - ods_hainan_hospital_info.ods_hainan_teaching_activity_spt_train_file_info_first_affiliated
    - ods_hainan_hospital_info.ods_hainan_teaching_activity_spt_train_file_info_sanya_central
*/

-- 教学活动_附件明细表：删除旧的（如果存在）
DROP TABLE IF EXISTS dwd_hainan_hospital_info.dwd_teaching_activity_file_detail_df;

-- 教学活动_附件明细表：创建
CREATE TABLE dwd_hainan_hospital_info.dwd_teaching_activity_file_detail_df
(
    activity_id            STRING COMMENT '教学活动ID',
    activity_hospital_id   INT COMMENT '所属医院ID',
    activity_hospital_name STRING COMMENT '所属医院',
    file_type              STRING COMMENT '附件类型',
    file_url               STRING COMMENT '附件URL',
    file_name              STRING COMMENT '附件名称'
) COMMENT '教学活动_附件明细表';

-- 教学活动_附件明细表：清空数据
TRUNCATE TABLE dwd_hainan_hospital_info.dwd_teaching_activity_file_detail_df;

-- 教学活动_附件明细表：插入数据
INSERT INTO dwd_hainan_hospital_info.dwd_teaching_activity_file_detail_df
SELECT DISTINCT concat(activity_hospital_id, '_', trainid) AS activity_id
              , activity_hospital_id
              , activity_hospital_name
              , file_type
              , file_url
              , file_name
FROM (
         -- 数据源 1: 130-解放军总医院海南医院
         SELECT 130                                                                          AS activity_hospital_id
              , '解放军总医院海南医院'                                                       AS activity_hospital_name
              , trainid
              , coalesce(nullif(trim(regexp_replace(filetype, '[\r\n"]', ' ')), ''), '未知') AS file_type
              , trim(regexp_replace(url, '[\r\n"]', ' '))                                    AS file_url
              , trim(regexp_replace(reffilename, '[\r\n"]', ' '))                            AS file_name
         FROM ods_hainan_hospital_info.ods_hainan_teaching_activity_spt_train_file_info_301
         WHERE trainid IS NOT NULL
           AND trainid >= 0
         UNION ALL
         -- 数据源 2: 110-海南医学院第一附属医院
         SELECT 110                                                                          AS activity_hospital_id
              , '海南医学院第一附属医院'                                                     AS activity_hospital_name
              , trainid
              , coalesce(nullif(trim(regexp_replace(filetype, '[\r\n"]', ' ')), ''), '未知') AS file_type
              , trim(regexp_replace(url, '[\r\n"]', ' '))                                    AS file_url
              , trim(regexp_replace(reffilename, '[\r\n"]', ' '))                            AS file_name
         FROM ods_hainan_hospital_info.ods_hainan_teaching_activity_spt_train_file_info_first_affiliated
         WHERE trainid IS NOT NULL
           AND trainid >= 0
         UNION ALL
         -- 数据源 3: 111-三亚中心医院（海南省第三人民医院）
         SELECT 111                                                                          AS activity_hospital_id
              , '三亚中心医院（海南省第三人民医院）'                                           AS activity_hospital_name
              , trainid
              , coalesce(nullif(trim(regexp_replace(filetype, '[\r\n"]', ' ')), ''), '未知') AS file_type
              , trim(regexp_replace(url, '[\r\n"]', ' '))                                    AS file_url
              , trim(regexp_replace(reffilename, '[\r\n"]', ' '))                            AS file_name
         FROM ods_hainan_hospital_info.ods_hainan_teaching_activity_spt_train_file_info_sanya_central
         WHERE trainid IS NOT NULL
           AND trainid >= 0) AS combined_data;

/*
中文表名：教学活动_参与学生明细宽表
数据库表名：dwd_teaching_activity_student_detail_wide_df
源数据表：
    - ods_hainan_hospital_info.ods_hainan_basic_spt_student_item_301
    - ods_hainan_hospital_info.ods_hainan_basic_spt_student_item_first_affiliated
    - ods_hainan_hospital_info.ods_hainan_basic_spt_student_item_sanya_central
    - dwd_hainan_hospital_info.dwd_teaching_activity_person_detail_df
*/

-- 教学活动_参与学生明细宽表：删除旧的（如果存在）
DROP TABLE IF EXISTS dwd_hainan_hospital_info.dwd_teaching_activity_student_detail_wide_df;

-- 教学活动_参与学生明细宽表：创建
CREATE TABLE dwd_hainan_hospital_info.dwd_teaching_activity_student_detail_wide_df
(
    student_id             INT COMMENT '学生ID',
    student_name           STRING COMMENT '学生名字',
    student_hospital_id    INT COMMENT '学生所属医院ID',
    student_hospital_name  STRING COMMENT '学生所属医院',
    student_major_id       INT COMMENT '学生所属专业ID',
    student_major_name     STRING COMMENT '学生所属专业名称',
    student_admission_year INT COMMENT '学生入学年份',
    student_type           STRING COMMENT '学生类型'
) COMMENT '教学活动_参与学生明细宽表';

-- 教学活动_参与学生明细宽表：清空数据
TRUNCATE TABLE dwd_hainan_hospital_info.dwd_teaching_activity_student_detail_wide_df;

-- 教学活动_参与学生明细宽表：插入数据
INSERT INTO dwd_hainan_hospital_info.dwd_teaching_activity_student_detail_wide_df
SELECT DISTINCT total_st.student_id
              , total_st.student_name
              , total_st.student_hospital_id
              , total_st.student_hospital_name
              , total_st.student_major_id
              , total_st.student_major_name
              , total_st.student_admission_year
              , total_st.student_type
FROM (
         -- item中学生信息汇总
         -- 数据源 1: 130-解放军总医院海南医院
         SELECT 130                                                                                AS student_hospital_id
              , '解放军总医院海南医院'                                                             AS student_hospital_name
              , coalesce(personid, -1)                                                             AS student_id
              , trim(regexp_replace(personname, '[\r\n"]', ' '))                                   AS student_name
              , coalesce(spt_major_id, -1)                                                         AS student_major_id
              , coalesce(nullif(trim(regexp_replace(spt_major_name, '[\r\n"]', ' ')), ''), '未知') AS student_major_name
              , gradeyear                                                                          AS student_admission_year
              , coalesce(nullif(trim(regexp_replace(value, '[\r\n"]', ' ')), ''), '未知')          AS student_type
         FROM ods_hainan_hospital_info.ods_hainan_basic_spt_student_item_301
         UNION ALL
         -- 数据源 2: 110-海南医学院第一附属医院
         SELECT 110                                                                                AS student_hospital_id
              , '海南医学院第一附属医院'                                                           AS student_hospital_name
              , coalesce(personid, -1)                                                             AS student_id
              , trim(regexp_replace(personname, '[\r\n"]', ' '))                                   AS student_name
              , coalesce(spt_major_id, -1)                                                         AS student_major_id
              , coalesce(nullif(trim(regexp_replace(spt_major_name, '[\r\n"]', ' ')), ''), '未知') AS student_major_name
              , gradeyear                                                                          AS student_admission_year
              , coalesce(nullif(trim(regexp_replace(value, '[\r\n"]', ' ')), ''), '未知')          AS student_type
         FROM ods_hainan_hospital_info.ods_hainan_basic_spt_student_item_first_affiliated
         UNION ALL
         -- 数据源 3: 111-三亚中心医院（海南省第三人民医院）
         SELECT 111                                                                                AS student_hospital_id
              , '三亚中心医院（海南省第三人民医院）'                                                 AS student_hospital_name
              , coalesce(personid, -1)                                                             AS student_id
              , trim(regexp_replace(personname, '[\r\n"]', ' '))                                   AS student_name
              , coalesce(spt_major_id, -1)                                                         AS student_major_id
              , coalesce(nullif(trim(regexp_replace(spt_major_name, '[\r\n"]', ' ')), ''), '未知') AS student_major_name
              , gradeyear                                                                          AS student_admission_year
              , coalesce(nullif(trim(regexp_replace(value, '[\r\n"]', ' ')), ''), '未知')          AS student_type
         FROM ods_hainan_hospital_info.ods_hainan_basic_spt_student_item_sanya_central) total_st
         LEFT JOIN dwd_hainan_hospital_info.dwd_teaching_activity_person_detail_df ac_pd
                   ON total_st.student_id = ac_pd.person_id
                       AND total_st.student_name = ac_pd.person_name
                       AND total_st.student_hospital_id = ac_pd.activity_hospital_id
                       AND total_st.student_hospital_name = ac_pd.activity_hospital_name
WHERE ac_pd.person_id IS NOT NULL
  AND ac_pd.person_type = '学员';

/*
中文表名：教学活动_学员参与日志宽表
数据库表名：dwd_teaching_activity_student_log_wide_df
源数据表：
    - dwd_hainan_hospital_info.dwd_teaching_activity_student_detail_wide_df
    - dwd_hainan_hospital_info.dwd_teaching_activity_person_detail_df
    - dwd_hainan_hospital_info.dwd_teaching_activity_detail_df
*/

-- 教学活动_学员参与日志宽表：删除旧的（如果存在）
DROP TABLE IF EXISTS dwd_hainan_hospital_info.dwd_teaching_activity_student_log_wide_df;

-- 教学活动_学员参与日志宽表：创建
CREATE TABLE dwd_hainan_hospital_info.dwd_teaching_activity_student_log_wide_df
(
    student_id                     INT COMMENT '学员ID',
    student_name                   STRING COMMENT '学员名字',
    student_hospital_id            INT COMMENT '学员所属医院ID',
    student_hospital_name          STRING COMMENT '学员所属医院',
    student_major_id               INT COMMENT '学员所属专业ID',
    student_major_name             STRING COMMENT '学员所属专业名称',
    student_admission_year         INT COMMENT '学员入学年份',
    student_type                   STRING COMMENT '学员类型',
    activity_id                    STRING COMMENT '教学活动ID',
    activity_name                  STRING COMMENT '教学活动名称',
    activity_type_id               INT COMMENT '教学活动类型ID',
    activity_type_name             STRING COMMENT '教学活动类型名称',
    activity_major_id              INT COMMENT '教学活动专业ID',
    activity_major_name            STRING COMMENT '教学活动专业名称',
    activity_office_name           STRING COMMENT '教学活动所属科室',
    activity_start_time            TIMESTAMP COMMENT '教学活动开始时间',
    activity_end_time              TIMESTAMP COMMENT '教学活动结束时间',
    activity_is_signed             STRING COMMENT '教学活动是否签到',
    activity_evaluation_score_rate DOUBLE COMMENT '教学活动评价得分率'
) COMMENT '教学活动_学员参与日志宽表';

-- 教学活动_学员参与日志宽表：清空数据
TRUNCATE TABLE dwd_hainan_hospital_info.dwd_teaching_activity_student_log_wide_df;

-- 教学活动_学员参与日志宽表：插入数据
INSERT INTO dwd_hainan_hospital_info.dwd_teaching_activity_student_log_wide_df
SELECT asdw.student_id
     , asdw.student_name
     , asdw.student_hospital_id
     , asdw.student_hospital_name
     , asdw.student_major_id
     , asdw.student_major_name
     , asdw.student_admission_year
     , asdw.student_type
     , ad.activity_id
     , ad.activity_name
     , ad.activity_type_id
     , ad.activity_type_name
     , ad.activity_major_id
     , ad.activity_major_name
     , ad.activity_office_name
     , ad.activity_start_time
     , ad.activity_end_time
     , apd.is_signed             AS activity_is_signed
     , apd.evaluation_score_rate AS activity_is_signed
FROM dwd_hainan_hospital_info.dwd_teaching_activity_student_detail_wide_df asdw
         LEFT JOIN dwd_hainan_hospital_info.dwd_teaching_activity_person_detail_df apd
                   ON asdw.student_id = apd.person_id AND asdw.student_name = apd.person_name
                       AND asdw.student_hospital_id = apd.activity_hospital_id AND asdw.student_hospital_name = apd.activity_hospital_name
         LEFT JOIN dwd_hainan_hospital_info.dwd_teaching_activity_detail_df ad
                   ON apd.activity_id = ad.activity_id AND apd.activity_hospital_id = ad.activity_hospital_id AND
                      apd.activity_hospital_name = ad.activity_hospital_name;


/*
中文表名：教学活动_参与老师明细宽表
数据库表名：dwd_teaching_activity_teacher_detail_wide_df
源数据表：
    - ods_hainan_hospital_info.ods_hainan_teacher_performance_spt_teacher_item_301
    - ods_hainan_hospital_info.ods_hainan_teacher_performance_spt_teacher_item_first_affiliated
    - ods_hainan_hospital_info.ods_hainan_teacher_performance_spt_teacher_item_sanya_central
    - dwd_hainan_hospital_info.dwd_teaching_activity_person_detail_df
*/
-- 教学活动_参与老师明细宽表：删除旧的（如果存在）
DROP TABLE IF EXISTS dwd_hainan_hospital_info.dwd_teaching_activity_teacher_detail_wide_df;

-- 教学活动_参与老师明细宽表：创建
CREATE TABLE dwd_hainan_hospital_info.dwd_teaching_activity_teacher_detail_wide_df
(
    teacher_id                  INT COMMENT '老师ID',
    teacher_name                STRING COMMENT '老师名字',
    teacher_hospital_id         INT COMMENT '老师所属医院ID',
    teacher_hospital_name       STRING COMMENT '老师所属医院',
    -- teacher_major_id                      INT COMMENT '老师所属专业ID',
    teacher_major_name          STRING COMMENT '老师所属专业名称',
    teacher_office_name         STRING COMMENT '老师所属科室',
    teacher_title               STRING COMMENT '老师职称',
    teacher_degree              STRING COMMENT '老师学位',
    teacher_highest_degree      STRING COMMENT '老师最高学位',
    teacher_promotion_time      TIMESTAMP COMMENT '老师晋升时间',
    teacher_phone               STRING COMMENT '老师电话',
    teacher_id_card             STRING COMMENT '老师身份证',
    teacher_is_general_practice STRING COMMENT '老师是否全科'

) COMMENT '教学活动_参与老师明细宽表';

-- 教学活动_参与老师明细宽表：清空数据
TRUNCATE TABLE dwd_hainan_hospital_info.dwd_teaching_activity_teacher_detail_wide_df;

-- 教学活动_参与老师明细宽表：插入数据
INSERT INTO dwd_hainan_hospital_info.dwd_teaching_activity_teacher_detail_wide_df
SELECT DISTINCT total_t.teacher_id
              , total_t.teacher_name
              , total_t.teacher_hospital_id
              , total_t.teacher_hospital_name
              --, total_t.teacher_major_id
              , total_t.teacher_major_name
              , total_t.teacher_office_name
              , total_t.teacher_title
              , total_t.teacher_degree
              , total_t.teacher_highest_degree
              , total_t.teacher_promotion_time
              , total_t.teacher_phone
              , total_t.teacher_id_card
              , total_t.teacher_is_general_practice
FROM (
         -- item中老师信息汇总
         -- 数据源 1: 130-解放军总医院海南医院
         SELECT 130                                                                                            AS teacher_hospital_id
              , '解放军总医院海南医院'                                                                         AS teacher_hospital_name
              , coalesce(personid, -1)                                                                         AS teacher_id
              , trim(regexp_replace(personname, '[\r\n"]', ' '))                                               AS teacher_name
              --, coalesce(spt_major_id, -1)                                                            AS teacher_major_id
              , coalesce(nullif(trim(regexp_replace(spt_major_name, '[\r\n"]', ' ')), ''), '未知')             AS teacher_major_name
              , coalesce(nullif(trim(regexp_replace(name, '[\r\n"]', ' ')), ''), '未知')                       AS teacher_office_name
              , coalesce(nullif(trim(regexp_replace(professionaltitle, '[\r\n"]', ' ')), ''), '未知')          AS teacher_title
              , coalesce(nullif(trim(regexp_replace(academicdegree, '[\r\n"]', ' ')), ''), '未知')             AS teacher_degree
              , coalesce(nullif(trim(regexp_replace(highestdegree, '[\r\n"]', ' ')), ''), '未知')              AS teacher_highest_degree
              , promotiontime                                                                                  AS teacher_promotion_time
              , coalesce(nullif(trim(regexp_replace(phoneno, '[\r\n"]', ' ')), ''), '未知')                    AS teacher_phone
              , coalesce(nullif(trim(regexp_replace(identificationcard, '[\r\n"]', ' ')), ''), '未知')         AS teacher_id_card
              , coalesce(nullif(trim(regexp_replace(isgeneralfillingdepartment, '[\r\n"]', ' ')), ''), '未知') AS teacher_is_general_practice
         FROM ods_hainan_hospital_info.ods_hainan_teacher_performance_spt_teacher_item_301
         UNION ALL
         -- 数据源 2: 110-海南医学院第一附属医院
         SELECT 110                                                                                            AS teacher_hospital_id
              , '海南医学院第一附属医院'                                                                       AS teacher_hospital_name
              , coalesce(personid, -1)                                                                         AS teacher_id
              , trim(regexp_replace(personname, '[\r\n"]', ' '))                                               AS teacher_name
              --, coalesce(spt_major_id, -1)                                                            AS teacher_major_id
              , coalesce(nullif(trim(regexp_replace(spt_major_name, '[\r\n"]', ' ')), ''), '未知')             AS teacher_major_name
              , coalesce(nullif(trim(regexp_replace(name, '[\r\n"]', ' ')), ''), '未知')                       AS teacher_office_name
              , coalesce(nullif(trim(regexp_replace(professionaltitle, '[\r\n"]', ' ')), ''), '未知')          AS teacher_title
              , coalesce(nullif(trim(regexp_replace(academicdegree, '[\r\n"]', ' ')), ''), '未知')             AS teacher_degree
              , coalesce(nullif(trim(regexp_replace(highestdegree, '[\r\n"]', ' ')), ''), '未知')              AS teacher_highest_degree
              , promotiontime                                                                                  AS teacher_promotion_time
              , coalesce(nullif(trim(regexp_replace(phoneno, '[\r\n"]', ' ')), ''), '未知')                    AS teacher_phone
              , coalesce(nullif(trim(regexp_replace(identificationcard, '[\r\n"]', ' ')), ''), '未知')         AS teacher_id_card
              , coalesce(nullif(trim(regexp_replace(isgeneralfillingdepartment, '[\r\n"]', ' ')), ''), '未知') AS teacher_is_general_practice
         FROM ods_hainan_hospital_info.ods_hainan_teacher_performance_spt_teacher_item_first_affiliated
         UNION ALL
         -- 数据源 3: 111-三亚中心医院（海南省第三人民医院）
         SELECT 111
              , '三亚中心医院（海南省第三人民医院）'
              , coalesce(personid, -1)                                                                         AS teacher_id
              , trim(regexp_replace(personname, '[\r\n"]', ' '))                                               AS teacher_name
              --, coalesce(spt_major_id, -1)                                                            AS teacher_major_id
              , coalesce(nullif(trim(regexp_replace(spt_major_name, '[\r\n"]', ' ')), ''), '未知')             AS teacher_major_name
              , coalesce(nullif(trim(regexp_replace(name, '[\r\n"]', ' ')), ''), '未知')                       AS teacher_office_name
              , coalesce(nullif(trim(regexp_replace(professionaltitle, '[\r\n"]', ' ')), ''), '未知')          AS teacher_title
              , coalesce(nullif(trim(regexp_replace(academicdegree, '[\r\n"]', ' ')), ''), '未知')             AS teacher_degree
              , coalesce(nullif(trim(regexp_replace(highestdegree, '[\r\n"]', ' ')), ''), '未知')              AS teacher_highest_degree
              , promotiontime                                                                                  AS teacher_promotion_time
              , coalesce(nullif(trim(regexp_replace(phoneno, '[\r\n"]', ' ')), ''), '未知')                    AS teacher_phone
              , coalesce(nullif(trim(regexp_replace(identificationcard, '[\r\n"]', ' ')), ''), '未知')         AS teacher_id_card
              , coalesce(nullif(trim(regexp_replace(isgeneralfillingdepartment, '[\r\n"]', ' ')), ''), '未知') AS teacher_is_general_practice
         FROM ods_hainan_hospital_info.ods_hainan_teacher_performance_spt_teacher_item_sanya_central) total_t
         LEFT JOIN dwd_hainan_hospital_info.dwd_teaching_activity_person_detail_df apd
                   ON total_t.teacher_id = apd.person_id AND total_t.teacher_name = apd.person_name
                       AND total_t.teacher_hospital_id = apd.activity_hospital_id
                       AND total_t.teacher_hospital_name = apd.activity_hospital_name
WHERE apd.person_id IS NOT NULL
  AND apd.person_type = '老师';

/*
中文表名：教学活动_教师参与日志宽表
数据库表名：dwd_teaching_activity_teacher_log_wide_df
源数据表：
    - dwd_hainan_hospital_info.dwd_teaching_activity_teacher_detail_wide_df
    - dwd_hainan_hospital_info.dwd_teaching_activity_person_detail_df
    - dwd_hainan_hospital_info.dwd_teaching_activity_detail_df
*/

-- 教学活动_教师参与日志宽表：删除旧的（如果存在）
DROP TABLE IF EXISTS dwd_hainan_hospital_info.dwd_teaching_activity_teacher_log_wide_df;

-- 教学活动_教师参与日志宽表：创建
CREATE TABLE dwd_hainan_hospital_info.dwd_teaching_activity_teacher_log_wide_df
(
    teacher_id                  INT COMMENT '教师ID',
    teacher_name                STRING COMMENT '教师名字',
    teacher_hospital_id         INT COMMENT '教师所属医院ID',
    teacher_hospital_name       STRING COMMENT '教师所属医院',
    --teacher_major_id            INT COMMENT '教师所属专业ID',
    teacher_major_name          STRING COMMENT '教师所属专业名称',
    teacher_office_name         STRING COMMENT '教师所属科室',
    teacher_title               STRING COMMENT '教师职称',
    teacher_degree              STRING COMMENT '教师学位',
    teacher_highest_degree      STRING COMMENT '教师最高学位',
    teacher_promotion_time      TIMESTAMP COMMENT '教师晋升时间',
    teacher_phone               STRING COMMENT '教师电话',
    teacher_id_card             STRING COMMENT '教师身份证',
    teacher_is_general_practice STRING COMMENT '教师是否全科',
    activity_id                 STRING COMMENT '教学活动ID',
    activity_name               STRING COMMENT '教学活动名称',
    activity_type_id            INT COMMENT '教学活动类型ID',
    activity_type_name          STRING COMMENT '教学活动类型名称',
    activity_major_id           INT COMMENT '教学活动专业ID',
    activity_major_name         STRING COMMENT '教学活动专业名称',
    activity_office_name        STRING COMMENT '教学活动所属科室',
    activity_start_time         TIMESTAMP COMMENT '教学活动开始时间',
    activity_end_time           TIMESTAMP COMMENT '教学活动结束时间',
    is_signed                   STRING COMMENT '是否签到',
    evaluation_score_rate       DOUBLE COMMENT '评价得分率'
) COMMENT '教学活动_教师参与日志宽表';

-- 教学活动_教师参与日志宽表：清空数据
TRUNCATE TABLE dwd_hainan_hospital_info.dwd_teaching_activity_teacher_log_wide_df;

-- 教学活动_教师参与日志宽表：插入数据
INSERT INTO dwd_hainan_hospital_info.dwd_teaching_activity_teacher_log_wide_df
SELECT atdw.teacher_id
     , atdw.teacher_name
     , atdw.teacher_hospital_id
     , atdw.teacher_hospital_name
     --, atdw.teacher_major_id
     , atdw.teacher_major_name
     , atdw.teacher_office_name
     , atdw.teacher_title
     , atdw.teacher_degree
     , atdw.teacher_highest_degree
     , atdw.teacher_promotion_time
     , atdw.teacher_phone
     , atdw.teacher_id_card
     , atdw.teacher_is_general_practice
     , ad.activity_id
     , ad.activity_name
     , ad.activity_type_id
     , ad.activity_type_name
     , ad.activity_major_id
     , ad.activity_major_name
     , ad.activity_office_name
     , ad.activity_start_time
     , ad.activity_end_time
     , apd.is_signed
     , apd.evaluation_score_rate
FROM dwd_hainan_hospital_info.dwd_teaching_activity_teacher_detail_wide_df atdw
         LEFT JOIN dwd_hainan_hospital_info.dwd_teaching_activity_person_detail_df apd
                   ON atdw.teacher_id = apd.person_id AND atdw.teacher_name = apd.person_name
                       AND atdw.teacher_hospital_id = apd.activity_hospital_id AND atdw.teacher_hospital_name = apd.activity_hospital_name
         LEFT JOIN dwd_hainan_hospital_info.dwd_teaching_activity_detail_df ad
                   ON apd.activity_id = ad.activity_id AND apd.activity_hospital_id = ad.activity_hospital_id
                       AND apd.activity_hospital_name = ad.activity_hospital_name;

/*
中文表名：教学活动_有招录医院的入院教育明细宽表
数据库表名：dwd_teaching_activity_admission_hospital_education_detail_wide_df
源数据表：
    - dwd_hainan_hospital_info.dwd_hainan_admission_data_view_students
    - dwd_hainan_hospital_info.dwd_teaching_activity_detail_df
*/

-- 教学活动_有招录医院的入院教育明细宽表：删除旧的（如果存在）
DROP TABLE IF EXISTS dwd_hainan_hospital_info.dwd_teaching_activity_admission_hospital_education_detail_wide_df;

-- 教学活动_有招录医院的入院教育明细宽表：创建
CREATE TABLE dwd_hainan_hospital_info.dwd_teaching_activity_admission_hospital_education_detail_wide_df
(
    admission_year             INT COMMENT '年份',
    admission_hospital_id      INT COMMENT '医院ID',
    admission_hospital_name    STRING COMMENT '医院名称',
    admission_student_count    INT COMMENT '入院学生人数',
    activity_id                STRING COMMENT '教学活动ID',
    activity_name              STRING COMMENT '教学活动名称',
    activity_type_id           INT COMMENT '教学活动类型ID',
    activity_type_name         STRING COMMENT '教学活动类型名称',
    activity_major_id          INT COMMENT '教学活动专业ID',
    activity_major_name        STRING COMMENT '教学活动专业名称',
    activity_office_name       STRING COMMENT '教学活动所属科室',
    activity_start_time        TIMESTAMP COMMENT '教学活动开始时间',
    activity_end_time          TIMESTAMP COMMENT '教学活动结束时间',
    activity_duration_minutes  INT COMMENT '教学活动时长（分钟）',
    activity_participant_count INT COMMENT '教学活动参与人数'
) COMMENT '教学活动_有招录医院的入院教育明细宽表';

-- 教学活动_有招录医院的入院教育明细宽表：清空数据
TRUNCATE TABLE dwd_hainan_hospital_info.dwd_teaching_activity_admission_hospital_education_detail_wide_df;

-- 教学活动_有招录医院的入院教育明细宽表：插入数据
INSERT INTO dwd_hainan_hospital_info.dwd_teaching_activity_admission_hospital_education_detail_wide_df
SELECT ad.admission_year
     , ad.admission_hospital_id
     , ad.admission_hospital_name
     , ad.admission_student_count
     , ac.activity_id
     , ac.activity_name
     , ac.activity_type_id
     , ac.activity_type_name
     , ac.activity_major_id
     , ac.activity_major_name
     , ac.activity_office_name
     , ac.activity_start_time
     , ac.activity_end_time
     , ac.activity_duration_minutes
     , ac.activity_participant_count
FROM (SELECT admission_year             AS admission_year
           , training_base_id           AS admission_hospital_id
           , training_base              AS admission_hospital_name
           , count(DISTINCT student_id) AS admission_student_count
      FROM dwd_hainan_hospital_info.dwd_hainan_admission_data_view_students
      WHERE admission_year >= 2019
      GROUP BY admission_year, training_base_id, training_base) ad
         LEFT JOIN (SELECT activity_hospital_id
                         , activity_hospital_name
                         , date_format(activity_start_time, 'yyyy') AS activity_year
                         , activity_id
                         , activity_name
                         , activity_type_id
                         , activity_type_name
                         , activity_major_id
                         , activity_major_name
                         , activity_office_name
                         , activity_start_time
                         , activity_end_time
                         , activity_duration_minutes
                         , activity_participant_count
                    FROM dwd_hainan_hospital_info.dwd_teaching_activity_detail_df
                    WHERE year(activity_start_time) >= 2019
                      AND activity_type_id = 14
                      AND activity_type_name = '入院教育') ac
                   ON ad.admission_hospital_id = ac.activity_hospital_id AND ad.admission_hospital_name = ac.activity_hospital_name AND
                      ad.admission_year = ac.activity_year;

/*
中文表名：教学活动_有招录医院专业基地的入专业教育明细宽表
数据库表名：dwd_teaching_activity_admission_hospital_major_education_detail_wide_df
源数据表：
    - dwd_hainan_hospital_info.dwd_hainan_admission_data_view_students
    - dwd_hainan_hospital_info.dwd_teaching_activity_detail_df
*/

-- 教学活动_有招录医院专业基地的入专业教育明细宽表：删除旧的（如果存在）
DROP TABLE IF EXISTS dwd_hainan_hospital_info.dwd_teaching_activity_admission_hospital_major_education_detail_wide_df;

-- 教学活动_有招录医院专业基地的入专业教育明细宽表：创建
CREATE TABLE dwd_hainan_hospital_info.dwd_teaching_activity_admission_hospital_major_education_detail_wide_df
(
    admission_year             INT COMMENT '年份',
    admission_hospital_id      INT COMMENT '医院ID',
    admission_hospital_name    STRING COMMENT '医院名称',
    admission_major_id         INT COMMENT '专业ID',
    admission_major_name       STRING COMMENT '专业名称',
    admission_student_count    INT COMMENT '入专业基地学生人数',
    activity_id                STRING COMMENT '教学活动ID',
    activity_name              STRING COMMENT '教学活动名称',
    activity_type_id           INT COMMENT '教学活动类型ID',
    activity_type_name         STRING COMMENT '教学活动类型名称',
    activity_office_name       STRING COMMENT '教学活动所属科室',
    activity_start_time        TIMESTAMP COMMENT '教学活动开始时间',
    activity_end_time          TIMESTAMP COMMENT '教学活动结束时间',
    activity_duration_minutes  INT COMMENT '教学活动时长（分钟）',
    activity_participant_count INT COMMENT '教学活动参与人数'
) COMMENT '教学活动_有招录医院专业基地的入专业教育明细宽表';

-- 教学活动_有招录医院专业基地的入专业教育明细宽表：清空数据
TRUNCATE TABLE dwd_hainan_hospital_info.dwd_teaching_activity_admission_hospital_major_education_detail_wide_df;

-- 教学活动_有招录医院专业基地的入专业教育明细宽表：插入数据
INSERT INTO dwd_hainan_hospital_info.dwd_teaching_activity_admission_hospital_major_education_detail_wide_df
SELECT ad.admission_year
     , ad.admission_hospital_id
     , ad.admission_hospital_name
     , ad.admission_major_id
     , ad.admission_major_name
     , ad.admission_student_count
     , ac.activity_id
     , ac.activity_name
     , ac.activity_type_id
     , ac.activity_type_name
     , ac.activity_office_name
     , ac.activity_start_time
     , ac.activity_end_time
     , ac.activity_duration_minutes
     , ac.activity_participant_count
FROM (SELECT admission_year             AS admission_year
           , training_base_id           AS admission_hospital_id
           , training_base              AS admission_hospital_name
           , specialty_base_id          AS admission_major_id
           , specialty_base             AS admission_major_name
           , count(DISTINCT student_id) AS admission_student_count
      FROM dwd_hainan_hospital_info.dwd_hainan_admission_data_view_students
      WHERE admission_year >= 2019
      GROUP BY admission_year, training_base_id, training_base, specialty_base_id, specialty_base) ad
         LEFT JOIN (SELECT activity_hospital_id
                         , activity_hospital_name
                         , activity_major_id
                         , activity_major_name
                         , date_format(activity_start_time, 'yyyy') AS activity_year
                         , activity_id
                         , activity_name
                         , activity_type_id
                         , activity_type_name
                         , activity_office_name
                         , activity_start_time
                         , activity_end_time
                         , activity_duration_minutes
                         , activity_participant_count
                    FROM dwd_hainan_hospital_info.dwd_teaching_activity_detail_df
                    WHERE year(activity_start_time) >= 2019
                      AND activity_type_id = 15
                      AND activity_type_name = '入专业基地教育') ac
                   ON ad.admission_hospital_id = ac.activity_hospital_id AND ad.admission_hospital_name = ac.activity_hospital_name
                       AND ad.admission_major_id = ac.activity_major_id AND ad.admission_major_name = ac.activity_major_name AND
                      ad.admission_year = ac.activity_year;

/*
中文表名：教学活动_轮转学员当月教学活动明细宽表
数据库表名：dwd_teaching_activity_round_student_detail_wide_df
源数据表：
    - ods_hainan_hospital_info.dwd_hainan_round_spt_round_total_info
    - ods_hainan_hospital_info.dwd_teaching_activity_student_sign_wide_df
*/

-- 教学活动_轮转学员当月教学活动明细宽表：删除旧的（如果存在）
DROP TABLE IF EXISTS dwd_hainan_hospital_info.dwd_teaching_activity_round_student_detail_wide_df;

-- 教学活动_轮转学员当月教学活动明细宽表：创建
CREATE TABLE dwd_hainan_hospital_info.dwd_teaching_activity_round_student_detail_wide_df
(
    rotation_hospital_id            INT COMMENT '轮转医院ID',
    rotation_hospital_name          STRING COMMENT '轮转医院名称',
    rotation_student_major_id       INT COMMENT '轮转学员专业ID',
    rotation_student_major_name     STRING COMMENT '轮转学员专业名称',
    rotation_student_id             INT COMMENT '轮转学员ID',
    rotation_student_name           STRING COMMENT '轮转学员名字',
    rotation_student_admission_year INT COMMENT '轮转学员入学年份',
    rotation_office_name            STRING COMMENT '轮转科室名称',
    rotation_office_major_id        INT COMMENT '轮转科室专业ID',
    rotation_office_major_name      STRING COMMENT '轮转科室专业名称',
    rotation_office_outline_name    STRING COMMENT '轮转科室大纲名称',
    rotation_start_time             TIMESTAMP COMMENT '轮转开始时间',
    rotation_end_time               TIMESTAMP COMMENT '轮转结束时间',
    rotation_month                  STRING COMMENT '轮转月份',
    rotation_teacher_name           STRING COMMENT '轮转老师名字',
    rotation_teacher_major_id       INT COMMENT '轮转老师专业ID',
    rotation_teacher_major_name     STRING COMMENT '轮转老师专业名称',
    rotation_finish_skill_score     DOUBLE COMMENT '轮转完成技能评分',
    activity_id                     STRING COMMENT '教学活动ID',
    activity_name                   STRING COMMENT '教学活动名称',
    activity_type_id                INT COMMENT '教学活动类型ID',
    activity_type_name              STRING COMMENT '教学活动类型名称',
    activity_major_id               INT COMMENT '教学活动专业ID',
    activity_major_name             STRING COMMENT '教学活动专业名称',
    activity_office_name            STRING COMMENT '教学活动所属科室',
    activity_start_time             TIMESTAMP COMMENT '教学活动开始时间',
    activity_end_time               TIMESTAMP COMMENT '教学活动结束时间',
    activity_month                  STRING COMMENT '教学活动月份',
    activity_is_signed              STRING COMMENT '教学活动是否签到',
    activity_evaluation_score_rate  DOUBLE COMMENT '教学活动评价得分率'
) COMMENT '教学活动_轮转学员当月教学活动明细宽表';

-- 教学活动_轮转学员当月教学活动明细宽表：清空数据
TRUNCATE TABLE dwd_hainan_hospital_info.dwd_teaching_activity_round_student_detail_wide_df;

-- 教学活动_轮转学员当月教学活动明细宽表：插入数据
INSERT INTO dwd_hainan_hospital_info.dwd_teaching_activity_round_student_detail_wide_df
SELECT r.rotation_hospital_id
     , r.rotation_hospital_name
     , r.rotation_student_major_id
     , r.rotation_student_major_name
     , sd.student_id                                 AS rotation_student_id
     , r.rotation_student_name
     , r.rotation_student_admission_year
     , r.rotation_office_name
     , r.rotation_office_major_id
     , r.rotation_office_major_name
     , r.rotation_office_outline_name
     , r.rotation_start_time
     , r.rotation_end_time
     , r.rotation_month
     , r.rotation_teacher_name
     , r.rotation_teacher_major_id
     , r.rotation_teacher_major_name
     , r.rotation_finish_skill_score
     , s.activity_id
     , s.activity_name
     , s.activity_type_id
     , s.activity_type_name
     , s.activity_major_id
     , s.activity_major_name
     , s.activity_office_name
     , s.activity_start_time
     , s.activity_end_time
     , date_format(s.activity_start_time, 'yyyy-MM') AS activity_month
     , s.activity_is_signed
     , s.activity_evaluation_score_rate
FROM (
         -- 基础数据转换
         SELECT personname                                 AS rotation_student_name
              , CASE
                    WHEN hospital = '301海南医院' THEN 130
                    WHEN hospital = '海南医学院第一附属医院' THEN 110
                    WHEN hospital = '三亚中心医院（海南省第三人民医院）' THEN 111
                    ELSE -1
             END                                           AS rotation_hospital_id
              , CASE
                    WHEN hospital = '301海南医院' THEN '解放军总医院海南医院'
                    WHEN hospital = '海南医学院第一附属医院' THEN '海南医学院第一附属医院'
                    WHEN hospital = '三亚中心医院（海南省第三人民医院）' THEN '三亚中心医院（海南省第三人民医院）'
                    ELSE '其他'
             END                                           AS rotation_hospital_name
              , spt_major_id                               AS rotation_student_major_id
              , spt_major_name                             AS rotation_student_major_name
              , gradeyear                                  AS rotation_student_admission_year
              , officename                                 AS rotation_office_name
              , office_major_id                            AS rotation_office_major_id
              , office_major_name                          AS rotation_office_major_name
              , outlinename                                AS rotation_office_outline_name
              , starttime                                  AS rotation_start_time
              , endtime                                    AS rotation_end_time
              , teachername                                AS rotation_teacher_name
              , teacher_major_id                           AS rotation_teacher_major_id
              , teacher_major_name                         AS rotation_teacher_major_name
              , finishskillscore                           AS rotation_finish_skill_score
              , date_format(add_months(to_date(concat(date_format(starttime, 'yyyy-MM'), '-01'))
                                , n_table.num), 'yyyy-MM') AS rotation_month
         FROM dwd_hainan_hospital_info.dwd_hainan_round_spt_round_total_info
                  LATERAL VIEW explode(array(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)) n_table AS num
         WHERE
           -- 修正条件：确保生成的月份不超过实际的轮转月数
             n_table.num <= floor(months_between(endtime, starttime))
           AND starttime IS NOT NULL
           AND endtime IS NOT NULL
           AND starttime <= endtime) r
         LEFT JOIN dwd_hainan_hospital_info.dwd_teaching_activity_student_log_wide_df s
                   ON r.rotation_student_name = s.student_name
                       AND r.rotation_hospital_id = s.student_hospital_id
                       AND r.rotation_hospital_name = s.student_hospital_name
                       AND r.rotation_student_major_id = s.student_major_id
                       AND r.rotation_student_major_name = s.student_major_name
                       AND r.rotation_student_admission_year = s.student_admission_year
                       AND r.rotation_month = date_format(s.activity_start_time, 'yyyy-MM')
    -- 补充学生ID信息
         LEFT JOIN dwd_hainan_hospital_info.dwd_teaching_activity_student_detail_wide_df sd
                   ON r.rotation_hospital_id = sd.student_hospital_id
                       AND r.rotation_hospital_name = sd.student_hospital_name
                       AND r.rotation_student_major_id = sd.student_major_id
                       AND r.rotation_student_major_name = sd.student_major_name
                       AND r.rotation_student_name = sd.student_name
                       AND r.rotation_student_admission_year = sd.student_admission_year;

/*
中文表名：教学活动_名称不规范明细宽表
数据库表名：dwd_teaching_activity_name_abnormal_detail_wide_df
源数据表：
    - dwd_teaching_activity_detail_df
*/

-- 教学活动_名称不规范明细宽表：删除旧的（如果存在）
DROP TABLE IF EXISTS dwd_hainan_hospital_info.dwd_teaching_activity_name_abnormal_detail_wide_df;

-- 教学活动_名称不规范明细宽表：创建
CREATE TABLE dwd_hainan_hospital_info.dwd_teaching_activity_name_abnormal_detail_wide_df
(
    activity_hospital_id   INT COMMENT '教学活动医院ID',
    activity_hospital_name STRING COMMENT '教学活动医院名称',
    activity_major_id      INT COMMENT '教学活动专业ID',
    activity_major_name    STRING COMMENT '教学活动专业名称',
    activity_month         STRING COMMENT '教学活动月份',
    activity_id            STRING COMMENT '教学活动ID',
    activity_name          STRING COMMENT '教学活动名称',
    activity_type_id       INT COMMENT '教学活动类型ID',
    activity_type_name     STRING COMMENT '教学活动类型名称',
    name_abnormal_type     INT COMMENT '名称不规范类型',
    activity_office_name   STRING COMMENT '教学活动所属科室',
    teacher_id             INT COMMENT '教师ID',
    teacher_name           STRING COMMENT '教师名字',
    activity_start_time    TIMESTAMP COMMENT '教学活动开始时间'
) COMMENT '教学活动_名称不规范明细宽表';

-- 教学活动_名称不规范明细宽表：清空数据
TRUNCATE TABLE dwd_hainan_hospital_info.dwd_teaching_activity_name_abnormal_detail_wide_df;

-- 教学活动_名称不规范明细宽表：插入数据
INSERT INTO dwd_hainan_hospital_info.dwd_teaching_activity_name_abnormal_detail_wide_df
SELECT ad.activity_hospital_id
     , ad.activity_hospital_name
     , ad.activity_major_id
     , ad.activity_major_name
     , ad.activity_month
     , ad.activity_id
     , ad.activity_name
     , ad.activity_type_id
     , ad.activity_type_name
     , ad.name_abnormal_type
     , tl.activity_office_name
     , tl.teacher_id
     , tl.teacher_name
     , tl.activity_start_time
FROM (SELECT activity_hospital_id
           , activity_hospital_name
           , activity_major_id
           , activity_major_name
           , activity_month
           , activity_id
           , activity_type_id
           , activity_name
           , activity_type_name
           , name_abnormal_type
      FROM (SELECT activity_hospital_id
                 , activity_hospital_name
                 , activity_major_id
                 , activity_major_name
                 , date_format(activity_start_time, 'yyyy-MM') AS activity_month
                 , activity_name
                 , activity_type_id
                 , activity_type_name
                 , count(DISTINCT activity_id)                 AS activity_count
                 -- 规范类型
                 -- 0-规范，1-单教学活动内名称与类型同命名（命名重复），2-不同教学活动名称相同相同（短期内相似主题
                 , CASE
                       WHEN activity_name = activity_type_name THEN 1
                       WHEN count(DISTINCT activity_id) > 1 THEN 2
                       ELSE 0
              END                                              AS name_abnormal_type
                 , collect_list(activity_id)                   AS activity_ids
            FROM dwd_hainan_hospital_info.dwd_teaching_activity_detail_df
            GROUP BY activity_hospital_id, activity_hospital_name, activity_major_id, activity_major_name, date_format(activity_start_time, 'yyyy-MM')
                   , activity_name, activity_type_id
                   , activity_type_name) grouped_table
               LATERAL VIEW explode(activity_ids) exploded_table AS activity_id) ad
         LEFT JOIN dwd_hainan_hospital_info.dwd_teaching_activity_teacher_log_wide_df tl
                   ON ad.activity_hospital_id = tl.teacher_hospital_id AND ad.activity_hospital_name = tl.teacher_hospital_name
                       AND ad.activity_major_id = tl.activity_major_id AND ad.activity_major_name = tl.activity_major_name
                       AND ad.activity_id = tl.activity_id AND ad.activity_name = tl.activity_name;