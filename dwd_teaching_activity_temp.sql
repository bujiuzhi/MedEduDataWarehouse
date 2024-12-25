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
        教学活动_活动明细表 dwd_teaching_activity_detail_df
        教学活动_参与人员明细表 dwd_teaching_activity_person_detail_df
        教学活动_附件明细表   dwd_teaching_activity_file_detail_df

        -- 基于清洗后的数据拉的宽表 --
        教学活动_学员签到宽表  dwd_teaching_activity_student_sign_wide_df
        教学活动_学员评价宽表  dwd_teaching_activity_student_evaluation_wide_df
        教学活动_老师明细宽表    dwd_teaching_activity_teacher_detail_wide_df
        教学活动_轮转学员当月教学活动情况宽表  dwd_teaching_activity_round_student_detail_wide_df

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
*/

-- 教学活动_活动明细表：删除旧的（如果存在）
DROP TABLE IF EXISTS dwd_hainan_hospital_info.dwd_teaching_activity_detail_df;

-- 教学活动_活动明细表：创建
CREATE TABLE dwd_hainan_hospital_info.dwd_teaching_activity_detail_df
(
    activity_id        STRING COMMENT '教学活动ID',
    activity_name      STRING COMMENT '教学活动名称',
    activity_type_id   INT COMMENT '类型ID',
    activity_type_name STRING COMMENT '类型名称',
    hospital_id        INT COMMENT '医院ID',
    hospital_name      STRING COMMENT '医院名称',
    major_id           INT COMMENT '专业ID',
    major_name         STRING COMMENT '专业名称',
    office_name        STRING COMMENT '所属科室',
    start_time         TIMESTAMP COMMENT '开始时间',
    end_time           TIMESTAMP COMMENT '结束时间',
    duration_minutes   INT COMMENT '时长（分钟）',
    participant_count  INT COMMENT '应评价人数'
) COMMENT '教学活动_活动明细表';

-- 教学活动_活动明细表：清空数据
TRUNCATE TABLE dwd_hainan_hospital_info.dwd_teaching_activity_detail_df;

-- 教学活动_活动明细表：插入数据
INSERT INTO dwd_hainan_hospital_info.dwd_teaching_activity_detail_df
SELECT DISTINCT concat(hospital_id, '_', trainid) AS activity_id
              , activity_name
              , activity_type_id
              , activity_type_name
              , hospital_id
              , hospital_name
              , major_id
              , major_name
              , office_name
              , start_time
              , end_time
              , duration_minutes
              , participant_count
FROM (
         -- 数据源 1: 130-解放军总医院海南医院
         SELECT 130                                                                                   AS hospital_id
              , '解放军总医院海南医院'                                                                AS hospital_name
              , trainid
              , regexp_replace(title, '[\r\n]', ' ')                                                  AS activity_name
              , coalesce(spt_traintype_id, -1)                                                        AS activity_type_id
              , coalesce(nullif(trim(regexp_replace(spt_traintype_name, '[\r\n]', ' ')), ''), '其他') AS activity_type_name
              , coalesce(spt_major_id, -1)                                                            AS major_id
              , coalesce(nullif(trim(regexp_replace(spt_major_name, '[\r\n]', ' ')), ''), '其他')     AS major_name
              , coalesce(nullif(trim(regexp_replace(officename, '[\r\n]', ' ')), ''), '其他')         AS office_name
              , starttime                                                                             AS start_time
              , endtime                                                                               AS end_time
              , teaching_activity_duration                                                            AS duration_minutes
              , participant_count
         FROM ods_hainan_hospital_info.ods_hainan_teaching_activity_spt_train_sheet_data_301
         WHERE trainid IS NOT NULL
           AND trainid >= 0
           AND (spt_traintype_id IS NOT NULL OR spt_major_id IS NOT NULL)
         UNION ALL
         -- 数据源 2: 110-海南医学院第一附属医院
         SELECT 110                                                                                   AS hospital_id
              , '海南医学院第一附属医院'                                                              AS hospital_name
              , trainid
              , regexp_replace(title, '[\r\n]', ' ')                                                  AS activity_name
              , coalesce(spt_traintype_id, -1)                                                        AS activity_type_id
              , coalesce(nullif(trim(regexp_replace(spt_traintype_name, '[\r\n]', ' ')), ''), '其他') AS activity_type_name
              , coalesce(spt_major_id, -1)                                                            AS major_id
              , coalesce(nullif(trim(regexp_replace(spt_major_name, '[\r\n]', ' ')), ''), '其他')     AS major_name
              , coalesce(nullif(trim(regexp_replace(officename, '[\r\n]', ' ')), ''), '其他')         AS office_name
              , starttime                                                                             AS start_time
              , endtime                                                                               AS end_time
              , teaching_activity_duration                                                            AS duration_minutes
              , participant_count
         FROM ods_hainan_hospital_info.ods_hainan_teaching_activity_spt_train_sheet_data_first_affiliated
         WHERE trainid IS NOT NULL
           AND trainid >= 0
         UNION ALL
         -- 数据源 3: 111-三亚中心医院（海南省第三人民医院）
         SELECT 111                                                                                   AS hospital_id
              , '三亚中心医院（海南省第三人民医院）'                                                    AS hospital_name
              , trainid
              , regexp_replace(title, '[\r\n]', ' ')                                                  AS activity_name
              , coalesce(spt_traintype_id, -1)                                                        AS activity_type_id
              , coalesce(nullif(trim(regexp_replace(spt_traintype_name, '[\r\n]', ' ')), ''), '其他') AS activity_type_name
              , coalesce(spt_major_id, -1)                                                            AS major_id
              , coalesce(nullif(trim(regexp_replace(spt_major_name, '[\r\n]', ' ')), ''), '其他')     AS major_name
              , coalesce(nullif(trim(regexp_replace(officename, '[\r\n]', ' ')), ''), '其他')         AS office_name
              , starttime                                                                             AS start_time
              , endtime                                                                               AS end_time
              , teaching_activity_duration                                                            AS duration_minutes
              , participant_count
         FROM ods_hainan_hospital_info.ods_hainan_teaching_activity_spt_train_sheet_data_sanya_central
         WHERE trainid IS NOT NULL
           AND trainid >= 0) AS combined_data;

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
    activity_id           STRING COMMENT '教学活动ID',
    hospital_id           INT COMMENT '所属医院ID',
    hospital_name         STRING COMMENT '所属医院',
    person_id             INT COMMENT '人员ID',
    person_name           STRING COMMENT '人员名字',
    person_type           STRING COMMENT '人员类型',
    professional_title    STRING COMMENT '专业职称',
    evaluation_score_rate DOUBLE COMMENT '评价得分率',
    is_signed             STRING COMMENT '是否签到'
) COMMENT '教学活动参与人员明细表';

-- 教学活动_参与人员明细表：清空数据
TRUNCATE TABLE dwd_hainan_hospital_info.dwd_teaching_activity_person_detail_df;

-- 教学活动_参与人员明细表：插入数据
INSERT INTO dwd_hainan_hospital_info.dwd_teaching_activity_person_detail_df
SELECT DISTINCT concat(hospital_id, '_', trainid) AS activity_id
              , hospital_id
              , hospital_name
              , person_id
              , person_name
              , person_type
              , professional_title
              , evaluation_score_rate
              , is_signed
FROM (
         -- 数据源 1: 130-解放军总医院海南医院
         SELECT 130                                                                                   AS hospital_id
              , '解放军总医院海南医院'                                                                AS hospital_name
              , trainid
              , coalesce(personid, -1)                                                                AS person_id
              , coalesce(nullif(trim(regexp_replace(personname, '[\r\n"]', ' ')), ''), '其他')        AS person_name
              , coalesce(nullif(trim(regexp_replace(persontype, '[\r\n"]', ' ')), ''), '其他')        AS person_type
              , coalesce(nullif(trim(regexp_replace(professionaltitle, '[\r\n"]', ' ')), ''), '其他') AS professional_title
              , evalua_score_rate                                                                     AS evaluation_score_rate
              , coalesce(nullif(trim(regexp_replace(is_sign, '[\r\n"]', ' ')), ''), '其他')           AS is_signed
         FROM ods_hainan_hospital_info.ods_hainan_teaching_activity_spt_train_person_data_301
         WHERE trainid IS NOT NULL
           AND trainid >= 0
         UNION ALL
         -- 数据源 2: 110-海南医学院第一附属医院
         SELECT 110                                                                                   AS hospital_id
              , '海南医学院第一附属医院'                                                              AS hospital_name
              , trainid
              , coalesce(personid, -1)                                                                AS person_id
              , coalesce(nullif(trim(regexp_replace(personname, '[\r\n"]', ' ')), ''), '其他')        AS person_name
              , coalesce(nullif(trim(regexp_replace(persontype, '[\r\n"]', ' ')), ''), '其他')        AS person_type
              , coalesce(nullif(trim(regexp_replace(professionaltitle, '[\r\n"]', ' ')), ''), '其他') AS professional_title
              , evalua_score_rate                                                                     AS evaluation_score_rate
              , coalesce(nullif(trim(regexp_replace(is_sign, '[\r\n"]', ' ')), ''), '其他')           AS is_signed
         FROM ods_hainan_hospital_info.ods_hainan_teaching_activity_spt_train_person_data_first_affiliated
         WHERE trainid IS NOT NULL
           AND trainid >= 0
         UNION ALL
         -- 数据源 3: 111-三亚中心医院（海南省第三人民医院）
         SELECT 111                                                                                   AS hospital_id
              , '三亚中心医院（海南省第三人民医院）'                                                    AS hospital_name
              , trainid
              , coalesce(personid, -1)                                                                AS person_id
              , coalesce(nullif(trim(regexp_replace(personname, '[\r\n"]', ' ')), ''), '其他')        AS person_name
              , coalesce(nullif(trim(regexp_replace(persontype, '[\r\n"]', ' ')), ''), '其他')        AS person_type
              , coalesce(nullif(trim(regexp_replace(professionaltitle, '[\r\n"]', ' ')), ''), '其他') AS professional_title
              , evalua_score_rate                                                                     AS evaluation_score_rate
              , coalesce(nullif(trim(regexp_replace(is_sign, '[\r\n"]', ' ')), ''), '其他')           AS is_signed
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
    activity_id   STRING COMMENT '教学活动ID',
    hospital_id   INT COMMENT '所属医院ID',
    hospital_name STRING COMMENT '所属医院',
    file_type     STRING COMMENT '附件类型',
    file_url      STRING COMMENT '附件URL',
    file_name     STRING COMMENT '附件名称'
) COMMENT '教学活动附件明细表';

-- 教学活动_附件明细表：清空数据
TRUNCATE TABLE dwd_hainan_hospital_info.dwd_teaching_activity_file_detail_df;

-- 教学活动_附件明细表：插入数据
INSERT INTO dwd_hainan_hospital_info.dwd_teaching_activity_file_detail_df
SELECT DISTINCT concat(hospital_id, '_', trainid) AS activity_id
              , hospital_id
              , hospital_name
              , file_type
              , file_url
              , file_name
FROM (
         -- 数据源 1: 130-解放军总医院海南医院
         SELECT 130                                                                          AS hospital_id
              , '解放军总医院海南医院'                                                       AS hospital_name
              , trainid
              , coalesce(nullif(trim(regexp_replace(filetype, '[\r\n"]', ' ')), ''), '其他') AS file_type
              , trim(regexp_replace(url, '[\r\n"]', ' '))                                    AS file_url
              , trim(regexp_replace(reffilename, '[\r\n"]', ' '))                            AS file_name
         FROM ods_hainan_hospital_info.ods_hainan_teaching_activity_spt_train_file_info_301
         WHERE trainid IS NOT NULL
           AND trainid >= 0
         UNION ALL
         -- 数据源 2: 110-海南医学院第一附属医院
         SELECT 110                                                                          AS hospital_id
              , '海南医学院第一附属医院'                                                     AS hospital_name
              , trainid
              , coalesce(nullif(trim(regexp_replace(filetype, '[\r\n"]', ' ')), ''), '其他') AS file_type
              , trim(regexp_replace(url, '[\r\n"]', ' '))                                    AS file_url
              , trim(regexp_replace(reffilename, '[\r\n"]', ' '))                            AS file_name
         FROM ods_hainan_hospital_info.ods_hainan_teaching_activity_spt_train_file_info_first_affiliated
         WHERE trainid IS NOT NULL
           AND trainid >= 0
         UNION ALL
         -- 数据源 3: 111-三亚中心医院（海南省第三人民医院）
         SELECT 111                                                                          AS hospital_id
              , '三亚中心医院（海南省第三人民医院）'                                           AS hospital_name
              , trainid
              , coalesce(nullif(trim(regexp_replace(filetype, '[\r\n"]', ' ')), ''), '其他') AS file_type
              , trim(regexp_replace(url, '[\r\n"]', ' '))                                    AS file_url
              , trim(regexp_replace(reffilename, '[\r\n"]', ' '))                            AS file_name
         FROM ods_hainan_hospital_info.ods_hainan_teaching_activity_spt_train_file_info_sanya_central
         WHERE trainid IS NOT NULL
           AND trainid >= 0) AS combined_data;

/*
中文表名：教学活动_学员签到宽表
数据库表名：dwd_teaching_activity_student_sign_wide_df
源数据表：
    - ods_hainan_hospital_info.dwd_teaching_activity_person_detail_df
    - ods_hainan_hospital_info.dwd_teaching_activity_detail_df
*/

-- 教学活动_学员签到宽表：删除旧的（如果存在）
DROP TABLE IF EXISTS dwd_hainan_hospital_info.dwd_teaching_activity_student_sign_wide_df;

-- 教学活动_学员签到宽表：创建
CREATE TABLE dwd_hainan_hospital_info.dwd_teaching_activity_student_sign_wide_df
(
    activity_id        STRING COMMENT '教学活动ID',
    activity_name      STRING COMMENT '教学活动名称',
    activity_type_id   INT COMMENT '教学活动类型ID',
    activity_type_name STRING COMMENT '教学活动类型名称',
    hospital_id        INT COMMENT '教学活动所属医院ID',
    hospital_name      STRING COMMENT '教学活动所属医院',
    major_id           INT COMMENT '教学活动所属专业ID',
    major_name         STRING COMMENT '教学活动所属专业名称',
    office_name        STRING COMMENT '教学活动所属科室',
    start_time         TIMESTAMP COMMENT '教学活动开始时间',
    end_time           TIMESTAMP COMMENT '教学活动结束时间',
    student_id         INT COMMENT '学员ID',
    student_name       STRING COMMENT '学员名字',
    is_signed          STRING COMMENT '是否签到'
) COMMENT '教学活动学员签到宽表';

-- 教学活动_学员签到宽表：清空数据
TRUNCATE TABLE dwd_hainan_hospital_info.dwd_teaching_activity_student_sign_wide_df;

-- 教学活动_学员签到宽表：插入数据
INSERT INTO dwd_hainan_hospital_info.dwd_teaching_activity_student_sign_wide_df
SELECT DISTINCT a.activity_id        AS activity_id
              , a.activity_name      AS activity_name
              , a.activity_type_id   AS activity_type_id
              , a.activity_type_name AS activity_type_name
              , a.hospital_id        AS hospital_id
              , a.hospital_name      AS hospital_name
              , a.major_id           AS major_id
              , a.major_name         AS major_name
              , a.office_name        AS office_name
              , a.start_time         AS start_time
              , a.end_time           AS end_time
              , p.person_id          AS student_id
              , p.person_name        AS student_name
              , p.is_signed          AS is_signed
FROM dwd_hainan_hospital_info.dwd_teaching_activity_person_detail_df p
         LEFT JOIN dwd_hainan_hospital_info.dwd_teaching_activity_detail_df a
                   ON p.activity_id = a.activity_id
WHERE p.person_type = '学员';


/*
中文表名：教学活动_学员评价宽表
数据库表名：dwd_teaching_activity_student_evaluation_wide_df
源数据表：
    - ods_hainan_hospital_info.dwd_teaching_activity_person_detail_df
    - ods_hainan_hospital_info.dwd_teaching_activity_detail_df
*/
-- 教学活动_学员评价宽表：删除旧的（如果存在）
DROP TABLE IF EXISTS dwd_hainan_hospital_info.dwd_teaching_activity_student_evaluation_wide_df;

-- 教学活动_学员评价宽表：创建
CREATE TABLE dwd_hainan_hospital_info.dwd_teaching_activity_student_evaluation_wide_df
(
    activity_id        STRING COMMENT '教学活动ID',
    activity_name      STRING COMMENT '教学活动名称',
    activity_type_id   INT COMMENT '教学活动类型ID',
    activity_type_name STRING COMMENT '教学活动类型名称',
    hospital_id        INT COMMENT '教学活动所属医院ID',
    hospital_name      STRING COMMENT '教学活动所属医院',
    major_id           INT COMMENT '教学活动所属专业ID',
    major_name         STRING COMMENT '教学活动所属专业名称',
    office_name        STRING COMMENT '教学活动所属科室',
    start_time         TIMESTAMP COMMENT '教学活动开始时间',
    end_time           TIMESTAMP COMMENT '教学活动结束时间',
    student_id         INT COMMENT '学员ID',
    student_name       STRING COMMENT '学员名字',
    evaluation_score   DOUBLE COMMENT '评价得分',
    participant_count  INT COMMENT '教学活动应评价人数'
) COMMENT '教学活动学员评价宽表';

-- 教学活动_学员评价宽表：清空数据
TRUNCATE TABLE dwd_hainan_hospital_info.dwd_teaching_activity_student_evaluation_wide_df;

-- 教学活动_学员评价宽表：插入数据
INSERT INTO dwd_hainan_hospital_info.dwd_teaching_activity_student_evaluation_wide_df
SELECT DISTINCT a.activity_id           AS activity_id
              , a.activity_name         AS activity_name
              , a.activity_type_id      AS activity_type_id
              , a.activity_type_name    AS activity_type_name
              , a.hospital_id           AS hospital_id
              , a.hospital_name         AS hospital_name
              , a.major_id              AS major_id
              , a.major_name            AS major_name
              , a.office_name           AS office_name
              , a.start_time            AS start_time
              , a.end_time              AS end_time
              , p.person_id             AS student_id
              , p.person_name           AS student_name
              , p.evaluation_score_rate AS evaluation_score
              , a.participant_count     AS participant_count
FROM dwd_hainan_hospital_info.dwd_teaching_activity_person_detail_df p
         LEFT JOIN dwd_hainan_hospital_info.dwd_teaching_activity_detail_df a
                   ON p.activity_id = a.activity_id
WHERE p.person_type = '学员';

/*
中文表名：教学活动_老师明细宽表
数据库表名：dwd_teaching_activity_teacher_detail_wide_df
源数据表：
    - ods_hainan_hospital_info.dwd_teaching_activity_person_detail_df
    - ods_hainan_hospital_info.dwd_teaching_activity_detail_df
*/
-- 教学活动_老师明细宽表：删除旧的（如果存在）
DROP TABLE IF EXISTS dwd_hainan_hospital_info.dwd_teaching_activity_teacher_detail_wide_df;

-- 教学活动_老师明细宽表：创建
CREATE TABLE dwd_hainan_hospital_info.dwd_teaching_activity_teacher_detail_wide_df
(
    activity_id        STRING COMMENT '教学活动ID',
    activity_name      STRING COMMENT '教学活动名称',
    activity_type_id   INT COMMENT '教学活动类型ID',
    activity_type_name STRING COMMENT '教学活动类型名称',
    hospital_id        INT COMMENT '教学活动所属医院ID',
    hospital_name      STRING COMMENT '教学活动所属医院',
    major_id           INT COMMENT '教学活动所属专业ID',
    major_name         STRING COMMENT '教学活动所属专业名称',
    office_name        STRING COMMENT '教学活动所属科室',
    start_time         TIMESTAMP COMMENT '教学活动开始时间',
    end_time           TIMESTAMP COMMENT '教学活动结束时间',
    teacher_id         INT COMMENT '老师ID',
    teacher_name       STRING COMMENT '老师名字',
    teacher_title      STRING COMMENT '老师职称'
) COMMENT '教学活动老师明细宽表';

-- 教学活动_老师明细宽表：清空数据
TRUNCATE TABLE dwd_hainan_hospital_info.dwd_teaching_activity_teacher_detail_wide_df;

-- 教学活动_老师明细宽表：插入数据
INSERT INTO dwd_hainan_hospital_info.dwd_teaching_activity_teacher_detail_wide_df
SELECT DISTINCT a.activity_id        AS activity_id
              , a.activity_name      AS activity_name
              , a.activity_type_id   AS activity_type_id
              , a.activity_type_name AS activity_type_name
              , a.hospital_id        AS hospital_id
              , a.hospital_name      AS hospital_name
              , a.major_id           AS major_id
              , a.major_name         AS major_name
              , a.office_name        AS office_name
              , a.start_time         AS start_time
              , a.end_time           AS end_time
              , p.person_id          AS teacher_id
              , p.person_name        AS teacher_name
              , p.professional_title AS teacher_title
FROM dwd_hainan_hospital_info.dwd_teaching_activity_person_detail_df p
         LEFT JOIN dwd_hainan_hospital_info.dwd_teaching_activity_detail_df a
                   ON p.activity_id = a.activity_id
WHERE p.person_type = '老师';

/*
中文表名：教学活动_轮转学员当月教学活动情况宽表
数据库表名：dwd_teaching_activity_round_student_detail_wide_df
源数据表：
    - ods_hainan_hospital_info.dwd_hainan_round_spt_round_total_info
    - ods_hainan_hospital_info.dwd_teaching_activity_student_sign_wide_df
*/

-- 教学活动_轮转学员当月教学活动情况宽表：删除旧的（如果存在）
DROP TABLE IF EXISTS dwd_hainan_hospital_info.dwd_teaching_activity_round_student_detail_wide_df;

-- 教学活动_轮转学员当月教学活动情况宽表：创建
CREATE TABLE dwd_hainan_hospital_info.dwd_teaching_activity_round_student_detail_wide_df
(
    hospital_id         INT COMMENT '医院ID',
    hospital_name       STRING COMMENT '医院名称',
    major_id            INT COMMENT '专业ID',
    major_name          STRING COMMENT '专业名称',
    student_id          INT COMMENT '学生ID',
    student_name        STRING COMMENT '学生名字',
    admission_year      INT COMMENT '招录年份',
    rotation_office     STRING COMMENT '轮转科室',
    rotation_start_time TIMESTAMP COMMENT '轮转开始时间',
    rotation_end_time   TIMESTAMP COMMENT '轮转结束时间',
    rotation_month      STRING COMMENT '轮转月份',
    activity_id         STRING COMMENT '教学活动ID',
    activity_name       STRING COMMENT '教学活动名称',
    activity_type_id    INT COMMENT '教学活动类型ID',
    activity_type_name  STRING COMMENT '教学活动类型名称',
    activity_office     STRING COMMENT '教学活动所属科室',
    activity_start_time TIMESTAMP COMMENT '教学活动开始时间',
    activity_end_time   TIMESTAMP COMMENT '教学活动结束时间',
    activity_month      STRING COMMENT '教学活动月份',
    is_signed           STRING COMMENT '是否签到'
) COMMENT '教学活动_轮转学员当月教学活动情况宽表';

-- 教学活动_轮转学员当月教学活动情况宽表：清空数据
TRUNCATE TABLE dwd_hainan_hospital_info.dwd_teaching_activity_round_student_detail_wide_df;

-- 教学活动_轮转学员当月教学活动情况宽表：插入数据
INSERT INTO dwd_hainan_hospital_info.dwd_teaching_activity_round_student_detail_wide_df
SELECT r.hospital_id
     , r.hospital_name
     , r.major_id
     , r.major_name
     , s.student_id
     , r.student_name
     , r.admission_year
     , r.rotation_office
     , r.rotation_start_time
     , r.rotation_end_time
     , r.rotation_month
     , s.activity_id
     , s.activity_name
     , s.activity_type_id
     , s.activity_type_name
     , s.office_name                        AS activity_office
     , s.start_time                         AS activity_start_time
     , s.end_time                           AS activity_end_time
     , date_format(s.start_time, 'yyyy-MM') AS activity_month
     , s.is_signed
FROM (
         -- 基础数据转换
         SELECT personname     AS student_name
              , CASE
                    WHEN hospital = '301海南医院' THEN 130
                    WHEN hospital = '海南医学院第一附属医院' THEN 110
                    WHEN hospital = '三亚中心医院（海南省第三人民医院）' THEN 111
                    ELSE -1
             END               AS hospital_id
              , CASE
                    WHEN hospital = '301海南医院' THEN '解放军总医院海南医院'
                    WHEN hospital = '海南医学院第一附属医院' THEN '海南医学院第一附属医院'
                    WHEN hospital = '三亚中心医院（海南省第三人民医院）' THEN '三亚中心医院（海南省第三人民医院）'
                    ELSE '其他'
             END               AS hospital_name
              , spt_major_id   AS major_id
              , spt_major_name AS major_name
              , gradeyear      AS admission_year
              , officename     AS rotation_office
              , starttime      AS rotation_start_time
              , endtime        AS rotation_end_time
              , date_format(
                 add_months(
                         to_date(concat(date_format(starttime, 'yyyy-MM'), '-01')),
                         n_table.num
                 ),
                 'yyyy-MM'
                )              AS rotation_month
         FROM dwd_hainan_hospital_info.dwd_hainan_round_spt_round_total_info
                  LATERAL VIEW explode(array(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)) n_table AS num
         WHERE
           -- 修正条件：确保生成的月份不超过实际的轮转月数
             n_table.num <= floor(months_between(endtime, starttime))
           AND starttime IS NOT NULL
           AND endtime IS NOT NULL
           AND starttime <= endtime) r
         LEFT JOIN dwd_hainan_hospital_info.dwd_teaching_activity_student_sign_wide_df s
                   ON r.student_name = s.student_name
                       AND r.hospital_id = s.hospital_id
                       AND r.hospital_name = s.hospital_name
                       AND r.major_id = s.major_id
                       AND r.major_name = s.major_name
                       -- 轮转时间与教学活动时间有交集
                       AND r.rotation_start_time <= s.end_time
                       AND r.rotation_end_time >= s.start_time;


