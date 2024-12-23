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
        教学活动明细表 dwd_teaching_activity_detail_df
        教学活动参与人员明细表 dwd_teaching_activity_person_detail_df
        教学活动附件明细表   dwd_teaching_activity_file_detail_df

        -- 基于清洗后的数据拉的宽表 --
        教学活动学员签到宽表  dwd_teaching_activity_student_sign_wide_df
        教学活动学员评价宽表  dwd_teaching_activity_student_evaluation_wide_df
        教学活动老师明细宽表    dwd_teaching_activity_teacher_detail_wide_df
===================================================================*/

/*
    设置Hive执行引擎和MapReduce相关参数
    SET hive.execution.engine=mr;
    SET mapreduce.task.io.sort.mb = 1000;
*/

-- 确保目标数据库存在
CREATE DATABASE IF NOT EXISTS dwd_hainan_hospital_info;

/*
    教学活动明细表
    数据库表名：dwd_teaching_activity_detail_df
    清洗逻辑：
        1. 清洗每个字段的换行符（[\r\n]），使用 `regexp_replace` 去除换行符。
        2. 对空值进行处理，使用 `coalesce` 函数替换空值字段为默认值（如 '其他' 或 -1）。
        3. 保证 `trainid` 非空且大于等于0。
    计算逻辑：
        - 使用 `UNION ALL` 将来自三个医院的数据合并，生成统一的教学活动明细表。
        - 每个活动的 ID 由医院 ID 和 `trainid` 组成，保证活动唯一性。
        - 该表用于记录教学活动的基本信息，包括活动名称、活动类型、开始时间、结束时间、持续时长等。
*/
-- 教学活动明细表：删除旧的（如果存在）
DROP TABLE IF EXISTS dwd_hainan_hospital_info.dwd_teaching_activity_detail_df;

-- 教学活动明细表：创建
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
) COMMENT '教学活动明细表';

-- 教学活动明细表：清空数据
TRUNCATE TABLE dwd_hainan_hospital_info.dwd_teaching_activity_detail_df;

-- 教学活动明细表：插入数据
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
    教学活动参与人员明细表
    数据库表名：dwd_teaching_activity_person_detail_df
    清洗逻辑：
        1. 清洗人员名字、人员类型、职称等字段的换行符（[\r\n"]）。
        2. 对空值进行处理，使用 `coalesce` 函数替换空值字段为默认值（如 '其他'）。
        3. 保证 `trainid` 非空且大于等于0。
        4. 确保人员ID不为空且大于等于0。
    计算逻辑：
        - 使用 `UNION ALL` 将来自三个医院的数据合并，生成统一的教学活动参与人员明细表。
        - 该表用于记录每个教学活动中参与人员的详细信息，包括人员 ID、名字、类型、职称、评分、签到状态等。
*/
-- 教学活动参与人员明细表：删除旧的（如果存在）
DROP TABLE IF EXISTS dwd_hainan_hospital_info.dwd_teaching_activity_person_detail_df;

-- 教学活动参与人员明细表：创建
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

-- 教学活动参与人员明细表：清空数据
TRUNCATE TABLE dwd_hainan_hospital_info.dwd_teaching_activity_person_detail_df;

-- 教学活动参与人员明细表：插入数据
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
    教学活动附件明细表
    数据库表名：dwd_teaching_activity_file_detail_df
    清洗逻辑：
        1. 清洗附件类型、URL、文件名字段的换行符（[\r\n"]）。
        2. 对空值进行处理，使用 `coalesce` 函数替换空值字段为默认值（如 '其他'）。
        3. 保证 `trainid` 非空且大于等于0。
    计算逻辑：
        - 使用 `UNION ALL` 将来自三个医院的数据合并，生成统一的教学活动附件明细表。
        - 该表用于记录与教学活动相关的附件信息，包括附件类型、文件 URL 和文件名称。
*/
-- 教学活动附件明细表：删除旧的（如果存在）
DROP TABLE IF EXISTS dwd_hainan_hospital_info.dwd_teaching_activity_file_detail_df;

-- 教学活动附件明细表：创建
CREATE TABLE dwd_hainan_hospital_info.dwd_teaching_activity_file_detail_df
(
    activity_id   STRING COMMENT '教学活动ID',
    hospital_id   INT COMMENT '所属医院ID',
    hospital_name STRING COMMENT '所属医院',
    file_type     STRING COMMENT '附件类型',
    file_url      STRING COMMENT '附件URL',
    file_name     STRING COMMENT '附件名称'
) COMMENT '教学活动附件明细表';

-- 教学活动附件明细表：清空数据
TRUNCATE TABLE dwd_hainan_hospital_info.dwd_teaching_activity_file_detail_df;

-- 教学活动附件明细表：插入数据
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
    教学活动学员签到宽表
    数据库表名：dwd_teaching_activity_student_sign_wide_df
    清洗逻辑：
        1. 通过 `JOIN` 将学员签到信息与教学活动明细表关联。
        2. 仅选择学员类型的参与人员（`person_type = '学员'`）。
        3. 使用 `date_format` 将签到时间格式化为 "yyyy-MM"。
    计算逻辑：
        - 将每个学员的签到状态和签到日期作为宽表字段，生成每个学员在不同教学活动中的签到记录。
        - 为每个学员生成唯一的签到记录，包括学员的 ID、姓名、是否签到和签到日期。
*/
-- 教学活动学员签到宽表：删除旧的（如果存在）
DROP TABLE IF EXISTS dwd_hainan_hospital_info.dwd_teaching_activity_student_sign_wide_df;

-- 教学活动学员签到宽表：创建
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
    is_signed          STRING COMMENT '是否签到',
    sign_date          STRING COMMENT '签到日期'
) COMMENT '教学活动学员签到宽表';

-- 教学活动学员签到宽表：清空数据
TRUNCATE TABLE dwd_hainan_hospital_info.dwd_teaching_activity_student_sign_wide_df;

-- 教学活动学员签到宽表：插入数据
INSERT INTO dwd_hainan_hospital_info.dwd_teaching_activity_student_sign_wide_df
SELECT DISTINCT a.activity_id                        AS activity_id
              , a.activity_name                      AS activity_name
              , a.activity_type_id                   AS activity_type_id
              , a.activity_type_name                 AS activity_type_name
              , a.hospital_id                        AS hospital_id
              , a.hospital_name                      AS hospital_name
              , a.major_id                           AS major_id
              , a.major_name                         AS major_name
              , a.office_name                        AS office_name
              , a.start_time                         AS start_time
              , a.end_time                           AS end_time
              , p.person_id                          AS student_id
              , p.person_name                        AS student_name
              , p.is_signed                          AS is_signed
              , date_format(a.start_time, 'yyyy-MM') AS sign_date
FROM dwd_hainan_hospital_info.dwd_teaching_activity_person_detail_df p
         JOIN dwd_hainan_hospital_info.dwd_teaching_activity_detail_df a
              ON p.activity_id = a.activity_id
WHERE p.person_type = '学员';


/*
    教学活动学员评价宽表
    数据库表名：dwd_teaching_activity_student_evaluation_wide_df
    清洗逻辑：
        1. 通过 `JOIN` 将学员评价信息与教学活动明细表关联。
        2. 仅选择学员类型的参与人员（`person_type = '学员'`）。
        3. 保证学员评价得分率为有效值。
    计算逻辑：
        - 将每个学员的评价得分与教学活动相关的其他信息一起生成宽表。
        - 记录每个学员的评价得分、所属教学活动、医院、专业等信息。
*/
-- 教学活动学员评价宽表：删除旧的（如果存在）
DROP TABLE IF EXISTS dwd_hainan_hospital_info.dwd_teaching_activity_student_evaluation_wide_df;

-- 教学活动学员评价宽表：创建
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

-- 教学活动学员评价宽表：清空数据
TRUNCATE TABLE dwd_hainan_hospital_info.dwd_teaching_activity_student_evaluation_wide_df;

-- 教学活动学员评价宽表：插入数据
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
         JOIN dwd_hainan_hospital_info.dwd_teaching_activity_detail_df a
              ON p.activity_id = a.activity_id
WHERE p.person_type = '学员';

/*
    教学活动老师明细宽表
    数据库表名：dwd_teaching_activity_teacher_detail_wide_df
    清洗逻辑：
        1. 通过 `JOIN` 将老师信息与教学活动明细表关联。
        2. 仅选择老师类型的参与人员（`person_type = '老师'`）。
    计算逻辑：
        - 将每个老师的信息与教学活动相关的其他信息一起生成宽表。
        - 记录每个老师的职称、所属教学活动、医院、专业等信息。
*/
-- 教学活动老师明细宽表：删除旧的（如果存在）
DROP TABLE IF EXISTS dwd_hainan_hospital_info.dwd_teaching_activity_teacher_detail_wide_df;

-- 教学活动老师明细宽表：创建
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

-- 教学活动老师明细宽表：清空数据
TRUNCATE TABLE dwd_hainan_hospital_info.dwd_teaching_activity_teacher_detail_wide_df;

-- 教学活动老师明细宽表：插入数据
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
         JOIN dwd_hainan_hospital_info.dwd_teaching_activity_detail_df a
              ON p.activity_id = a.activity_id
WHERE p.person_type = '老师';

