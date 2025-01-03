/*===================================================================
ads层数据整理：
目前已有表：
    教学活动_学员平均参与次数按年度按医院指标表	ads_teaching_activity_student_avg_times_by_year_hospital
    教学活动_学员签到按年度按医院指标表	ads_teaching_activity_student_sign_in_by_year_hospital
    教学活动_学员评价按年度按医院指标表	ads_teaching_activity_student_evaluation_by_year_hospital
    教学活动_开展数量不达标按月按医院按专业报送表	ads_teaching_activity_count_not_standard_by_month_hospital_major_report
    教学活动_名称不规范按月按医院指标表	ads_teaching_activity_name_not_standard_by_month_hospital
    教学活动_不同教学活动名称相同（短期内相似主题）报送表	ads_teaching_activity_different_name_same_report
    教学活动_轮转学员未参加当期轮转专业教学活动报送表	ads_teaching_activity_round_student_not_major_activity_report
====================================================================*/

/*
    设置Hive执行引擎和MapReduce相关参数
    SET hive.execution.engine=mr;
    SET mapreduce.task.io.sort.mb = 1000;
*/

-- 确保目标数据库存在
CREATE DATABASE IF NOT EXISTS ads_hainan_hospital_info;

/*
中文表名：教学活动_学员平均参与次数按年度按医院指标表
ads_hainan_hospital_info.ads_teaching_activity_student_avg_times_by_year_hospital
源数据表：
    - dws_hainan_hospital_info.dws_teaching_activity_data_overview_by_hospital_year
 */

-- 教学活动_学员平均参与次数按年度按医院指标表：删除旧的（如果存在）
DROP TABLE IF EXISTS ads_hainan_hospital_info.ads_teaching_activity_student_avg_times_by_year_hospital;

-- 教学活动_学员平均参与次数按年度按医院指标表：创建
CREATE TABLE IF NOT EXISTS ads_hainan_hospital_info.ads_teaching_activity_student_avg_times_by_year_hospital
(
    year                                INT COMMENT '统计年份',
    hospital_id                         INT COMMENT '医院ID',
    hospital_name                       STRING COMMENT '医院名称',
    activity_count                      INT COMMENT '该年该医院开展的教学活动数量',
    total_person_times                  INT COMMENT '该年该医院总的参与人次',
    student_count                       INT COMMENT '该年该医院参与的学员数',
    avg_activity_times_per_student      DOUBLE COMMENT '该年该医院平均每个学员参与的活动次数',
    avg_activity_times_per_student_rank INT COMMENT '每年该医院平均每人参与活动次数排名'
) COMMENT '教学活动_学员平均参与次数按年度按医院指标表';

-- 教学活动_学员平均参与次数按年度按医院指标表：清空数据
TRUNCATE TABLE ads_hainan_hospital_info.ads_teaching_activity_student_avg_times_by_year_hospital;

-- 教学活动_学员平均参与次数按年度按医院指标表：插入数据
INSERT INTO ads_hainan_hospital_info.ads_teaching_activity_student_avg_times_by_year_hospital
SELECT year
     , hospital_id
     , hospital_name
     , activity_count
     , total_person_times
     , student_count
     , round(total_person_times / student_count, 2)                                                     AS avg_activity_times_per_student
     , row_number() OVER (PARTITION BY year ORDER BY round(total_person_times / student_count, 2) DESC) AS avg_activity_times_per_student_rank
FROM dws_hainan_hospital_info.dws_teaching_activity_data_overview_by_hospital_year;

/*
中文表名：教学活动_学员签到按年度按医院指标表
ads_hainan_hospital_info.ads_teaching_activity_student_sign_in_by_year_hospital
源数据表：
    - dws_hainan_hospital_info.dws_teaching_activity_data_overview_by_hospital_year
*/

-- 教学活动_学员签到按年度按医院指标表：删除旧的（如果存在）
DROP TABLE IF EXISTS ads_hainan_hospital_info.ads_teaching_activity_student_sign_in_by_year_hospital;

-- 教学活动_学员签到按年度按医院指标表：创建
CREATE TABLE IF NOT EXISTS ads_hainan_hospital_info.ads_teaching_activity_student_sign_in_by_year_hospital
(
    year             INT COMMENT '统计年份',
    hospital_id      INT COMMENT '医院ID',
    hospital_name    STRING COMMENT '医院名称',
    total_sign_count INT COMMENT '该年该医院应签到次数',
    sign_count       INT COMMENT '该年该医院签到次数',
    sign_rate        STRING COMMENT '签到率',
    sign_rate_rank   INT COMMENT '签到率排名'
) COMMENT '教学活动_学员签到按年度按医院指标表';

-- 教学活动_学员签到按年度按医院指标表：清空数据
TRUNCATE TABLE ads_hainan_hospital_info.ads_teaching_activity_student_sign_in_by_year_hospital;

-- 教学活动_学员签到按年度按医院指标表：插入数据
INSERT INTO ads_hainan_hospital_info.ads_teaching_activity_student_sign_in_by_year_hospital
SELECT year
     , hospital_id
     , hospital_name
     , total_sign_count
     , sign_count
     , round(sign_count / total_sign_count, 2)                                                     AS sign_rate
     , row_number() OVER (PARTITION BY year ORDER BY round(sign_count / total_sign_count, 2) DESC) AS sign_rate_rank
FROM dws_hainan_hospital_info.dws_teaching_activity_data_overview_by_hospital_year;

/*
中文表名：教学活动_学员评价按年度按医院指标表
ads_hainan_hospital_info.ads_teaching_activity_student_evaluation_by_year_hospital
源数据表：
    - dws_hainan_hospital_info.dws_teaching_activity_data_overview_by_hospital_year
*/

-- 教学活动_学员评价按年度按医院指标表：删除旧的（如果存在）
DROP TABLE IF EXISTS ads_hainan_hospital_info.ads_teaching_activity_student_evaluation_by_year_hospital;

-- 教学活动_学员评价按年度按医院指标表：创建
CREATE TABLE IF NOT EXISTS ads_hainan_hospital_info.ads_teaching_activity_student_evaluation_by_year_hospital
(
    year                   INT COMMENT '统计年份',
    hospital_id            INT COMMENT '医院ID',
    hospital_name          STRING COMMENT '医院名称',
    total_evaluation_count INT COMMENT '该年该医院应评价总数',
    evaluation_count       INT COMMENT '该年该医院评价次数',
    evaluation_rate        STRING COMMENT '评价率',
    evaluation_rate_rank   INT COMMENT '评价率排名'
) COMMENT '教学活动_学员评价按年度按医院指标表';

-- 教学活动_学员评价按年度按医院指标表：清空数据
TRUNCATE TABLE ads_hainan_hospital_info.ads_teaching_activity_student_evaluation_by_year_hospital;

-- 教学活动_学员评价按年度按医院指标表：插入数据
INSERT INTO ads_hainan_hospital_info.ads_teaching_activity_student_evaluation_by_year_hospital
SELECT year
     , hospital_id
     , hospital_name
     , total_evaluation_count
     , evaluation_count
     , round(evaluation_count / total_evaluation_count, 2)                                                     AS evaluation_rate
     , row_number() OVER (PARTITION BY year ORDER BY round(evaluation_count / total_evaluation_count, 2) DESC) AS evaluation_rate_rank
FROM dws_hainan_hospital_info.dws_teaching_activity_data_overview_by_hospital_year;

/*
中文表名：教学活动_开展数量不达标按月按医院按专业报送表
ads_hainan_hospital_info.ads_teaching_activity_count_not_standard_by_month_hospital_major_report
源数据表：
    - dws_hainan_hospital_info.dws_teaching_activity_count_by_hospital_major_type_month
*/

-- 教学活动_开展数量不达标按月按医院按专业报送表：删除旧的（如果存在）
DROP TABLE IF EXISTS ads_hainan_hospital_info.ads_teaching_activity_count_not_standard_by_month_hospital_major_report;

-- 教学活动_开展数量不达标按月按医院按专业报送表：创建
CREATE TABLE IF NOT EXISTS ads_hainan_hospital_info.ads_teaching_activity_count_not_standard_by_month_hospital_major_report
(
    hospital_id   INT COMMENT '医院ID',
    hospital_name STRING COMMENT '医院名称',
    major_id      INT COMMENT '专业ID',
    major_name    STRING COMMENT '专业名称',
    month_list    STRING COMMENT '未达标月份列表'
) COMMENT '教学活动_开展数量不达标按月按医院按专业报送表';

-- 教学活动_开展数量不达标按月按医院按专业报送表：清空数据
TRUNCATE TABLE ads_hainan_hospital_info.ads_teaching_activity_count_not_standard_by_month_hospital_major_report;

-- 教学活动_开展数量不达标按月按医院按专业报送表：插入数据
INSERT INTO ads_hainan_hospital_info.ads_teaching_activity_count_not_standard_by_month_hospital_major_report
SELECT hospital_id
     , hospital_name
     , major_id
     , major_name
     -- 去重并排序后，将列表连接为字符串
     , concat_ws(';',sort_array(collect_set(month))) AS month_list
FROM dws_hainan_hospital_info.dws_teaching_activity_count_by_hospital_major_type_month
WHERE is_standard = 0
GROUP BY hospital_id, hospital_name, major_id, major_name;

/*
中文表名：教学活动_名称不规范按月按医院指标表
ads_hainan_hospital_info.ads_teaching_activity_name_not_standard_by_month_hospital
源数据表：
    - dws_hainan_hospital_info.dws_teaching_activity_name_irregular_count_by_hospital_major_month
*/

-- 教学活动_名称不规范按月按医院指标表：删除旧的（如果存在）
DROP TABLE IF EXISTS ads_hainan_hospital_info.ads_teaching_activity_name_not_standard_by_month_hospital;

-- 教学活动_名称不规范按月按医院指标表：创建
CREATE TABLE IF NOT EXISTS ads_hainan_hospital_info.ads_teaching_activity_name_not_standard_by_month_hospital
(
    month                                     STRING COMMENT '统计月份',
    hospital_id                               INT COMMENT '医院ID',
    hospital_name                             STRING COMMENT '医院名称',
    activity_count                            INT COMMENT '该月该医院开展的教学活动数量',
    standard_count                            INT COMMENT '该月该医院满足规范的教学活动数量',
    standard_count_rate                       DOUBLE COMMENT '该月该医院满足规范的教学活动数量占比',
    single_activity_same_type_name_count      INT COMMENT '该月该医院单教学活动内名称与类型同命名（命名重复）数量',
    single_activity_same_type_name_count_rate DOUBLE COMMENT '该月该医院单教学活动内名称与类型同命名（命名重复）数量占比',
    different_activity_same_name_count        INT COMMENT '该月该医院不同教学活动名称相同相同（短期内相似主题）数量',
    different_activity_same_name_count_rate   DOUBLE COMMENT '该月该医院不同教学活动名称相同（短期内相似主题）数量占比'

) COMMENT '教学活动_名称不规范按月按医院指标表';

-- 教学活动_名称不规范按月按医院指标表：清空数据
TRUNCATE TABLE ads_hainan_hospital_info.ads_teaching_activity_name_not_standard_by_month_hospital;

-- 教学活动_名称不规范按月按医院指标表：插入数据
INSERT INTO ads_hainan_hospital_info.ads_teaching_activity_name_not_standard_by_month_hospital
SELECT month
     , hospital_id
     , hospital_name
     , sum(activity_count)                                                                          AS activity_count
     , sum(standard_count)                                                                          AS standard_count
     , concat(round(sum(standard_count) / sum(activity_count), 2) * 100, '%')                       AS standard_count_rate
     , sum(single_activity_same_type_name_count)                                                    AS single_activity_same_type_name_count
     , concat(round(sum(single_activity_same_type_name_count) / sum(activity_count), 2) * 100, '%') AS single_activity_same_type_name_count_rate
     , sum(different_activity_same_name_count)                                                      AS different_activity_same_name_count
     , concat(round(sum(different_activity_same_name_count) / sum(activity_count), 2) * 100, '%')   AS different_activity_same_name_count_rate
FROM dws_hainan_hospital_info.dws_teaching_activity_name_irregular_count_by_hospital_major_month
GROUP BY month, hospital_id, hospital_name;

/*
中文表名：教学活动_不同教学活动名称相同（短期内相似主题）报送表
ads_hainan_hospital_info.ads_teaching_activity_different_name_same_report
源数据表：
    - dwd_hainan_hospital_info.dwd_teaching_activity_name_abnormal_detail_wide_df
*/

-- 教学活动_不同教学活动名称相同（短期内相似主题）报送表：删除旧的（如果存在）
DROP TABLE IF EXISTS ads_hainan_hospital_info.ads_teaching_activity_different_name_same_report;

-- 教学活动_不同教学活动名称相同（短期内相似主题）报送表：创建
CREATE TABLE IF NOT EXISTS ads_hainan_hospital_info.ads_teaching_activity_different_name_same_report
(
    month                STRING COMMENT '统计月份',
    hospital_id          INT COMMENT '医院ID',
    hospital_name        STRING COMMENT '医院名称',
    major_id             INT COMMENT '专业ID',
    major_name           STRING COMMENT '专业名称',
    activity_id          INT COMMENT '活动ID',
    activity_name        STRING COMMENT '教学活动名称',
    activity_type_id     INT COMMENT '活动类型ID',
    activity_type_name   STRING COMMENT '活动类型名称',
    activity_office_name STRING COMMENT '科室名称',
    teacher_id           INT COMMENT '教师ID',
    teacher_name         STRING COMMENT '教师名称',
    activity_start_time  STRING COMMENT '活动开始时间'
) COMMENT '教学活动_不同教学活动名称相同（短期内相似主题）报送表';

-- 教学活动_不同教学活动名称相同（短期内相似主题）报送表：清空数据
TRUNCATE TABLE ads_hainan_hospital_info.ads_teaching_activity_different_name_same_report;

-- 教学活动_不同教学活动名称相同（短期内相似主题）报送表：插入数据
INSERT INTO ads_hainan_hospital_info.ads_teaching_activity_different_name_same_report
SELECT activity_month         AS month
     , activity_hospital_id   AS hospital_id
     , activity_hospital_name AS hospital_name
     , activity_major_id      AS major_id
     , activity_major_name    AS major_name
     , activity_id
     , activity_name
     , activity_type_id
     , activity_type_name
     , activity_office_name
     , teacher_id
     , teacher_name
     , activity_start_time
FROM dwd_hainan_hospital_info.dwd_teaching_activity_name_abnormal_detail_wide_df
-- 规范类型
-- 0-规范，1-单教学活动内名称与类型同命名，2-不同教学活动名称相同相同（一个月内）
WHERE name_abnormal_type = 2
ORDER BY activity_month DESC, activity_hospital_id, activity_major_id, activity_id;

/*
中文表名：教学活动_轮转学员未参加当期轮转专业教学活动报送表
ads_hainan_hospital_info.ads_teaching_activity_round_student_not_major_activity_report
源数据表：
    - dws_hainan_hospital_info.dws_teaching_activity_round_student_activity_count_by_hospital_month
*/

-- 教学活动_轮转学员未参加当期轮转专业教学活动报送表：删除旧的（如果存在）
DROP TABLE IF EXISTS ads_hainan_hospital_info.ads_teaching_activity_round_student_not_major_activity_report;

-- 教学活动_轮转学员未参加当期轮转专业教学活动报送表：创建
CREATE TABLE IF NOT EXISTS ads_hainan_hospital_info.ads_teaching_activity_round_student_not_major_activity_report
(
    hospital_id            INT COMMENT '医院ID',
    hospital_name          STRING COMMENT '医院名称',
    rotation_major_id      INT COMMENT '轮转专业ID',
    rotation_major_name    STRING COMMENT '轮转专业名称',
    rotation_month         STRING COMMENT '轮转月份',
    student_id             INT COMMENT '学员ID',
    student_name           STRING COMMENT '学员姓名',
    student_admission_year INT COMMENT '学员入学年份',
    rotation_office_name   STRING COMMENT '轮转科室名称',
    rotation_teacher_name  STRING COMMENT '轮转教师姓名',
    rotation_start_time    STRING COMMENT '轮转开始时间',
    rotation_end_time      STRING COMMENT '轮转结束时间'
) COMMENT '教学活动_轮转学员未参加当期轮转专业教学活动报送表';

-- 教学活动_轮转学员未参加当期轮转专业教学活动报送表：清空数据
TRUNCATE TABLE ads_hainan_hospital_info.ads_teaching_activity_round_student_not_major_activity_report;

-- 教学活动_轮转学员未参加当期轮转专业教学活动报送表：插入数据
INSERT INTO ads_hainan_hospital_info.ads_teaching_activity_round_student_not_major_activity_report
SELECT hospital_id
     , hospital_name
     , rotation_office_major_id as rotation_major_id
     , rotation_office_major_name as rotation_major_name
     , rotation_month
     , student_id
     , student_name
     , student_admission_year
     , rotation_office_name
     , rotation_teacher_name
     , rotation_start_time
     , rotation_end_time
FROM dws_hainan_hospital_info.dws_teaching_activity_round_student_activity_count_by_hospital_month
WHERE same_major_activity_count = 0;


