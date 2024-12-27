SELECT year
     , sum(total_person_times) AS total_person_times
     , sum(student_count)      AS student_count
     , round(sum(total_person_times) / sum(student_count), 2) AS avg_person_times_per_student
FROM dws_hainan_hospital_info.dws_teaching_activity_person_times_by_hospital_year
GROUP BY year;

