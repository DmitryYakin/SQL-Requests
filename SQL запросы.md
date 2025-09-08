SQL запросы
1.
DO $$
DECLARE
    distinct_patients bigint[] := '{}';
    batch_size int := 50000000;
    min_patient bigint;
    max_patient bigint;
    current_start bigint;
    batch_count int := 0;
BEGIN
    SELECT MIN(patient), MAX(patient) INTO min_patient, max_patient FROM medical_document;
    current_start := min_patient;
    
    WHILE current_start <= max_patient LOOP
        batch_count := batch_count + 1;
        RAISE NOTICE 'Обработка батча %: ID % - %', batch_count, current_start, current_start + batch_size - 1;
        
        SELECT distinct_patients || array_agg(DISTINCT patient::bigint) INTO distinct_patients
        FROM medical_document;
        
        current_start := current_start + batch_size;
        
        IF batch_count % 4 = 0 THEN
            SELECT array_agg(DISTINCT x) INTO distinct_patients 
            FROM unnest(distinct_patients) AS x;
        END IF;
    END LOOP;
    
    SELECT array_agg(DISTINCT x) INTO distinct_patients 
    FROM unnest(distinct_patients) AS x;
    
    RAISE NOTICE 'ИТОГО уникальных patient_id: %', array_length(distinct_patients, 1);
END $$;
2.
select format('https://cloud.webiomed.ru/#/dataset/raw-data/%s', patient_id),
format('https://cloud.webiomed.ru/#/dhra/requests/%s', max(id)),
max(examination_index),
max(data_quality_index),
max(created_date),
count(id)
from ai_request ar
where region_id in = 228
  and examination_index is not null
  and ar.created_date > '2024-09-01'
group by 1
order by 3 desc, 2 desc
;
3.
select count(mdp.id), mdp.hospital_dir_id, nh.short_name
from medical_data_patient mdp 
join nsi_hospitals nh on nh.id = mdp.hospital_dir_id
where mdp.modified_region = '1156373'
group by 2, 3
order by 1 desc
;
4.
SELECT DISTINCT ar.patient_id
FROM ai_request ar
JOIN (
    SELECT ar2.patient_id, MAX(ar2.id) as max_id
    FROM ai_request ar2 
    JOIN mv_medical_patient_18_not_dead mmpnd ON mmpnd.id = ar2.patient_id
    -- WHERE ar2.region_id = 83
    GROUP BY ar2.patient_id
) latest ON ar.id = latest.max_id
WHERE ar.attention_level IN ('red', 'yellow')
;