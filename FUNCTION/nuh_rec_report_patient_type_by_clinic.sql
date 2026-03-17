-- DROP FUNCTION public.nuh_rec_report_patient_type_by_clinic(timestamptz, timestamptz);

CREATE OR REPLACE FUNCTION public.nuh_rec_report_patient_type_by_clinic(p_start_ts timestamp with time zone, p_end_ts timestamp with time zone)
 RETURNS jsonb
 LANGUAGE plpgsql
AS $function$
DECLARE
    result jsonb;
BEGIN
    WITH first_visit AS (
        SELECT hn, MIN(created_at) AS first_created_at
        FROM visit
        GROUP BY hn
    ),
    visit_data AS (
        SELECT
            c.name AS clinic_name,
            v.hn,
            CASE
                WHEN v.created_at = fv.first_created_at THEN 'ใหม่'
                ELSE 'เก่า'
            END AS patient_type
        FROM encounter e
        JOIN visit v ON v.vn = e.vn
        JOIN clinic c ON c.id = e.clinic_id
        JOIN first_visit fv ON fv.hn = v.hn
        WHERE v.latest_status_code = 'completed'
          AND e.deleted_at IS NULL
          AND v.created_at >= p_start_ts
          AND v.created_at < p_end_ts
    ),
    clinic_summary AS (
        SELECT
            clinic_name,
            COUNT(DISTINCT CASE WHEN patient_type = 'ใหม่' THEN hn END) AS new_patients,
            COUNT(DISTINCT CASE WHEN patient_type = 'เก่า' THEN hn END) AS old_patients
        FROM visit_data
        GROUP BY clinic_name
    ),
    summary_data AS (
        SELECT
            clinic_name,
            new_patients,
            old_patients,
            new_patients + old_patients AS total_patients,
            0 AS sort_order
        FROM clinic_summary

        UNION ALL

        SELECT
            'Total',
            SUM(new_patients),
            SUM(old_patients),
            SUM(new_patients + old_patients),
            1
        FROM clinic_summary
    )
    SELECT jsonb_build_object(
        'meta', jsonb_build_object(
            'hospital_name', 'โรงพยาบาลมหาวิทยาลัยนเรศวร',
            'report_name', 'รายงานผู้ป่วยใหม่/เก่า แยกตามคลินิก',
            'start_timestamp', p_start_ts,
            'end_timestamp', p_end_ts
        ),
        'content',
        COALESCE(
            jsonb_agg(
                jsonb_build_object(
                    'clinic_name', clinic_name,
                    'new_patients', new_patients,
                    'old_patients', old_patients,
                    'total_patients', total_patients
                )
                ORDER BY sort_order, clinic_name
            ),
            '[]'::jsonb
        )
    )
    INTO result
    FROM summary_data;

    RETURN result;
END;
$function$
;
