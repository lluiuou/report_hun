-- DROP FUNCTION public.nuh_rec_diagnosis_report(timestamptz, timestamptz);

CREATE OR REPLACE FUNCTION public.nuh_rec_diagnosis_report(p_start timestamp with time zone, p_end timestamp with time zone)
 RETURNS jsonb
 LANGUAGE plpgsql
AS $function$
DECLARE
    result jsonb;
BEGIN
    WITH diagnosis_data AS (
        SELECT
            v.hn AS hn,
            dx->>'code' AS diagnosis_code,
            dx->>'display' AS diagnosis_name,
            TO_CHAR(v.created_at AT TIME ZONE 'Asia/Bangkok', 'DD/MM/YYYY') AS visit_date
        FROM visit v,
             jsonb_array_elements(v.diagnosis) AS dx
        WHERE v.latest_status_code = 'completed'
          AND v.deleted_at IS NULL
          AND v.created_at >= (p_start - interval '7 hours')
          AND v.created_at < (p_end - interval '7 hours')
          AND dx->>'code' IS NOT NULL
    )
    SELECT jsonb_build_object(
        'meta', jsonb_build_object(
            'report_name', 'รายงานจำนวนผู้ป่วยตามรหัสโรค (ICD-10)',
            'start_datetime', p_start,
            'end_datetime', p_end,
            'total_records', (SELECT COUNT(*) FROM diagnosis_data),
            'generated_at', now()
        ),
        'content',
        (
            SELECT jsonb_agg(
                jsonb_build_object(
                    'hn', hn,
                    'diagnosis_code', diagnosis_code,
                    'diagnosis_name', diagnosis_name,
                    'visit_date', visit_date
                )
                ORDER BY hn, visit_date
            )
            FROM diagnosis_data
        )
    )
    INTO result;

    RETURN result;
END;
$function$
;
