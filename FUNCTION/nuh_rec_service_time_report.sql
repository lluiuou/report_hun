-- DROP FUNCTION public.nuh_rec_service_time_report(timestamptz, timestamptz);

CREATE OR REPLACE FUNCTION public.nuh_rec_service_time_report(p_start timestamp with time zone, p_end timestamp with time zone)
 RETURNS jsonb
 LANGUAGE plpgsql
AS $function$
DECLARE
    result jsonb;
BEGIN
    WITH visit_data AS (
        SELECT
            ROW_NUMBER() OVER (ORDER BY v.created_at) AS seq_no,
            v.hn AS hn,
            TO_CHAR(v.created_at AT TIME ZONE 'Asia/Bangkok', 'HH24:MI') AS register_time,
            TO_CHAR(q.called_at AT TIME ZONE 'Asia/Bangkok', 'HH24:MI') AS screening_time,
            TO_CHAR(e.created_at AT TIME ZONE 'Asia/Bangkok', 'HH24:MI') AS doctor_time,
            TO_CHAR(e.completed_at AT TIME ZONE 'Asia/Bangkok', 'HH24:MI') AS completed_time,
            ROUND(EXTRACT(EPOCH FROM (e.completed_at - v.created_at)) / 60) AS tat_minutes
        FROM visit v
        JOIN encounter e ON e.vn = v.vn
        LEFT JOIN queue q ON q.en = e.en
        WHERE v.latest_status_code = 'completed'
          AND e.deleted_at IS NULL
          AND v.created_at >= p_start
          AND v.created_at < p_end
    )
    SELECT jsonb_build_object(
        'meta', jsonb_build_object(
            'report_name', 'รายงานระยะเวลาในการรับบริการ',
            'start_datetime', p_start,
            'end_datetime', p_end,
            'total_records', (SELECT COUNT(*) FROM visit_data),
            'generated_at', now()
        ),
        'content',
        (
            SELECT jsonb_agg(
                jsonb_build_object(
                    'seq_no', seq_no,
                    'hn', hn,
                    'register_time', register_time,
                    'screening_time', screening_time,
                    'doctor_time', doctor_time,
                    'completed_time', completed_time,
                    'tat_minutes', tat_minutes
                )
            )
            FROM visit_data
        )
    )
    INTO result;

    RETURN result;
END;
$function$
;
