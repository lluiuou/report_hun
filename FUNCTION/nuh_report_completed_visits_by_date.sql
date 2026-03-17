-- DROP FUNCTION public.nuh_report_completed_visits_by_date(timestamptz, timestamptz);

CREATE OR REPLACE FUNCTION public.nuh_report_completed_visits_by_date(start_time timestamp with time zone, end_time timestamp with time zone)
 RETURNS jsonb
 LANGUAGE sql
 STABLE
AS $function$
    SELECT jsonb_build_object(
        'meta', jsonb_build_object(
            'hospital_name', 'โรงพยาบาลมหาวิทยาลัยนเรศวร',
            'report_name', 'รายงานจำนวนผู้ป่วยที่มาลงทะเบียนประจำวัน',
            'start_time', start_time,
            'end_time', end_time,
            'date_range',
                'ตั้งแต่วันที่ ' ||
                to_char(((start_time AT TIME ZONE 'Asia/Bangkok')::date + interval '543 years'), 'DD/MM/YYYY') ||
                ' - ' ||
                to_char(((end_time   AT TIME ZONE 'Asia/Bangkok')::date + interval '543 years'), 'DD/MM/YYYY'),
            'total', COUNT(*)
        ),
        'content', COALESCE(
            jsonb_agg(
                jsonb_build_object(
                    'clinic_name', c."name",
                    'clinic_id', c.id,
                    'hn', v.hn
                )
            ),
            '[]'::jsonb
        )
    )
    FROM visit v
    JOIN clinic c
        ON c.id = ANY(v.clinic_ids)
    WHERE v.latest_status_code = 'completed'
      AND v.created_at >= start_time
      AND v.created_at < end_time;
$function$
;
