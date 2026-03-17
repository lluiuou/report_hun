-- DROP FUNCTION public.nuh_opd_by_department_and_scheme(timestamptz, timestamptz, text, text);

CREATE OR REPLACE FUNCTION public.nuh_opd_by_department_and_scheme(p_start_datetime timestamp with time zone, p_end_datetime timestamp with time zone, p_patient_category text DEFAULT NULL::text, p_dept_name text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE plpgsql
AS $function$
DECLARE
    result jsonb;
BEGIN
    WITH date_series AS (
        SELECT generate_series(
            date_trunc('day', p_start_datetime),
            date_trunc('day', p_end_datetime),
            interval '1 day'
        )::date AS visit_date
    ),
    visit_data AS (
        SELECT
            v.created_at::date AS visit_date,
            COALESCE(
                ip.nhso_insurance_plan_category_code,
                CASE
                    WHEN ip.name IN ('เงินสด', 'เงินสด เบิกได้') THEN 'CASH'
                    ELSE 'OTHER'
                END
            ) AS category
        FROM visit v
        LEFT JOIN insurance_plan ip
            ON ip.id = v.insurance_plan_ids[1]
        LEFT JOIN LATERAL unnest(v.department_ids) AS did ON true
        LEFT JOIN department d
            ON d.id = did
        WHERE v.active = true
          AND v.deleted_at IS NULL
          AND v.latest_status_code = 'completed'
          AND v.created_at >= p_start_datetime
          AND v.created_at <= p_end_datetime
          AND (p_dept_name IS NULL OR d.name = p_dept_name)
          AND (
                p_patient_category IS NULL
                OR ip.nhso_insurance_plan_category_code = p_patient_category
              )
    ),
    daily_summary AS (
        SELECT
            ds.visit_date,
            COALESCE(SUM(CASE WHEN vd.category = 'UCS' THEN 1 END), 0) AS ucs,
            COALESCE(SUM(CASE WHEN vd.category = 'SSS' THEN 1 END), 0) AS sss,
            COALESCE(SUM(CASE WHEN vd.category IN ('OFC', 'BKK', 'BMT', 'LGO', 'NHS') THEN 1 END), 0) AS ofc,
            COALESCE(SUM(CASE WHEN vd.category = 'CASH' THEN 1 END), 0) AS cash
        FROM date_series ds
        LEFT JOIN visit_data vd
            ON vd.visit_date = ds.visit_date
        GROUP BY ds.visit_date
    ),
    total_summary AS (
        SELECT
            COALESCE(SUM(CASE WHEN category = 'UCS' THEN 1 END), 0) AS ucs,
            COALESCE(SUM(CASE WHEN category = 'SSS' THEN 1 END), 0) AS sss,
            COALESCE(SUM(CASE WHEN category IN ('OFC', 'BKK', 'BMT', 'LGO', 'NHS') THEN 1 END), 0) AS ofc,
            COALESCE(SUM(CASE WHEN category = 'CASH' THEN 1 END), 0) AS cash
        FROM visit_data
    )
    SELECT jsonb_build_object(
        'meta', jsonb_build_object(
            'report_name', 'รายงานจำนวนผู้ป่วยแยกตามสิทธิการรักษา',
            'start_datetime', p_start_datetime,
            'end_datetime', p_end_datetime,
            'dept_name', COALESCE(p_dept_name, 'ทั้งหมด'),
            'patient_category', COALESCE(p_patient_category, 'ทั้งหมด'),
            'generated_at', now()
        ),
        'content',
        (
            SELECT jsonb_agg(row_data)
            FROM (
                SELECT jsonb_build_object(
                    'วันที่', to_char(visit_date, 'DD/MM/YYYY'),
                    'บัตรทอง', ucs,
                    'ประกันสังคม', sss,
                    'ข้าราชการ', ofc,
                    'ชำระเงินเอง', cash
                )
                FROM daily_summary

                UNION ALL

                SELECT jsonb_build_object(
                    'วันที่', 'รวม',
                    'บัตรทอง', t.ucs,
                    'ประกันสังคม', t.sss,
                    'ข้าราชการ', t.ofc,
                    'ชำระเงินเอง', t.cash
                )
                FROM total_summary t
            ) x(row_data)
        )
    )
    INTO result;

    RETURN result;
END;
$function$
;
