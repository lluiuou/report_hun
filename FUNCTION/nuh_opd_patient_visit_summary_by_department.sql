-- DROP FUNCTION public.nuh_opd_patient_visit_summary_by_department(timestamptz, timestamptz, text, text);

CREATE OR REPLACE FUNCTION public.nuh_opd_patient_visit_summary_by_department(p_start_datetime timestamp with time zone, p_end_datetime timestamp with time zone, p_patient_category text DEFAULT NULL::text, p_dept_name text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE plpgsql
AS $function$
DECLARE
    result jsonb;
BEGIN

    WITH params AS ( 
        SELECT
            p_start_datetime AS start_datetime,
            p_end_datetime   AS end_datetime,
            p_patient_category AS patient_category,
            p_dept_name AS dept_name
    ),

    visit_data AS (
        SELECT
            did AS department_id,
            v.vn,
            v.hn,
            CASE
                WHEN NOT EXISTS (
                    SELECT 1 
                    FROM visit v2
                    WHERE v2.hn = v.hn
                      AND v2.created_at < v.created_at
                ) THEN 'NEW'
                ELSE 'OLD'
            END AS p_type
        FROM params p
        CROSS JOIN visit v
        LEFT JOIN insurance_plan ip ON ip.id = v.insurance_plan_ids[1]
        LEFT JOIN LATERAL unnest(v.department_ids) AS did ON true
        WHERE v.active = true
          AND v.deleted_at IS NULL
          AND v.latest_status_code = 'completed'
          AND v.created_at >= p.start_datetime
          AND v.created_at <= p.end_datetime
          AND (
                p.patient_category IS NULL 
                OR ip.nhso_insurance_plan_category_code = p.patient_category
              )
    ),

    summary AS (
        SELECT
            d.id   AS department_id,
            d.name AS department_name,
            COALESCE(COUNT(DISTINCT CASE WHEN vd.p_type = 'NEW' THEN vd.hn END), 0) AS new_patients,
            COALESCE(COUNT(DISTINCT CASE WHEN vd.p_type = 'OLD' THEN vd.hn END), 0) AS old_patients,
            COALESCE(COUNT(DISTINCT vd.hn), 0) AS total_patients,
            COALESCE(COUNT(vd.vn), 0) AS total_visits
        FROM params p
        CROSS JOIN department d
        LEFT JOIN visit_data vd ON vd.department_id = d.id
        WHERE d.active = true
          AND d.deleted_at IS NULL
          AND (
                p.dept_name IS NULL 
                OR d.name = p.dept_name
              )
        GROUP BY d.id, d.name
    ),

    grand_total AS (
        SELECT COALESCE(SUM(total_visits),0) AS grand_total_visits 
        FROM summary
    ),

    final AS (
        SELECT
            department_name,
            new_patients,
            old_patients,
            total_patients,
            total_visits,
            CASE 
                WHEN gt.grand_total_visits > 0
                THEN ROUND((total_visits::numeric / gt.grand_total_visits) * 100, 2)
                ELSE 0
            END AS percent,
            SUM(
                CASE 
                    WHEN gt.grand_total_visits > 0
                    THEN ROUND((total_visits::numeric / gt.grand_total_visits) * 100, 2)
                    ELSE 0
                END
            ) OVER (ORDER BY department_name) AS commulative_view
        FROM summary
        CROSS JOIN grand_total gt
    )

    SELECT jsonb_build_object(
        'meta', jsonb_build_object(
            'start_datetime', p_start_datetime,
            'end_datetime', p_end_datetime,
            'patient_category', p_patient_category,
            'department_filter', p_dept_name,
            'grand_total_visits', gt.grand_total_visits
        ),
        'departments', (
            SELECT jsonb_agg(d.name ORDER BY d.name)
            FROM department d
            WHERE d.active = true 
              AND d.deleted_at IS NULL
        ),
        'content', (
            SELECT jsonb_agg(
                jsonb_build_object(
                    'department', department_name,
                    'new_patients', new_patients,
                    'old_patients', old_patients,
                    'total_patients', total_patients,
                    'total_visits', total_visits,
                    'percent', percent,
                    'commulative_view', commulative_view
                )
                ORDER BY department_name
            )
            FROM final
        )
    )
    INTO result
    FROM grand_total gt;

    RETURN result;
END;
$function$
;
