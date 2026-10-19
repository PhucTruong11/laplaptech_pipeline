-- Silver: laptop_benchmark_result — cleaned benchmark scores
-- Casts string/mixed values to proper numeric types

-- Mỗi laptop chọn benchmark ID lớn nhất (quy tắc deterministic, không suy đoán thời gian).
WITH selected AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY laptop_model_id ORDER BY id::bigint DESC) AS version_rank
    FROM {{ ref('bronze_laptop_benchmark_result') }}
    WHERE laptop_model_id IS NOT NULL
)
SELECT
    id::int                                     AS benchmark_id,
    laptop_model_id::int                        AS laptop_model_id,

    -- Battery benchmarks (minutes)
    office_battery_result_minutes::numeric      AS office_battery_minutes,
    gaming_battery_result_minutes::numeric      AS gaming_battery_minutes,

    -- Geekbench scores
    geekbench_6_cpu_single_core_plugged_in::int AS geekbench6_single,
    geekbench_6_cpu_multi_core_plugged_in::int  AS geekbench6_multi,
    geekbench_6_cpu_multi_core_battery::int     AS geekbench6_multi_battery,

    -- Derived: hours for readability
    ROUND(office_battery_result_minutes::numeric / 60.0, 1)
                                                AS office_battery_hours,
    ROUND(gaming_battery_result_minutes::numeric / 60.0, 1)
                                                AS gaming_battery_hours

FROM selected
WHERE version_rank = 1
