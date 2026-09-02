-- Bronze: laptop_benchmark_result — raw pass-through
SELECT * FROM {{ source('raw', 'laptop_benchmark_result') }}
