SELECT
    code AS violation_code,
    definition,
    manhattan_96th_st_below,
    all_other_areas
FROM
    {{ source('parking_raw', 'PARKING_VIOLATION_CODES') }}
