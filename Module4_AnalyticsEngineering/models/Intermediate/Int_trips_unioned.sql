with yellow_data as (
    SELECT * FROM {{ ref('stg_yellow_trip_data') }}
),

green_data as (
    SELECT * FROM {{ ref('stg_green_tripdata') }}
),

unioned_data as (
    SELECT * FROM yellow_data
    UNION ALL
    SELECT * FROM green_data
)

SELECT * FROM unioned_data


