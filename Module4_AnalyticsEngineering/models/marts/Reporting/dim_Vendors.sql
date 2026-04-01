WITH trips_unioned AS(
SELECT * 
FROM {{ref('Int_trips_unioned')}}
),

vendors AS(
    SELECT DISTINCT vendorid,
   {{ get_vendornames('vendorid') }} AS vendor_name
    FROM trips_unioned
)

SELECT * FROm Vendors