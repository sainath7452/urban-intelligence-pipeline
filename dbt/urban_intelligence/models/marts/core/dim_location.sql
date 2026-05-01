-- dim_location.sql
-- Location dimension with NYC taxi zone categorization
-- Maps known location IDs to zone names and boroughs

WITH locations AS (
    SELECT
        location_id,
        zone_name,
        borough,
        service_zone
    FROM (
        SELECT 1 AS location_id, 'Newark Airport' AS zone_name, 'EWR' AS borough, 'Airports' AS service_zone UNION ALL
        SELECT 132, 'JFK Airport', 'Queens', 'Airports' UNION ALL
        SELECT 138, 'LaGuardia Airport', 'Queens', 'Airports' UNION ALL
        SELECT 161, 'Midtown Center', 'Manhattan', 'Yellow Zone' UNION ALL
        SELECT 162, 'Midtown East', 'Manhattan', 'Yellow Zone' UNION ALL
        SELECT 163, 'Midtown North', 'Manhattan', 'Yellow Zone' UNION ALL
        SELECT 164, 'Midtown South', 'Manhattan', 'Yellow Zone' UNION ALL
        SELECT 230, 'Times Square/Theatre District', 'Manhattan', 'Yellow Zone' UNION ALL
        SELECT 236, 'Upper East Side North', 'Manhattan', 'Yellow Zone' UNION ALL
        SELECT 237, 'Upper East Side South', 'Manhattan', 'Yellow Zone' UNION ALL
        SELECT 238, 'Upper West Side North', 'Manhattan', 'Yellow Zone' UNION ALL
        SELECT 239, 'Upper West Side South', 'Manhattan', 'Yellow Zone' UNION ALL
        SELECT 186, 'Penn Station/Madison Sq West', 'Manhattan', 'Yellow Zone' UNION ALL
        SELECT 170, 'Murray Hill', 'Manhattan', 'Yellow Zone' UNION ALL
        SELECT 48,  'Clinton East', 'Manhattan', 'Yellow Zone' UNION ALL
        SELECT 50,  'Clinton West', 'Manhattan', 'Yellow Zone' UNION ALL
        SELECT 234, 'Union Sq', 'Manhattan', 'Yellow Zone' UNION ALL
        SELECT 79,  'East Village', 'Manhattan', 'Yellow Zone' UNION ALL
        SELECT 249, 'West Village', 'Manhattan', 'Yellow Zone' UNION ALL
        SELECT 113, 'Greenwich Village North', 'Manhattan', 'Yellow Zone'
    )
),

final AS (
    SELECT
        location_id,
        COALESCE(zone_name, 'Unknown Zone')     AS zone_name,
        COALESCE(borough, 'Unknown Borough')    AS borough,
        COALESCE(service_zone, 'Unknown')       AS service_zone,
        CASE
            WHEN service_zone = 'Airports' THEN TRUE
            ELSE FALSE
        END                                     AS is_airport
    FROM locations
)

SELECT * FROM final