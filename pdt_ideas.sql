## Difference in Lap times and Position:
SELECT 
  RACE_ID
  , lt.DRIVER_ID
  , lt.LAP
  , lt.POSITION
  , lt.TIME
  , lt.MILLISECONDS
  , d.surname
  , d.forename
  , lt.milliseconds - lag(lt.milliseconds, 1) OVER (PARTITION BY lt.RACE_ID || '-' || lt.DRIVER_ID ORDER BY lt.race_id, lt.driver_id, lt.lap asc) as "Difference in Sequential Lap Times (ms)"
  , lt.position - lag(lt.position, 1) OVER (PARTITION BY lt.RACE_ID || '-' || lt.DRIVER_ID ORDER BY lt.race_id, lt.driver_id, lt.lap asc) as "Positions Gained each lap"
  FROM F1_DB.LAP_TIMES lt
  inner join F1_DB.drivers d
  on lt.driver_id = d.driver_id
  ORDER BY RACE_ID, DRIVER_ID, LAP ASC

## Driver rank by season
SELECT  
    ds.driver_id
    , r.year
    , max(ds.points) as end_of_season_points
    , rank() over(partition by r.year order by year desc, max(ds.points) desc) as current_rank
FROM F1_DB.DRIVER_STANDINGS ds
left join F1_DB.races r
on ds.race_id = r.race_id
left join f1_db.drivers d
on ds.driver_id = d.driver_id
group by r.year, ds.driver_id, d.surname, d.forename

## Constructor rank by Season
select 
  cs.constructor_id
  , max(cs.points)
  , r.year
  , rank() over(partition by r.year order by r.year desc, max(cs.points) desc)
from f1_db.constructor_standings cs
left join f1_db.races r
on cs.race_id = r.race_id
group by cs.constructor_id, r.year
order by r.year desc, max(cs.points) desc
