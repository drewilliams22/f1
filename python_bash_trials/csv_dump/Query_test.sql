use f1test;

with 
base as (
select re.race_id + 1 as upcoming_race, ra.year as prior_race_year, re.race_id as prior_race
    , concat(re.race_id, '-', re.driver_id) as comp_key
    , re.driver_id, re.constructor_id
    , re.points as prior_points_driver
    , ds.points as prior_total_points_driver
        , ds.wins as prior_total_wins_driver
        , ds.position as prior_position_driver
        , cs.points as prior_total_points_constructor
        , cs.wins as prior_total_wins_constructor
from results re
  join races ra
    on re.race_id = ra.race_id
  join constructor_standings cs
    on re.race_id = cs.race_id and re.constructor_id = cs.constructor_id
  join driver_standings ds
    on re.race_id = ds.race_id and re.driver_id = ds.driver_id
  where ra.year = 2019
),
step1_zscore as (
  select lt.race_id, lt.driver_id, lt.milliseconds
      , avg(lt.milliseconds) OVER (partition by lt.race_id) as ms_avg_race
      , stddev(lt.milliseconds) OVER (partition by lt.race_id) as sd_ms
      , concat(lt.race_id, '-', lt.driver_id) as fk
  from lap_times lt
    join races ra
      on lt.race_id = ra.race_id
    where ra.year = 2019
),
z_score as (
    select  avg((milliseconds - ms_avg_race) / sd_ms) as scaled_performance, fk
    from step1_zscore
    group by fk
),
quali as (
	select concat(race_id, '-', driver_id) as fk
    , q1, q2, q3
    from qualifying
)
select ra.year as upcoming_race_year -- Just to double check for backtesting (cant predict the first race of the next year with the last race of the prior year)
    , b.*
    , z.scaled_performance as prior_race_scaled_performance
    , q.q1, q.q2, q.q3
    , re.position as pred_position
from base b
  left join races ra
    on b.upcoming_race = ra.race_id
  left join z_score z
    on b.comp_key = z.fk
  left join quali q
	on b.comp_key = q.fk
  left join results re
	on b.upcoming_race = re.race_id and b.driver_id = re.driver_id
order by driver_id asc, prior_race asc
;


select * from results;
