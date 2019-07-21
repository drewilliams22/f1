use f1test;
select * from qualifying;

with 
base as (
	select
		re.race_id + 1 as upcoming_race, ra.year as prior_race_year, re.race_id as prior_race
		, concat(re.race_id, '-', re.driver_id) as comp_key
		, re.driver_id, re.constructor_id
		, re.points as prior_points_driver
		, re.position as prior_position_driver
		, ds.points as prior_total_points_driver
		, ds.wins as prior_total_wins_driver
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
	  select  
		  avg((milliseconds - ms_avg_race) / sd_ms) as scaled_performance, fk
	  from step1_zscore
	  group by fk
),
quali as (
	  select 
		race_id, driver_id
		, substring_index(q1, ':', 1)*60*1000 as q1_min_ms
		, substring_index(substring_index(q1, '.', 1), ':', -1) * 1000 as q1_sec_ms
		, substring_index(q1, '.', -1) as q1_ms
		, substring_index(q2, ':', 1)*60*1000 as q2_min_ms
		, substring_index(substring_index(q2, '.', 1), ':', -1) * 1000 as q2_sec_ms
		, substring_index(q2, '.', -1) as q2_ms
		, substring_index(q3, ':', 1)*60*1000 as q3_min_ms
		, substring_index(substring_index(q3, '.', 1), ':', -1) * 1000 as q3_sec_ms
		, substring_index(q3, '.', -1) as q3_ms
		, position as prior_pole_position_quali
      from qualifying
),
quali_step_1 as (
	  select
		race_id, driver_id
        , prior_pole_position_quali
		, q1_min_ms + q1_sec_ms + q1_ms as q1_ms_tot
        , q2_min_ms + q2_sec_ms + q2_ms as q2_ms_tot
        , q3_min_ms + q3_sec_ms + q3_ms as q3_ms_tot
	  from quali		
),
quali_avgs as (
	  select
		race_id, driver_id
        , prior_pole_position_quali
        , q1_ms_tot
        , q2_ms_tot
        , q3_ms_tot
        , avg(q1_ms_tot) over (partition by race_id) as q1_avg
        , avg(q2_ms_tot) over (partition by race_id) as q2_avg
        , avg(q3_ms_tot) over (partition by race_id) as q3_avg
        , stddev(q1_ms_tot) over (partition by race_id) as q1_sd
        , stddev(q2_ms_tot) over (partition by race_id) as q2_sd
        , stddev(q3_ms_tot) over (partition by race_id) as q3_sd
        from quali_step_1
),
quali_z as (
	  select
		concat(race_id, '-', driver_id) as fk
        , prior_pole_position_quali as prior_pole_position
        , race_id, driver_id
        , (q1_ms_tot - q1_avg)/q1_sd as q1_z
        , (q2_ms_tot - q2_avg)/q2_sd as q2_z
        , (q3_ms_tot - q3_avg)/q3_sd as q3_z
        from quali_avgs
)
select
    d.surname
    , ra.year as upcoming_race_year -- Just to double check for backtesting (cant predict the first race of the next year with the last race of the prior year)
    , b.*
    , z.scaled_performance as prior_race_scaled_performance
    , q.q1_z, q.q2_z, q.q3_z
    , q.prior_pole_position
    , re.position as upcoming_race_result
from base b
  left join driver d
    on b.driver_id = d.driver_id
  left join races ra
    on b.upcoming_race = ra.race_id
  left join z_score z
    on b.comp_key = z.fk
  left join quali_z q
	on b.comp_key = q.fk
  left join results re
	on b.upcoming_race = re.race_id and b.driver_id = re.driver_id
order by driver_id asc, prior_race asc;

select * from constructors;

