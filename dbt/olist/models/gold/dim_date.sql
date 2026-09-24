-- Calendar 2016-01-01 .. 2020-12-31; date_key = yyyymmdd.
select
    cast(format_datetime(day, 'yyyyMMdd') as integer) as date_key,
    day as date,
    cast(year(day) as integer) as year,
    cast(quarter(day) as integer) as quarter,
    cast(month(day) as integer) as month,
    format_datetime(day, 'MMMM') as month_name,
    cast(week(day) as integer) as iso_week,
    cast(day_of_week(day) as integer) as day_of_week,
    format_datetime(day, 'EEEE') as day_name,
    day_of_week(day) in (6, 7) as is_weekend
from unnest(sequence(date '2016-01-01', date '2020-12-31', interval '1' day)) as t(day)
