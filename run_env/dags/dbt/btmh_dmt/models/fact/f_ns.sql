{{
  config(
    cluster_by=['ID_NHAN_VIEN', 'MA_NHAN_VIEN']
  )
}}

with src as (
  select
    code,
    personnel_id,
    name,
    date_from,
    department_main,
    department_id,
    position_id,
    job_title,
    case
      when trim(cast(job_status as string)) in ('Làm việc', 'Đang làm việc') then 'Đang làm việc'
      else trim(cast(job_status as string))
    end as job_status,
    contract_code,
    job_contract,
    birthday,
    gender,
    safe_cast(job_date_join as date) as job_date_join,
    job_out_reason,
    live_manager_id,
    year,
    month
  from {{ ref('d_nhan_su_2') }}
),

periods as (
  select
    s.*,
    lag(s.job_status) over w_main as prev_job_status,
    case
      when s.job_status = 'Đang làm việc'
        and (lag(s.job_status) over w_main = 'Nghỉ việc' or lag(s.job_status) over w_main is null)
        then 1
      else 0
    end as is_new_period
  from src s
  window w_main as (
    partition by s.personnel_id
    order by s.year, s.month, s.date_from
  )
),

with_period_id as (
  select
    p.*,
    sum(p.is_new_period) over (
      partition by p.personnel_id
      order by p.year, p.month, p.date_from
      rows between unbounded preceding and current row
    ) as employment_period
  from periods p
),

start_dates as (
  select
    code,
    personnel_id,
    employment_period,
    min(date_from) as calculated_start_date
  from with_period_id
  where job_status = 'Đang làm việc'
  group by 1, 2, 3
),

has_quit as (
  select distinct
    personnel_id,
    1 as has_quit
  from src
  where job_status = 'Nghỉ việc'
),

single_quit as (
  select
    personnel_id,
    1 as is_single_quit
  from src
  group by personnel_id
  having count(*) = 1
     and max(case when job_status = 'Nghỉ việc' then 1 else 0 end) = 1
),

joined as (
  select
    main.*,
    sd.calculated_start_date,
    hq.has_quit,
    sq.is_single_quit
  from with_period_id main
  left join start_dates sd
    on main.personnel_id = sd.personnel_id
   and main.employment_period = sd.employment_period
  left join has_quit hq
    on main.personnel_id = hq.personnel_id
  left join single_quit sq
    on main.personnel_id = sq.personnel_id
)

select distinct
  cast(code as string) as MA_NHAN_VIEN,
  cast(year as int64) as NAM,
  cast(month as int64) as THANG,
  cast(personnel_id as string) as ID_NHAN_VIEN,
  cast(name as string) as TEN_NHAN_VIEN,
  cast(date_from as date) as NGAY_BAT_DAU_HOP_DONG,
  cast(department_main as string) as KHOI,
  cast(department_id as string) as PHONG_BAN,
  cast(position_id as string) as VI_TRI_CHUC_DANH,
  cast(job_title as string) as CHUC_DANH,
  cast(job_status as string) as TRANG_THAI_CONG_VIEC,
  cast(contract_code as string) as MA_HOP_DONG_LAO_DONG,
  cast(job_contract as string) as LOAI_HOP_DONG,
  birthday as NAM_SINH,
  cast(gender as string) as GIOI_TINH,

  case
    when is_single_quit = 1 then cast(date_from as date)
    when has_quit = 1 then cast(calculated_start_date as date)
    else cast(job_date_join as date)
  end as NGAY_BAT_DAU_LAM_VIEC,

  case
    when job_status = 'Nghỉ việc' then cast(date_from as date)
    else cast(null as date)
  end as NGAY_NGHI_VIEC,

  cast(job_out_reason as string) as LY_DO_NGHI_VIEC,
  cast(live_manager_id as string) as TEN_QUAN_LY
from joined
