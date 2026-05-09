

with params as (
  select 
(
  
    current_date()
  
)
 as run_date
),

src_ls as (
  select *
  from `btmh-airflow-dbt-lab-2026`.`oneoffice`.`ns_lich_su_cong_viec`
  where dt = (select run_date from params)
),

src_ns as (
  select *
  from `btmh-airflow-dbt-lab-2026`.`oneoffice`.`ns_ho_so_nhan_su`
  where dt = (select run_date from params)
),

joined_raw as (
  select
    cast(ls.ID as string) as ID,
    cast(ls.code as string) as code,
    cast(ls.personnel_id as string) as personnel_id,
    cast(ls.name as string) as name,
    cast(ls.date_from as string) as date_from,
    cast(ls.type as string) as type,

    cast(ls.department_main as string) as department_main,
    cast(ls.department_id as string) as department_id,
    cast(ls.contract_code as string) as contract_code,
    cast(ls.job_contract as string) as job_contract,

    cast(ls.position_id as string) as position_id,
    cast(ls.job_title as string) as job_title,
    cast(ls.company_name as string) as company_name,
    cast(ls.branch_name as string) as branch_name,
    cast(ls.business_name as string) as business_name,
    cast(ls.job_status as string) as job_status,

    ns.birthday,
    ns.gender,
    ns.job_date_join,
    ns.job_date_out,
    cast(ns.job_out_reason as string) as job_out_reason,
    ns.live_manager_id,

    SAFE.PARSE_DATE('%d/%m/%Y', cast(ls.date_from as string)) as date_from_parsed,

    case
      when cast(ls.job_status as string) = 'Làm việc' then 1
      when cast(ls.job_status as string) = 'Nghỉ việc' then 2
      else 3
    end as status_priority

  from src_ls ls
  left join src_ns ns
    on cast(ls.personnel_id as string) = cast(ns.ID as string)
  cross join params p
),

joined as (
  select *
  from joined_raw
  where date_from_parsed is not null
    and date_from_parsed <= (select run_date from params)
    and not (cast(type as string) like '%Kết thúc hợp đồng%')
),

calculated as (
  select
    cast(code as string) as code,
    cast(personnel_id as string) as personnel_id,
    cast(name as string) as name,

    date_from_parsed as date_from,

    cast(department_main as string) as department_main,
    cast(department_id as string) as department_id,
    cast(contract_code as string) as contract_code,
    cast(job_contract as string) as job_contract,

    cast(position_id as string) as position_id,
    cast(job_title as string) as job_title,
    cast(company_name as string) as company_name,
    cast(branch_name as string) as branch_name,
    cast(business_name as string) as business_name,
    cast(job_status as string) as job_status,

    birthday,
    gender,
    job_date_join,
    job_date_out,
    job_out_reason,
    live_manager_id,

    status_priority,

    lead(date_from_parsed) over w_main as next_date_from,
    lead(cast(job_status as string)) over w_main as next_job_status,

    lag(cast(job_status as string)) over w_main as prev_job_status,
    lag(date_from_parsed) over w_main as prev_date_from,

    LAST_VALUE(cast(department_main as string) IGNORE NULLS) over w_ff as department_main_fill,
    LAST_VALUE(cast(department_id as string) IGNORE NULLS) over w_ff as department_id_fill,
    LAST_VALUE(cast(contract_code as string) IGNORE NULLS) over w_ff as contract_code_fill,
    LAST_VALUE(cast(job_contract as string) IGNORE NULLS) over w_ff as job_contract_fill

  from joined
  window
    w_main as (
      partition by cast(personnel_id as string), cast(code as string)
      order by date_from_parsed, status_priority
    ),
    w_ff as (
      partition by cast(personnel_id as string), cast(code as string)
      order by date_from_parsed, status_priority
      rows between unbounded preceding and current row
    )
),

filled as (
  select
    c.code,
    c.personnel_id,
    c.name,
    c.date_from,

    case
      when c.job_status = 'Nghỉ việc' then c.date_from
      when c.job_status != 'Nghỉ việc'
        and c.next_job_status = 'Nghỉ việc'
        and extract(year from c.date_from) = extract(year from c.next_date_from)
        and extract(month from c.date_from) = extract(month from c.next_date_from)
        then last_day(date_add(c.date_from, interval -1 month), month)
      else coalesce(
        last_day(date_add(c.next_date_from, interval -1 month), month),
        (select run_date from params)
      )
    end as date_end,

    coalesce(c.department_main, c.department_main_fill) as department_main,
    coalesce(c.department_id, c.department_id_fill) as department_id,

    c.position_id,
    c.job_title,
    c.company_name,
    c.branch_name,
    c.business_name,
    c.job_status,

    coalesce(c.contract_code, c.contract_code_fill) as contract_code,
    coalesce(c.job_contract, c.job_contract_fill) as job_contract,

    c.birthday,
    c.gender,
    c.job_date_join,
    c.job_date_out,
    c.job_out_reason,
    c.live_manager_id,

    c.status_priority,
    c.prev_job_status,
    c.prev_date_from

  from calculated c
),

filtered as (
  select *
  from filled
  where (job_status != 'Nghỉ việc')
     or (
       job_status = 'Nghỉ việc'
       and (
         prev_job_status is null
         or prev_job_status != 'Nghỉ việc'
         or (
           prev_job_status = 'Nghỉ việc'
           and date_diff(date_trunc(date_from, month), date_trunc(prev_date_from, month), month) > 1
         )
       )
     )
),

exploded as (
  select
    f.*,
    month_date
  from (
    select
      f.*,
      date_trunc(f.date_from, month) as seq_start,
      date_trunc(
        case
          when date_trunc(f.date_end, month) < date_trunc(f.date_from, month)
            then f.date_from
          else f.date_end
        end,
        month
      ) as seq_end
    from filtered f
  ) f
  cross join unnest(generate_date_array(f.seq_start, f.seq_end, interval 1 month)) as month_date
),

final_snapshot as (
  select
    * except(rn_final)
  from (
    select
      e.*,
      row_number() over (
        partition by personnel_id, code, month_date
        order by status_priority desc, date_from desc
      ) as rn_final
    from exploded e
  )
  where rn_final = 1
)

select
  code,
  personnel_id,
  name,
  date_from,
  date_end,
  department_main,
  department_id,
  position_id,
  job_title,
  company_name,
  branch_name,
  business_name,
  job_status,
  contract_code,
  job_contract,
  birthday,
  gender,
  job_date_join,
  job_date_out,
  job_out_reason,
  live_manager_id,
  month_date,
  extract(year from month_date) as year,
  extract(month from month_date) as month
from final_snapshot