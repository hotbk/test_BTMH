

with base_in as (
  select
    ID_NHAN_VIEN,
    cast(NAM as int64) as NAM,
    cast(THANG as int64) as THANG,
    cast(NGAY_BAT_DAU_LAM_VIEC as date) as NGAY_BAT_DAU_LAM_VIEC,
    cast(NGAY_NGHI_VIEC as date) as NGAY_NGHI_VIEC,
    cast(TRANG_THAI_CONG_VIEC as string) as TRANG_THAI_CONG_VIEC
  from `btmh-airflow-dbt-lab-2026`.`btmh_dwh_fact`.`f_ns`
),


base_adjusted as (
  select
    b.*,
    (
      b.TRANG_THAI_CONG_VIEC = 'Nghỉ việc'
      and extract(day from b.NGAY_NGHI_VIEC) = 1
      and b.NGAY_NGHI_VIEC != b.NGAY_BAT_DAU_LAM_VIEC
    ) as cond_nghi_mung_1,

    case
      when (
        b.TRANG_THAI_CONG_VIEC = 'Nghỉ việc'
        and extract(day from b.NGAY_NGHI_VIEC) = 1
        and b.NGAY_NGHI_VIEC != b.NGAY_BAT_DAU_LAM_VIEC
        and b.THANG = 1
      ) then b.NAM - 1
      else b.NAM
    end as NAM_FINAL,

    case
      when (
        b.TRANG_THAI_CONG_VIEC = 'Nghỉ việc'
        and extract(day from b.NGAY_NGHI_VIEC) = 1
        and b.NGAY_NGHI_VIEC != b.NGAY_BAT_DAU_LAM_VIEC
        and b.THANG > 1
      ) then b.THANG - 1
      when (
        b.TRANG_THAI_CONG_VIEC = 'Nghỉ việc'
        and extract(day from b.NGAY_NGHI_VIEC) = 1
        and b.NGAY_NGHI_VIEC != b.NGAY_BAT_DAU_LAM_VIEC
        and b.THANG = 1
      ) then 12
      else b.THANG
    end as THANG_FINAL
  from base_in b
),

base as (
  select
    ID_NHAN_VIEN,
    NAM_FINAL as NAM,
    THANG_FINAL as THANG,
    NGAY_BAT_DAU_LAM_VIEC,
    NGAY_NGHI_VIEC,
    TRANG_THAI_CONG_VIEC,
    cond_nghi_mung_1
  from base_adjusted
),

multi_status_users as (
  select
    ID_NHAN_VIEN,
    NAM,
    THANG,
    count(distinct TRANG_THAI_CONG_VIEC) as StatusCount
  from base
  where not cond_nghi_mung_1
  group by 1, 2, 3
  having StatusCount = 2
),

multi_status as (
  select
    NAM,
    THANG,
    count(*) as MultiStatusCount
  from multi_status_users
  group by 1, 2
),

thuc_te_agg as (
  select
    NAM,
    THANG,
    count(distinct case when TRANG_THAI_CONG_VIEC = 'Đang làm việc' then ID_NHAN_VIEN end) as ActiveCount,
    count(distinct case when cond_nghi_mung_1 then ID_NHAN_VIEN end) as ResignedCount
  from base
  group by 1, 2
),

thuc_te as (
  select
    t.NAM,
    t.THANG,
    t.ActiveCount,
    t.ResignedCount,
    coalesce(m.MultiStatusCount, 0) as MultiStatusCount
  from thuc_te_agg t
  left join multi_status m
    on t.NAM = m.NAM
   and t.THANG = m.THANG
),

tuyen_moi as (
  select
    NAM,
    THANG,
    count(distinct case
      when extract(month from NGAY_BAT_DAU_LAM_VIEC) = THANG
       and extract(year from NGAY_BAT_DAU_LAM_VIEC) = NAM
      then ID_NHAN_VIEN
    end) as TUYEN_MOI
  from base
  group by 1, 2
),

nghi_viec as (
  select
    NAM,
    THANG,

    count(distinct case
      when TRANG_THAI_CONG_VIEC = 'Nghỉ việc'
       and extract(day from NGAY_NGHI_VIEC) != 1
      then ID_NHAN_VIEN
    end) as Part1,

    count(distinct case
      when extract(day from NGAY_NGHI_VIEC) = 1
       and NGAY_NGHI_VIEC != NGAY_BAT_DAU_LAM_VIEC
       and (
         extract(month from NGAY_NGHI_VIEC) = THANG + 1
         or (THANG = 12 and extract(month from NGAY_NGHI_VIEC) = 1)
       )
       and (
         extract(year from NGAY_NGHI_VIEC) = NAM
         or (THANG = 12 and extract(year from NGAY_NGHI_VIEC) = NAM + 1)
       )
      then ID_NHAN_VIEN
    end) as Part2

  from base
  group by 1, 2
),

lagged as (
  select
    b.*,
    lag(b.TRANG_THAI_CONG_VIEC) over w as Prev_Status,
    lag(b.NAM) over w as Prev_Nam,
    lag(b.THANG) over w as Prev_Thang
  from base b
  window w as (
    partition by b.ID_NHAN_VIEN
    order by b.NAM, b.THANG
  )
),

lagged_diff as (
  select
    l.*,
    (l.NAM * 12 + l.THANG) - (l.Prev_Nam * 12 + l.Prev_Thang) as Month_Diff
  from lagged l
),

thai_san as (
  select
    NAM,
    THANG,
    count(distinct ID_NHAN_VIEN) as NGHI_THAI_SAN
  from lagged_diff
  where TRANG_THAI_CONG_VIEC = 'Nghỉ thai sản'
    and Prev_Status = 'Đang làm việc'
    and Month_Diff = 1
  group by 1, 2
),

di_lam_lai as (
  select
    NAM,
    THANG,
    count(distinct ID_NHAN_VIEN) as DI_LAM_LAI
  from lagged_diff
  where TRANG_THAI_CONG_VIEC = 'Đang làm việc'
    and Prev_Status = 'Nghỉ thai sản'
    and Month_Diff = 1
  group by 1, 2
),

axis as (
  select distinct
    NAM,
    THANG
  from base
)

select
  date(a.NAM, a.THANG, 1) as month_date,
  a.NAM,
  a.THANG,

  (
    coalesce(t.ActiveCount, 0)
    - coalesce(t.ResignedCount, 0)
    - coalesce(t.MultiStatusCount, 0)
  ) as THUC_TE,

  coalesce(tm.TUYEN_MOI, 0) as TUYEN_MOI,

  (coalesce(nv.Part1, 0) + coalesce(nv.Part2, 0)) as NGHI_VIEC,

  coalesce(ts.NGHI_THAI_SAN, 0) as NGHI_THAI_SAN,
  coalesce(dl.DI_LAM_LAI, 0) as DI_LAM_LAI

from axis a
left join thuc_te t
  on a.NAM = t.NAM
 and a.THANG = t.THANG
left join tuyen_moi tm
  on a.NAM = tm.NAM
 and a.THANG = tm.THANG
left join nghi_viec nv
  on a.NAM = nv.NAM
 and a.THANG = nv.THANG
left join thai_san ts
  on a.NAM = ts.NAM
 and a.THANG = ts.THANG
left join di_lam_lai dl
  on a.NAM = dl.NAM
 and a.THANG = dl.THANG