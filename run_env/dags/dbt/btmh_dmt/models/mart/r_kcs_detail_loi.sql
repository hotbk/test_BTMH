{{
  config(
    partition_by={"field": "report_date", "data_type": "date"},
    cluster_by=["nguon", "ma_loi"]
  )
}}

with
kcs as (
  select *
  from {{ ref('f_kcs_merged') }}
),

h as (
  select
    ma_hang,
    ten_hang,
    id_nhom,
    id_dai_chi
  from {{ ref('d_hang') }}
),

n as (
  select id, ma_nhom
  from {{ ref('d_nhom') }}
),

dt as (
  select ma_dt, ten_dt
  from {{ ref('d_doi_tac') }}
),

missing_dai_chi as (
  select distinct
    kcs.id_nhom_cchh as id_nhom_cchh
  from kcs
  left join h
    on kcs.ma_hang = h.ma_hang
  left join n
    on h.id_nhom = n.id
  where n.ma_nhom is null
),

cte_candidates as (
  select
    h.id_dai_chi,
    n.ma_nhom,
    count(1) as so_ma_hang
  from h
  left join n
    on h.id_nhom = n.id
  join missing_dai_chi m
    on cast(h.id_dai_chi as string) = cast(m.id_nhom_cchh as string)
  where n.ma_nhom is not null
  group by 1, 2
),

cte as (
  select
    id_dai_chi,
    ma_nhom as cte_ma_nhom
  from cte_candidates
  qualify row_number() over (
    partition by id_dai_chi
    order by so_ma_hang desc
  ) = 1
),

kcs_joined as (
  select
    date({{ btmh_to_timestamp_any('kcs.ngay_kcs_k1') }}) as report_date,
    cast(kcs.id as string) as id,
    cast(kcs.ma_lo as string) as ma_lo,
    cast(kcs.ma_hang as string) as ma_hang,
    cast(kcs.ma_vach as string) as ma_vach,
    cast(coalesce(h.ten_hang, kcs.ten_hang) as string) as ten_hang,
    cast(coalesce(n.ma_nhom, cte.cte_ma_nhom) as string) as nhom_hang,
    cast(kcs.id_nhom_cchh as string) as id_dai_chi,
    cast(kcs.nguon_nhap as string) as ma_nha_cung_cap,
    dt.ten_dt as ten_nha_cung_cap,
    -- Avoid ambiguous column names from `kcs.*` (BigQuery doesn't allow duplicate
    -- column names in a SELECT list).
    -- Only keep the error measure columns needed downstream.
    kcs.err_1,
    kcs.err_2,
    kcs.err_3,
    kcs.err_4,
    kcs.err_5,
    kcs.err_6,
    kcs.err_7,
    kcs.err_8,
    kcs.err_9,
    kcs.err_10,
    kcs.err_11,
    kcs.err_12,
    kcs.err_13,
    kcs.err_14,
    kcs.err_15,
    kcs.err_16,
    kcs.err_17,
    kcs.err_18,
    kcs.err_19,
    kcs.err_20,
    kcs.err_21,
    kcs.err_22,
    kcs.err_23,
    kcs.err_24
  from kcs
  left join h
    on kcs.ma_hang = h.ma_hang
  left join n
    on h.id_nhom = n.id
  left join dt
    on cast(kcs.nguon_nhap as string) = dt.ma_dt
  left join cte
    on cast(kcs.id_nhom_cchh as string) = cast(cte.id_dai_chi as string)
),

api_unpivot as (
  select
    report_date,
    'api' as nguon,
    id,
    ma_lo,
    ma_hang,
    ma_vach,
    ten_hang,
    nhom_hang,
    id_dai_chi,
    ma_nha_cung_cap,
    ten_nha_cung_cap,
    x.ma_loi,
    x.ten_loi,
    sum(x.so_luong) as so_luong
  from kcs_joined
  cross join unnest([
    struct('Err_1' as ma_loi, 'Chưa xi trắng Rodium' as ten_loi, coalesce(safe_cast(err_1 as int64), cast(safe_cast(err_1 as float64) as int64), 0) as so_luong),
    struct('Err_2' as ma_loi, 'Lỗi Xi Rodium chưa đạt' as ten_loi, coalesce(safe_cast(err_2 as int64), cast(safe_cast(err_2 as float64) as int64), 0) as so_luong),
    struct('Err_3' as ma_loi, 'Lỗi Phay hỏng' as ten_loi, coalesce(safe_cast(err_3 as int64), cast(safe_cast(err_3 as float64) as int64), 0) as so_luong),
    struct('Err_4' as ma_loi, 'Lỗi mẫu' as ten_loi, coalesce(safe_cast(err_4 as int64), cast(safe_cast(err_4 as float64) as int64), 0) as so_luong),
    struct('Err_5' as ma_loi, 'Lỗi Méo phom' as ten_loi, coalesce(safe_cast(err_5 as int64), cast(safe_cast(err_5 as float64) as int64), 0) as so_luong),
    struct('Err_6' as ma_loi, 'Lỗi bị rỗ' as ten_loi, coalesce(safe_cast(err_6 as int64), cast(safe_cast(err_6 as float64) as int64), 0) as so_luong),
    struct('Err_7' as ma_loi, 'Lỗi sửa nguội chưa kỹ' as ten_loi, coalesce(safe_cast(err_7 as int64), cast(safe_cast(err_7 as float64) as int64), 0) as so_luong),
    struct('Err_8' as ma_loi, 'Lỗi đánh bóng chưa kỹ' as ten_loi, coalesce(safe_cast(err_8 as int64), cast(safe_cast(err_8 as float64) as int64), 0) as so_luong),
    struct('Err_9' as ma_loi, 'Lỗi cườm chưa bóng' as ten_loi, coalesce(safe_cast(err_9 as int64), cast(safe_cast(err_9 as float64) as int64), 0) as so_luong),
    struct('Err_10' as ma_loi, 'Lỗi phun cát không đều màu' as ten_loi, coalesce(safe_cast(err_10 as int64), cast(safe_cast(err_10 as float64) as int64), 0) as so_luong),
    struct('Err_11' as ma_loi, 'Lỗi khoá' as ten_loi, coalesce(safe_cast(err_11 as int64), cast(safe_cast(err_11 as float64) as int64), 0) as so_luong),
    struct('Err_12' as ma_loi, 'Lỗi bản lề' as ten_loi, coalesce(safe_cast(err_12 as int64), cast(safe_cast(err_12 as float64) as int64), 0) as so_luong),
    struct('Err_13' as ma_loi, 'Lỗi khuyên nối' as ten_loi, coalesce(safe_cast(err_13 as int64), cast(safe_cast(err_13 as float64) as int64), 0) as so_luong),
    struct('Err_14' as ma_loi, 'Lỗi chốt bông không chắc chắn' as ten_loi, coalesce(safe_cast(err_14 as int64), cast(safe_cast(err_14 as float64) as int64), 0) as so_luong),
    struct('Err_15' as ma_loi, 'Hỏng ren vặn' as ten_loi, coalesce(safe_cast(err_15 as int64), cast(safe_cast(err_15 as float64) as int64), 0) as so_luong),
    struct('Err_16' as ma_loi, 'Ố, ám, xước, cũ, mờ, xỉn, bẩn' as ten_loi, coalesce(safe_cast(err_16 as int64), cast(safe_cast(err_16 as float64) as int64), 0) as so_luong),
    struct('Err_17' as ma_loi, 'Rơi đá, vỡ đá. đá mù. đá lắp lệch,chấu đá' as ten_loi, coalesce(safe_cast(err_17 as int64), cast(safe_cast(err_17 as float64) as int64), 0) as so_luong),
    struct('Err_18' as ma_loi, 'Lỗi dấu đóng' as ten_loi, coalesce(safe_cast(err_18 as int64), cast(safe_cast(err_18 as float64) as int64), 0) as so_luong),
    struct('Err_19' as ma_loi, 'Nứt vỡ' as ten_loi, coalesce(safe_cast(err_19 as int64), cast(safe_cast(err_19 as float64) as int64), 0) as so_luong),
    struct('Err_20' as ma_loi, 'Gẫy, đứt, mòn, mất nét.' as ten_loi, coalesce(safe_cast(err_20 as int64), cast(safe_cast(err_20 as float64) as int64), 0) as so_luong),
    struct('Err_21' as ma_loi, 'Sai tuổi quy địnḥ (không đủ tuổi)' as ten_loi, coalesce(safe_cast(err_21 as int64), cast(safe_cast(err_21 as float64) as int64), 0) as so_luong),
    struct('Err_22' as ma_loi, 'Móp, méo, lõm, bẹp, rách.' as ten_loi, coalesce(safe_cast(err_22 as int64), cast(safe_cast(err_22 as float64) as int64), 0) as so_luong),
    struct('Err_23' as ma_loi, 'Sai số tay.' as ten_loi, coalesce(safe_cast(err_23 as int64), cast(safe_cast(err_23 as float64) as int64), 0) as so_luong),
    struct('Err_24' as ma_loi, 'Lỗi khác' as ten_loi, coalesce(safe_cast(err_24 as int64), cast(safe_cast(err_24 as float64) as int64), 0) as so_luong)
  ]) x
  where
    report_date is not null
    and extract(year from report_date) = extract(year from current_date())
  group by
    report_date,
    nguon,
    id,
    ma_lo,
    ma_hang,
    ma_vach,
    ten_hang,
    nhom_hang,
    id_dai_chi,
    ma_nha_cung_cap,
    ten_nha_cung_cap,
    x.ma_loi,
    x.ten_loi
),

-- Trang sức lỗi (GGS)
tsl_src as (
  select *
  from {{ source('ggs', 'ggs_trang_suc_loi') }}
),

tsl_prep as (
  select
    date({{ btmh_to_timestamp_any('tsl.ngay_kiem_tra') }}) as report_date,
    concat('TS-', coalesce(cast(safe_cast(tsl.stt as int64) as string), cast(tsl.stt as string))) as id,
    cast(tsl.nhom_lon as string) as nhom_hang,
    cast(tsl.ma_nha_cung_cap as string) as ma_nha_cung_cap,
    ddt.ten_dt as ten_nha_cung_cap,
    -- Prevent duplicate `ma_nha_cung_cap` (selected above) from `tsl.*`.
    tsl.* except (ma_nha_cung_cap)
  from tsl_src tsl
  left join dt ddt
    on cast(tsl.ma_nha_cung_cap as string) = ddt.ma_dt
),

tsl_unpivot as (
  select
    report_date,
    'ggs' as nguon,
    id,
    cast(null as string) as ma_lo,
    cast(null as string) as ma_hang,
    cast(null as string) as ma_vach,
    cast(null as string) as ten_hang,
    nhom_hang,
    cast(null as string) as id_dai_chi,
    ma_nha_cung_cap,
    ten_nha_cung_cap,
    x.ma_loi,
    x.ten_loi,
    sum(x.so_luong) as so_luong
  from tsl_prep
  cross join unnest([
    struct('Err_1' as ma_loi, 'Lỗi Xi Rodium chưa đạt' as ten_loi, coalesce(cast(safe_cast(loi_xi_rodium_chua_dat as float64) as int64), 0) as so_luong),
    struct('Err_2' as ma_loi, 'Chưa Xi trắng Rodium' as ten_loi, coalesce(cast(safe_cast(chua_xi_trang_rodium as float64) as int64), 0) as so_luong),
    struct('Err_3' as ma_loi, 'Lỗi phay hỏng' as ten_loi, coalesce(cast(safe_cast(loi_phay_hong as float64) as int64), 0) as so_luong),
    struct('Err_4' as ma_loi, 'Lỗi phun cát không đều màu' as ten_loi, coalesce(cast(safe_cast(loi_phun_cat_khong_deu_mau as float64) as int64), 0) as so_luong),
    struct('Err_5' as ma_loi, 'Lỗi mẫu' as ten_loi, coalesce(cast(safe_cast(loi_mau as float64) as int64), 0) as so_luong),
    struct('Err_6' as ma_loi, 'Lỗi méo phom' as ten_loi, coalesce(cast(safe_cast(loi_meo_phom as float64) as int64), 0) as so_luong),
    struct('Err_7' as ma_loi, 'Lỗi bị rỗ' as ten_loi, coalesce(cast(safe_cast(loi_bi_ro as float64) as int64), 0) as so_luong),
    struct('Err_8' as ma_loi, 'Lỗi sửa nguội chưa kỹ' as ten_loi, coalesce(cast(safe_cast(loi_sua_nguoi_chua_ky as float64) as int64), 0) as so_luong),
    struct('Err_9' as ma_loi, 'Lỗi đánh bóng chưa kỹ' as ten_loi, coalesce(cast(safe_cast(loi_danh_bong_chua_ky as float64) as int64), 0) as so_luong),
    struct('Err_10' as ma_loi, 'Lỗi cườm chưa bóng' as ten_loi, coalesce(cast(safe_cast(loi_cuom_chua_bong as float64) as int64), 0) as so_luong),
    struct('Err_11' as ma_loi, 'Lỗi khoá' as ten_loi, coalesce(cast(safe_cast(loi_khoa as float64) as int64), 0) as so_luong),
    struct('Err_12' as ma_loi, 'Lỗi bản lề' as ten_loi, coalesce(cast(safe_cast(loi_ban_le as float64) as int64), 0) as so_luong),
    struct('Err_13' as ma_loi, 'Lỗi khuyên nối' as ten_loi, coalesce(cast(safe_cast(loi_khuyen_noi as float64) as int64), 0) as so_luong),
    struct('Err_14' as ma_loi, 'Hỏng ren vặn' as ten_loi, coalesce(cast(safe_cast(hong_ren_van as float64) as int64), 0) as so_luong),
    struct('Err_15' as ma_loi, 'Lỗi chốt bông không chắc chắn' as ten_loi, coalesce(cast(safe_cast(loi_chot_bong_khong_chac_chan as float64) as int64), 0) as so_luong),
    struct('Err_16' as ma_loi, 'Gẫy, đứt, mòn, mất nét.' as ten_loi, coalesce(cast(safe_cast(gay_dut_mon_mat_net as float64) as int64), 0) as so_luong),
    struct('Err_17' as ma_loi, 'Móp, méo, lõm, bẹp, rách.' as ten_loi, coalesce(cast(safe_cast(mop_meo_lom_bep_rach as float64) as int64), 0) as so_luong),
    struct('Err_18' as ma_loi, 'Sai tuổi quy địnḥ (không đủ tuổi)' as ten_loi, coalesce(cast(safe_cast(sai_tuoi_quy_dinh as float64) as int64), 0) as so_luong),
    struct('Err_19' as ma_loi, 'Lỗi dấu đóng' as ten_loi, coalesce(cast(safe_cast(loi_dau_dong as float64) as int64), 0) as so_luong),
    struct('Err_20' as ma_loi, 'Sai số tay.' as ten_loi, coalesce(cast(safe_cast(sai_so_tay as float64) as int64), 0) as so_luong),
    struct('Err_21' as ma_loi, 'Nứt vỡ' as ten_loi, coalesce(cast(safe_cast(nut_vo as float64) as int64), 0) as so_luong),
    struct('Err_22' as ma_loi, 'Lỗi không cấn đối' as ten_loi, coalesce(cast(safe_cast(loi_khong_can_doi as float64) as int64), 0) as so_luong),
    struct('Err_23' as ma_loi, 'Lỗi đai' as ten_loi, coalesce(cast(safe_cast(loi_dai as float64) as int64), 0) as so_luong),
    struct('Err_24' as ma_loi, 'Lỗi cạnh' as ten_loi, coalesce(cast(safe_cast(loi_canh as float64) as int64), 0) as so_luong),
    struct('Err_25' as ma_loi, 'Lỗi bông tai cùng chiều' as ten_loi, coalesce(cast(safe_cast(loi_bong_tai_cung_chieu as float64) as int64), 0) as so_luong),
    struct('Err_26' as ma_loi, 'Lỗi cọc bông tai' as ten_loi, coalesce(cast(safe_cast(loi_coc_bong_tai as float64) as int64), 0) as so_luong),
    struct('Err_27' as ma_loi, 'Lỗi bị thủng' as ten_loi, coalesce(cast(safe_cast(loi_bi_thung as float64) as int64), 0) as so_luong),
    struct('Err_28' as ma_loi, 'Lỗi mất nét' as ten_loi, coalesce(cast(safe_cast(loi_mat_net as float64) as int64), 0) as so_luong),
    struct('Err_29' as ma_loi, 'Lỗi dây đỡ' as ten_loi, coalesce(cast(safe_cast(loi_day_do as float64) as int64), 0) as so_luong),
    struct('Err_30' as ma_loi, 'Lỗi ố ám đổi màu' as ten_loi, coalesce(cast(safe_cast(loi_o_am_doi_mau as float64) as int64), 0) as so_luong),
    struct('Err_31' as ma_loi, 'Lỗi đen xỉn' as ten_loi, coalesce(cast(safe_cast(loi_den_xin as float64) as int64), 0) as so_luong),
    struct('Err_32' as ma_loi, 'Lỗi xước' as ten_loi, coalesce(cast(safe_cast(loi_xuoc as float64) as int64), 0) as so_luong),
    struct('Err_33' as ma_loi, 'Lỗi bẩn, cũ, không bóng' as ten_loi, coalesce(cast(safe_cast(loi_ban_cu_khong_bong as float64) as int64), 0) as so_luong),
    struct('Err_34' as ma_loi, 'Lỗi rơi đá' as ten_loi, coalesce(cast(safe_cast(loi_roi_da as float64) as int64), 0) as so_luong),
    struct('Err_35' as ma_loi, 'Lỗi đá lắp lệch' as ten_loi, coalesce(cast(safe_cast(loi_da_lap_lech as float64) as int64), 0) as so_luong),
    struct('Err_36' as ma_loi, 'Lỗi đá vỡ' as ten_loi, coalesce(cast(safe_cast(loi_da_vo as float64) as int64), 0) as so_luong),
    struct('Err_37' as ma_loi, 'Lỗi đá mù, không bắt sáng' as ten_loi, coalesce(cast(safe_cast(loi_da_mu_khong_bat_sang as float64) as int64), 0) as so_luong),
    struct('Err_38' as ma_loi, 'Lỗi dấu lệch' as ten_loi, coalesce(cast(safe_cast(loi_dau_lech as float64) as int64), 0) as so_luong),
    struct('Err_39' as ma_loi, 'Lỗi thiếu dấu' as ten_loi, coalesce(cast(safe_cast(loi_thieu_dau as float64) as int64), 0) as so_luong),
    struct('Err_40' as ma_loi, 'Lỗi sai dấu quy định' as ten_loi, coalesce(cast(safe_cast(loi_sai_dau_quy_dinh as float64) as int64), 0) as so_luong),
    struct('Err_41' as ma_loi, 'Lỗi lõm, móp, bẹp' as ten_loi, coalesce(cast(safe_cast(loi_lom_mop_bep as float64) as int64), 0) as so_luong),
    struct('Err_42' as ma_loi, 'Lỗi rách' as ten_loi, coalesce(cast(safe_cast(loi_rach as float64) as int64), 0) as so_luong),
    struct('Err_43' as ma_loi, 'Lỗi thủng' as ten_loi, coalesce(cast(safe_cast(loi_thung as float64) as int64), 0) as so_luong),
    struct('Err_44' as ma_loi, 'Lỗi sai kích thước vòng' as ten_loi, coalesce(cast(safe_cast(loi_sai_kich_thuoc_vong as float64) as int64), 0) as so_luong),
    struct('Err_45' as ma_loi, 'Lỗi sai kích thước kiềng' as ten_loi, coalesce(cast(safe_cast(loi_sai_kich_thuoc_kieng as float64) as int64), 0) as so_luong),
    struct('Err_46' as ma_loi, 'Lỗi mép ổ đá' as ten_loi, coalesce(cast(safe_cast(loi_mep_o_da as float64) as int64), 0) as so_luong),
    struct('Err_47' as ma_loi, 'Lỗi chỉ chân' as ten_loi, coalesce(cast(safe_cast(loi_chi_chan as float64) as int64), 0) as so_luong),
    struct('Err_48' as ma_loi, 'Ố, ám, xước, cũ, mờ, xỉn, bẩn' as ten_loi, coalesce(cast(safe_cast(o_am_xuoc_cu_mo_xin_ban as float64) as int64), 0) as so_luong),
    struct('Err_49' as ma_loi, 'Rơi đá, vỡ đá. đá mù. đá lắp lệch,chấu đá' as ten_loi, coalesce(cast(safe_cast(roi_da_vo_da_da_mu_da_lap_lechchau_da as float64) as int64), 0) as so_luong)
  ]) x
  where
    report_date is not null
    and extract(year from report_date) = extract(year from current_date())
  group by
    report_date,
    nguon,
    id,
    ma_lo,
    ma_hang,
    ma_vach,
    ten_hang,
    nhom_hang,
    id_dai_chi,
    ma_nha_cung_cap,
    ten_nha_cung_cap,
    x.ma_loi,
    x.ten_loi
),

-- Tích trữ lỗi (GGS)
ttl_src as (
  select *
  from {{ source('ggs', 'ggs_tich_tru_loi') }}
),

ttl_prep as (
  select
    date({{ btmh_to_timestamp_any('ttl.ngay_kiem_tra') }}) as report_date,
    concat('TT-', coalesce(cast(safe_cast(ttl.stt as int64) as string), cast(ttl.stt as string))) as id,
    cast(ttl.nhom_lon as string) as nhom_hang,
    cast(ttl.lo_nhap as string) as ma_nha_cung_cap,
    ddt.ten_dt as ten_nha_cung_cap,
    ttl.*
  from ttl_src ttl
  left join dt ddt
    on cast(ttl.lo_nhap as string) = ddt.ma_dt
),

ttl_unpivot as (
  select
    report_date,
    'ggs' as nguon,
    id,
    cast(null as string) as ma_lo,
    cast(null as string) as ma_hang,
    cast(null as string) as ma_vach,
    cast(null as string) as ten_hang,
    nhom_hang,
    cast(null as string) as id_dai_chi,
    ma_nha_cung_cap,
    ten_nha_cung_cap,
    x.ma_loi,
    x.ten_loi,
    sum(x.so_luong) as so_luong
  from ttl_prep
  cross join unnest([
    struct('Err_1' as ma_loi, 'Lỗi vỉ, lỗi viền' as ten_loi, coalesce(cast(safe_cast(loi_vi_loi_vien as float64) as int64), 0) as so_luong),
    struct('Err_2' as ma_loi, 'Tem vàng' as ten_loi, coalesce(cast(safe_cast(tem_vang as float64) as int64), 0) as so_luong),
    struct('Err_3' as ma_loi, 'Tem đỏ' as ten_loi, coalesce(cast(safe_cast(tem_do as float64) as int64), 0) as so_luong),
    struct('Err_4' as ma_loi, 'Rách tem' as ten_loi, coalesce(cast(safe_cast(rach_tem as float64) as int64), 0) as so_luong),
    struct('Err_5' as ma_loi, 'Phồng vỉ' as ten_loi, coalesce(cast(safe_cast(phong_vi as float64) as int64), 0) as so_luong),
    struct('Err_6' as ma_loi, 'Ép  lệch' as ten_loi, coalesce(cast(safe_cast(ep_lech as float64) as int64), 0) as so_luong),
    struct('Err_7' as ma_loi, 'Mối hàn' as ten_loi, coalesce(cast(safe_cast(moi_han as float64) as int64), 0) as so_luong),
    struct('Err_8' as ma_loi, 'Trọng lượng' as ten_loi, coalesce(cast(safe_cast(trong_luong as float64) as int64), 0) as so_luong),
    struct('Err_9' as ma_loi, 'Tuổi vàng' as ten_loi, coalesce(cast(safe_cast(tuoi_vang as float64) as int64), 0) as so_luong),
    struct('Err_10' as ma_loi, 'Méo lệch' as ten_loi, coalesce(cast(safe_cast(meo_lech as float64) as int64), 0) as so_luong),
    struct('Err_11' as ma_loi, 'Độ bóng' as ten_loi, coalesce(cast(safe_cast(do_bong as float64) as int64), 0) as so_luong),
    struct('Err_12' as ma_loi, 'Rỗ, sứt, lồi, lõm, ,sần' as ten_loi, coalesce(cast(safe_cast(ro_sut_loi_lom_san as float64) as int64), 0) as so_luong),
    struct('Err_13' as ma_loi, 'Ố đen' as ten_loi, coalesce(cast(safe_cast(o_den as float64) as int64), 0) as so_luong),
    struct('Err_14' as ma_loi, 'Dấu mờ mất nét' as ten_loi, coalesce(cast(safe_cast(dau_mo_mat_net as float64) as int64), 0) as so_luong),
    struct('Err_15' as ma_loi, 'Chữ số trong lòng nhẫn thiếu nét' as ten_loi, coalesce(cast(safe_cast(chu_so_trong_long_nhan_thieu_net as float64) as int64), 0) as so_luong),
    struct('Err_16' as ma_loi, 'Lỗi logo, Lỗi mặt' as ten_loi, coalesce(cast(safe_cast(loi_logo_loi_mat as float64) as int64), 0) as so_luong),
    struct('Err_17' as ma_loi, 'Xước, thủng, cạnh' as ten_loi, coalesce(cast(safe_cast(xuoc_thung_canh as float64) as int64), 0) as so_luong)
  ]) x
  where
    report_date is not null
    and extract(year from report_date) = extract(year from current_date())
  group by
    report_date,
    nguon,
    id,
    ma_lo,
    ma_hang,
    ma_vach,
    ten_hang,
    nhom_hang,
    id_dai_chi,
    ma_nha_cung_cap,
    ten_nha_cung_cap,
    x.ma_loi,
    x.ten_loi
)

select * from api_unpivot
union all
select * from tsl_unpivot
union all
select * from ttl_unpivot
