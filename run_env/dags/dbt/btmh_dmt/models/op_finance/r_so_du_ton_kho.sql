{{
  config(
    materialized='table',
    cluster_by=['Loai_Hang_2', 'nguon']
  )
}}

WITH selected_days AS (
    SELECT DATE_SUB(DATE(CURRENT_DATE()), INTERVAL 1 DAY) AS ngay
    UNION ALL
    SELECT DATE(CURRENT_DATE()) AS ngay
),

month_bounds AS (
    SELECT ngay,
           DATE_TRUNC(ngay, MONTH) AS month_start,
           EXTRACT(YEAR FROM ngay) AS nam,
           EXTRACT(MONTH FROM ngay) AS mm
    FROM selected_days
),

htk_base AS (
    SELECT nguon, ma_kho, CAST(ID_Hang AS STRING) AS id_hang,
           CAST(Nam AS INT64) AS nam, CAST(Mm AS INT64) AS mm,
           SUM(COALESCE(CAST(So_Luong AS NUMERIC), 0)) AS so_luong_ton,
           SUM(COALESCE(CAST(T_Tien1 AS NUMERIC), 0)) AS gia_tri_ton
    FROM {{ ref('f_hang_ton_kho') }}
    WHERE CAST(Nam AS INT64) IN (EXTRACT(YEAR FROM DATE_SUB(DATE(CURRENT_DATE()), INTERVAL 1 DAY)), EXTRACT(YEAR FROM DATE(CURRENT_DATE())))
      AND CAST(Mm AS INT64) IN (EXTRACT(MONTH FROM DATE_SUB(DATE(CURRENT_DATE()), INTERVAL 1 DAY)), EXTRACT(MONTH FROM DATE(CURRENT_DATE())))
    GROUP BY 1, 2, 3, 4, 5
),

nx_movements AS (
    SELECT DATE(nx.Ngay) AS ngay, nx.Nguon AS nguon, nx.Ma_Kho AS ma_kho, CAST(nx.ID_Hang AS STRING) AS id_hang,
           SUM(CASE WHEN ht.Ma_Ct IN ('NK', 'NM', 'NL', 'NS', 'PN') THEN COALESCE(CAST(nx.So_Luong_Theo_Dvt AS NUMERIC), 0) ELSE -COALESCE(CAST(nx.So_Luong_Theo_Dvt AS NUMERIC), 0) END) AS so_luong_ton,
           SUM(CASE WHEN ht.Ma_Ct IN ('NK', 'NM', 'NL', 'NS', 'PN') THEN COALESCE(CAST(nx.T_Tien1 AS NUMERIC), 0) ELSE -COALESCE(CAST(nx.T_Tien1 AS NUMERIC), 0) END) AS gia_tri_ton
    FROM {{ ref('f_nhap_xuat') }} nx
    LEFT JOIN {{ ref('d_hach_toan') }} ht ON nx.Nguon = ht.Nguon AND SAFE_CAST(nx.ID_Nx AS INT64) = SAFE_CAST(ht.ID AS INT64)
    WHERE DATE(nx.Ngay) BETWEEN DATE_TRUNC(DATE_SUB(DATE(CURRENT_DATE()), INTERVAL 1 DAY), MONTH) AND DATE(CURRENT_DATE())
      AND nx.ID_Dv >= 0 AND nx.ID_Hang IS NOT NULL
    GROUP BY 1, 2, 3, 4
),

dc_movements AS (
    SELECT ngay, nguon, ma_kho, id_hang, SUM(so_luong_ton) AS so_luong_ton, SUM(gia_tri_ton) AS gia_tri_ton
    FROM (
        SELECT DATE(Ngay) AS ngay, Nguon AS nguon, Ma_KhoX AS ma_kho, CAST(ID_Hang AS STRING) AS id_hang, -COALESCE(CAST(So_luong AS NUMERIC), 0) AS so_luong_ton, -COALESCE(CAST(T_Tien1 AS NUMERIC), 0) AS gia_tri_ton
        FROM {{ ref('f_dieu_chuyen') }}
        WHERE DATE(Ngay) BETWEEN DATE_TRUNC(DATE_SUB(DATE(CURRENT_DATE()), INTERVAL 1 DAY), MONTH) AND DATE(CURRENT_DATE()) AND ID_Hang IS NOT NULL
        UNION ALL
        SELECT DATE(Ngay) AS ngay, Nguon AS nguon, Ma_KhoN AS ma_kho, CAST(ID_Hang AS STRING) AS id_hang, COALESCE(CAST(So_luong AS NUMERIC), 0) AS so_luong_ton, COALESCE(CAST(T_Tien1 AS NUMERIC), 0) AS gia_tri_ton
        FROM {{ ref('f_dieu_chuyen') }}
        WHERE DATE(Ngay) BETWEEN DATE_TRUNC(DATE_SUB(DATE(CURRENT_DATE()), INTERVAL 1 DAY), MONTH) AND DATE(CURRENT_DATE()) AND ID_Hang IS NOT NULL
    ) dc
    GROUP BY 1, 2, 3, 4
),

sales_movements AS (
    SELECT DATE(Ngay_PhieuThu) AS ngay, Ma_Cong_Ty AS nguon, Ma_Kho AS ma_kho, CAST(ID_Hang AS STRING) AS id_hang,
           -SUM(COALESCE(CAST(So_Luong AS NUMERIC), 0)) AS so_luong_ton, -SUM(COALESCE(CAST(Gia_Von AS NUMERIC), 0)) AS gia_tri_ton
    FROM {{ ref('f_doanh_thu') }}
    WHERE ID_Dv >= 0 AND DATE(Ngay_PhieuThu) BETWEEN DATE_TRUNC(DATE_SUB(DATE(CURRENT_DATE()), INTERVAL 1 DAY), MONTH) AND DATE(CURRENT_DATE())
    GROUP BY 1, 2, 3, 4
),

all_movements AS (
    SELECT * FROM nx_movements UNION ALL SELECT * FROM dc_movements UNION ALL SELECT * FROM sales_movements
),

ton_kho_ngay AS (
    SELECT mb.ngay, x.nguon, x.ma_kho, x.id_hang, SUM(x.so_luong_ton) AS ton_cuoi_ngay_sl, SUM(x.gia_tri_ton) AS ton_cuoi_ngay_gt
    FROM month_bounds mb
    JOIN (
        SELECT mb_inner.ngay, h.nguon, h.ma_kho, h.id_hang, h.so_luong_ton, h.gia_tri_ton
        FROM month_bounds mb_inner JOIN htk_base h ON h.nam = mb_inner.nam AND h.mm = mb_inner.mm
        UNION ALL
        SELECT mb_inner.ngay, mv.nguon, mv.ma_kho, mv.id_hang, mv.so_luong_ton, mv.gia_tri_ton
        FROM month_bounds mb_inner JOIN all_movements mv ON mv.ngay >= mb_inner.month_start AND mv.ngay <= mb_inner.ngay
    ) x ON x.ngay = mb.ngay
    GROUP BY 1, 2, 3, 4
),

tf AS (
    SELECT ngay, Nguon, Ma_Kho, ID_Hang, SUM(So_luong_chuyen) AS So_luong_chuyen, SUM(Gia_tri_chuyen) AS Gia_tri_chuyen
    FROM (
        SELECT DATE(Ngay) AS ngay, Nguon, Ma_KhoX AS Ma_Kho, CAST(ID_Hang AS STRING) AS ID_Hang, -COALESCE(CAST(So_luong AS NUMERIC), 0) AS So_luong_chuyen, -COALESCE(CAST(T_Tien1 AS NUMERIC), 0) AS Gia_tri_chuyen
        FROM {{ ref('f_dieu_chuyen') }} WHERE DATE(Ngay) = DATE(CURRENT_DATE())
        UNION ALL
        SELECT DATE(Ngay) AS ngay, Nguon, Ma_KhoN AS Ma_Kho, CAST(ID_Hang AS STRING) AS ID_Hang, COALESCE(CAST(So_luong AS NUMERIC), 0) AS So_luong_chuyen, COALESCE(CAST(T_Tien1 AS NUMERIC), 0) AS Gia_tri_chuyen
        FROM {{ ref('f_dieu_chuyen') }} WHERE DATE(Ngay) = DATE(CURRENT_DATE())
    )
    GROUP BY 1, 2, 3, 4
),

all_keys AS (
    SELECT DISTINCT nguon, ma_kho, id_hang FROM ton_kho_ngay
    UNION DISTINCT
    SELECT DISTINCT Nguon, Ma_Kho, CAST(ID_Hang AS STRING) FROM tf
),

ton_kho AS (
    SELECT ad.ngay, ad.nguon, ad.ma_kho, ad.id_hang,
           COALESCE(prev.ton_cuoi_ngay_sl, 0) AS ton_dau_ngay_sl, COALESCE(prev.ton_cuoi_ngay_gt, 0) AS ton_dau_ngay_gt,
           COALESCE(curr.ton_cuoi_ngay_sl, 0) AS ton_cuoi_ngay_sl, COALESCE(curr.ton_cuoi_ngay_gt, 0) AS ton_cuoi_ngay_gt
    FROM (SELECT *, DATE(CURRENT_DATE()) AS ngay FROM all_keys) ad
    LEFT JOIN ton_kho_ngay curr ON curr.ngay = ad.ngay AND curr.nguon = ad.nguon AND curr.ma_kho = ad.ma_kho AND curr.id_hang = ad.id_hang
    LEFT JOIN ton_kho_ngay prev ON prev.ngay = DATE_SUB(ad.ngay, INTERVAL 1 DAY) AND prev.nguon = ad.nguon AND prev.ma_kho = ad.ma_kho AND prev.id_hang = ad.id_hang
),

so_ban AS (
    SELECT Ma_Cong_Ty AS Nguon, Ma_Kho, ID_Hang,
           SUM(CASE WHEN DATE(Ngay_PhieuThu) = CURRENT_DATE() THEN So_Luong ELSE 0 END) AS so_luong_ban_today,
           SUM(CASE WHEN DATE(Ngay_PhieuThu) = CURRENT_DATE() THEN Tien_PhieuThu1 ELSE 0 END) AS doanh_thu_today,
           SUM(CASE WHEN DATE(Ngay_PhieuThu) = CURRENT_DATE() THEN Gia_Von ELSE 0 END) AS gia_von_today,
           SUM(CASE WHEN DATE(Ngay_PhieuThu) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) THEN So_Luong ELSE 0 END) AS so_luong_ban_yesterday,
           SUM(CASE WHEN DATE(Ngay_PhieuThu) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY) AND DATE(Ngay_PhieuThu) < CURRENT_DATE() THEN So_Luong ELSE 0 END) AS so_luong_ban_3d,
           SUM(CASE WHEN DATE(Ngay_PhieuThu) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) AND DATE(Ngay_PhieuThu) < CURRENT_DATE() THEN So_Luong ELSE 0 END) AS so_luong_ban_7d
    FROM {{ ref('f_doanh_thu') }}
    WHERE ID_Dv >= 0 AND DATE(Ngay_PhieuThu) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) AND CURRENT_DATE()
    GROUP BY 1, 2, 3

    union all

    SELECT 'NY' AS Nguon, Ma_Kho, ID_Hang,
           null AS so_luong_ban_today,
           null AS doanh_thu_today,
           null AS gia_von_today,
           SUM(CASE WHEN DATE(Ngay_Chung_Tu) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) THEN So_Luong ELSE 0 END) AS so_luong_ban_yesterday,
           SUM(CASE WHEN DATE(Ngay_Chung_Tu) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY) AND DATE(Ngay_Chung_Tu) < CURRENT_DATE() THEN So_Luong ELSE 0 END) AS so_luong_ban_3d,
           SUM(CASE WHEN DATE(Ngay_Chung_Tu) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) AND DATE(Ngay_Chung_Tu) < CURRENT_DATE() THEN So_Luong ELSE 0 END) AS so_luong_ban_7d
    FROM {{ ref('f_b2b') }}
    where 1=1
    and Ma_Kho = 'B2BBL'
    AND DATE(Ngay_Chung_Tu) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) AND CURRENT_DATE()
    GROUP BY 1, 2, 3
),

ie AS (
    SELECT DATE(nx.Ngay) AS ngay, nx.Nguon, nx.Ma_Kho, nx.ID_Hang,
           SUM(CASE WHEN ht.Ma_Ct IN ('NK','NM','NL','NS','PN') THEN nx.So_Luong_Theo_Dvt END) AS So_luong_nhap,
           SUM(CASE WHEN ht.Ma_Ct NOT IN ('NK','NM','NL','NS','PN') THEN nx.So_Luong_Theo_Dvt END) AS So_luong_xuat,
           SUM(CASE WHEN ht.Ma_Ct IN ('NK','NM','NL','NS','PN') THEN nx.T_Tien1 END) AS Gia_tri_nhap,
           SUM(CASE WHEN ht.Ma_Ct NOT IN ('NK','NM','NL','NS','PN') THEN nx.T_Tien1 END) AS Gia_tri_xuat
    FROM {{ ref('f_nhap_xuat') }} nx
    LEFT JOIN {{ ref('d_hach_toan') }} ht ON nx.ID_Nx = ht.ID AND nx.Nguon = ht.Nguon
    WHERE nx.ID_Dv >= 0 AND DATE(nx.Ngay) = DATE(CURRENT_DATE())
    GROUP BY 1, 2, 3, 4
),

dp AS (
    SELECT Nguon, Ma_Kho, ID_Hang, SUM(Required_Qty) AS coc_chua_tra
    FROM {{ ref('v_gold_deposit') }}
    GROUP BY 1, 2, 3
),

final AS (
    SELECT tk.ngay,
           CASE WHEN tk.nguon = 'NY' THEN 'BTMH Bán lẻ' 
                WHEN tk.nguon = 'SX' THEN 'Sản xuất' 
                WHEN tk.nguon = 'BN' THEN 'Bắc Ninh' 
                WHEN tk.nguon = 'SG' THEN 'Hồ Chí Minh' 
                ELSE tk.nguon END AS nguon,
           tk.ma_kho,
           CASE WHEN LEFT(h.Tk_Hh, 3) = '152' THEN 'Nguyên liệu' 
                WHEN LEFT(h.Tk_Hh, 3) IN ('155', '156') THEN 'Thành phẩm' 
                ELSE 'Khác' END AS Loai_hang,
           h.Nganhhang_fix AS Nganh_hang,
           h.Dongsp_fix as nhom_sp_nho,
           h.nhom_sp_nho as nhom_sp_old, 
           h.Ten_Hang, 
           h.Ma_Hang, 
           h.Ma_nhom,
           CASE WHEN Nganhhang_fix = 'Vàng tích lũy' THEN h.T_Luong ELSE NULL END AS ban_vi,
           h.T_luong, 
           h.Tk_Hh,
           tk.ton_dau_ngay_sl, 
           tk.ton_cuoi_ngay_sl, 
           tk.ton_dau_ngay_gt, 
           tk.ton_cuoi_ngay_gt,
           COALESCE(ie.So_luong_nhap, 0) AS So_luong_nhap, 
           COALESCE(ie.Gia_tri_nhap, 0) AS Gia_tri_nhap,
           COALESCE(ie.So_luong_xuat, 0) AS So_luong_xuat, 
           COALESCE(ie.Gia_tri_xuat, 0) AS Gia_tri_xuat,
           COALESCE(tf.So_luong_chuyen, 0) AS So_luong_chuyen, 
           COALESCE(tf.Gia_tri_chuyen, 0) AS Gia_tri_chuyen,
           COALESCE(sb.so_luong_ban_today, 0) AS so_luong_ban_today,
           COALESCE(ie.So_luong_xuat, 0)  +  COALESCE(sb.so_luong_ban_today, 0) as so_luong_xuat_ban,
           COALESCE(ie.Gia_tri_xuat, 0)  +  COALESCE(sb.gia_von_today, 0) as gia_tri_xuat_ban, 
           COALESCE(sb.gia_von_today, 0) AS gia_von_today,
           COALESCE(sb.doanh_thu_today, 0) AS doanh_thu_today,
           COALESCE(sb.so_luong_ban_yesterday, 0) AS so_luong_ban_yesterday,
           COALESCE(sb.so_luong_ban_3d, 0) AS so_luong_ban_3d,
           COALESCE(sb.so_luong_ban_7d, 0) AS so_luong_ban_7d,
           COALESCE(dp.coc_chua_tra, 0) AS coc_chua_tra
    FROM ton_kho tk
    LEFT JOIN ie ON tk.nguon = ie.Nguon AND tk.ma_kho = ie.Ma_Kho AND tk.id_hang = CAST(ie.ID_Hang AS STRING) AND tk.ngay = ie.ngay
    LEFT JOIN tf ON tk.nguon = tf.Nguon AND tk.ma_kho = tf.Ma_Kho AND tk.id_hang = CAST(tf.ID_Hang AS STRING) AND tk.ngay = tf.ngay
    LEFT JOIN so_ban sb ON tk.nguon = sb.Nguon AND tk.ma_kho = sb.Ma_Kho AND tk.id_hang = CAST(sb.ID_Hang AS STRING)
    LEFT JOIN dp ON tk.nguon = dp.Nguon AND tk.ma_kho = dp.Ma_Kho AND tk.id_hang = CAST(dp.ID_Hang AS STRING)
    LEFT JOIN {{ ref('d_hang_1_agg') }} h ON tk.nguon = h.Ma_vung AND tk.id_hang = CAST(h.ID AS STRING)
)

,final2 as(
    SELECT DISTINCT *,
        CASE WHEN ( Nganh_hang = 'Khác' AND nhom_sp_old = 'Nguyên liệu tích trữ') THEN 'NL Tích trữ'
                WHEN (Left(Tk_hh,3) in ('152', '002') and nhom_sp_old = 'KGB') THEN 'NL Tích trữ'
                WHEN (ma_nhom = 'NLTT') THEN 'NL Tích trữ'
                WHEN (Nganh_hang = 'Khác' AND nhom_sp_old = 'NLVT') THEN 'NL Vàng tây'
                WHEN (Left(Tk_hh,3)='152' and Nganh_hang = 'Vàng Tây') THEN 'NL Vàng tây'
                WHEN (nhom_sp_nho = 'Vàng Tây_Kim cương viên') then 'NL Vàng tây'
                WHEN (Nganh_hang = 'Khác' AND (nhom_sp_old = 'NLBAC' OR Ma_nhom = 'NLBA')) THEN 'NL Bạc'
                WHEN (left(tk_hh,3)='152' and Nganh_hang = 'Khác') THEN 'NL Khác'
                ELSE Loai_hang END AS Loai_hang_2,
            case when Nganh_hang = 'Vàng tích lũy' then ton_dau_ngay_sl * ban_vi else ton_dau_ngay_sl end AS ton_dau_ngay_tl,
            case when Nganh_hang = 'Vàng tích lũy' then ton_cuoi_ngay_sl * ban_vi else ton_cuoi_ngay_sl end AS ton_cuoi_ngay_tl,
            So_luong_nhap * ban_vi AS So_luong_nhap_tl,
            So_luong_xuat * ban_vi AS So_luong_xuat_tl,
            so_luong_xuat_ban * ban_vi AS so_luong_xuat_ban_tl,
            So_luong_chuyen * ban_vi AS So_luong_chuyen_tl,
            so_luong_ban_today * ban_vi AS so_luong_ban_today_tl,
            so_luong_ban_yesterday * ban_vi AS so_luong_ban_yesterday_tl,
            so_luong_ban_3d * ban_vi AS so_luong_ban_3d_tl,
            so_luong_ban_7d * ban_vi AS so_luong_ban_7d_tl,
            coc_chua_tra * ban_vi AS coc_chua_tra_tl   
    FROM final
    WHERE LEFT(Tk_Hh, 3) <> '153' AND LEFT(Tk_Hh, 1) <> '2'
    and nguon <> 'HD'
    and not (ton_dau_ngay_sl = 0 and So_luong_nhap = 0 and So_luong_xuat = 0 and So_luong_chuyen = 0 and so_luong_ban_today = 0 and Gia_tri_chuyen = 0 and so_luong_ban_3d = 0 and so_luong_ban_7d = 0)
    -- and (ton_dau_ngay_sl + So_luong_nhap - So_luong_xuat + So_luong_chuyen - so_luong_ban_today) <> 0
)

select f.*, r.exchange_rate, DATETIME(CURRENT_TIMESTAMP(), 'Asia/Bangkok') as updated_at
from final2 f
left join {{ ref('r_exchange_rate') }} r on f.Ten_Hang = r.Ten_Hang
where 1=1
-- and Ma_Hang = 'NL06'
-- and nguon = 'Hồ Chí Minh'