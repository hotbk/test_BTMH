{% set start_date = "date('2024-01-01')" %}
{% set end_date = btmh_dmt.btmh_run_date() %}

WITH base AS (
    SELECT
        ngay,
        nguon,
        ma_kho,
        id_hang,
        so_luong_ton,
        gia_tri_ton
    FROM {{ source('dmt', 'f_ton_kho') }}
    WHERE ngay >= DATE_SUB({{ start_date }}, INTERVAL 1 DAY)
),

-- Tồn theo từng ngày (giữ nguyên, không group)
ton_kho_ngay AS (
    SELECT
        ngay,
        nguon,
        ma_kho,
        id_hang,
        so_luong_ton AS ton_cuoi_ngay_sl,
        gia_tri_ton  AS ton_cuoi_ngay_gt
    FROM base
),

-- Tất cả keys xuất hiện từ start_date
all_keys AS (
    SELECT DISTINCT nguon, ma_kho, id_hang
    FROM ton_kho_ngay
    WHERE ngay >= {{ start_date }}
),

-- Generate đủ ngày từ start_date đến end_date
all_days AS (
    SELECT k.nguon, k.ma_kho, k.id_hang, d.ngay
    FROM all_keys k
    CROSS JOIN (
        SELECT ngay
        FROM UNNEST(
            GENERATE_DATE_ARRAY({{ start_date }}, {{ end_date }}, INTERVAL 1 DAY)
        ) AS ngay
    ) d
),

ton_kho AS (
    SELECT
        ad.ngay,
        ad.nguon,
        ad.ma_kho,
        ad.id_hang,
        prev.ton_cuoi_ngay_sl AS ton_dau_ngay_sl,
        prev.ton_cuoi_ngay_gt AS ton_dau_ngay_gt,
        curr.ton_cuoi_ngay_sl,
        curr.ton_cuoi_ngay_gt
    FROM all_days ad
    LEFT JOIN ton_kho_ngay curr
        ON  curr.ngay    = ad.ngay
        AND curr.nguon   = ad.nguon
        AND curr.ma_kho  = ad.ma_kho
        AND curr.id_hang = ad.id_hang
    LEFT JOIN ton_kho_ngay prev
        ON  prev.ngay    = DATE_SUB(ad.ngay, INTERVAL 1 DAY)
        AND prev.nguon   = ad.nguon
        AND prev.ma_kho  = ad.ma_kho
        AND prev.id_hang = ad.id_hang
    WHERE
        curr.ton_cuoi_ngay_gt > 0
        OR prev.ton_cuoi_ngay_gt > 0
),

final AS (
    SELECT
        tk.ngay,
        tk.nguon,
        tk.ma_kho,
        h.Nganh_hang,
        h.nhom_sp_nho,
        h.Ten_Hang,
        h.Ma_Hang,
        h.T_Luong,
        h.Tk_Hh,
        tk.ton_dau_ngay_sl,
        tk.ton_cuoi_ngay_sl,
        tk.ton_dau_ngay_gt,
        tk.ton_cuoi_ngay_gt,
        coalesce(ie.So_luong_nhap, 0) AS So_luong_nhap,
        coalesce(ie.Gia_tri_nhap, 0) AS Gia_tri_nhap,
        coalesce(ie.So_luong_xuat, 0) AS So_luong_xuat,
        coalesce(ie.Gia_tri_xuat, 0) AS Gia_tri_xuat,
        coalesce(tf.So_luong_chuyen, 0) AS So_luong_chuyen,
        coalesce(tf.Gia_tri_chuyen, 0) AS Gia_tri_chuyen,
    FROM ton_kho tk
    LEFT JOIN {{ ref('v_import_export') }} ie
        ON  tk.nguon   = ie.Nguon
        AND tk.ma_kho  = ie.Ma_Kho
        AND tk.id_hang = CAST(ie.ID_Hang AS STRING)
        AND tk.ngay    = ie.ngay
    LEFT JOIN {{ ref('v_transfer') }} tf
        ON  tk.nguon   = tf.Nguon
        AND tk.ma_kho  = tf.Ma_Kho
        AND tk.id_hang = CAST(tf.ID_Hang AS STRING)
        AND tk.ngay    = tf.ngay
    LEFT JOIN {{ ref('d_hang_1_agg') }} h
        ON  tk.nguon   = h.Ma_vung
        AND tk.id_hang = CAST(h.ID AS STRING)
)

SELECT DISTINCT *
FROM final
