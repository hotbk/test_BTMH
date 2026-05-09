{% set selected_date = btmh_dmt.btmh_run_date() %}

WITH selected_days AS (
    SELECT
        DATE_SUB({{ selected_date }}, INTERVAL 1 DAY) AS ngay
    UNION ALL
    SELECT
        {{ selected_date }} AS ngay
),

month_bounds AS (
    SELECT
        ngay,
        DATE_TRUNC(ngay, MONTH) AS month_start,
        EXTRACT(YEAR FROM ngay) AS nam,
        EXTRACT(MONTH FROM ngay) AS mm
    FROM selected_days
),


htk_base AS (
    SELECT
        nguon,
        ma_kho,
        CAST(ID_Hang AS STRING) AS id_hang,
        CAST(Nam AS INT64) AS nam,
        CAST(Mm AS INT64) AS mm,
        SUM(COALESCE(CAST(So_Luong AS NUMERIC), 0)) AS so_luong_ton,
        SUM(COALESCE(CAST(T_Tien1 AS NUMERIC), 0)) AS gia_tri_ton
    FROM {{ ref('f_hang_ton_kho') }}
    WHERE CAST(Nam AS INT64) IN (
            EXTRACT(YEAR FROM DATE_SUB({{ selected_date }}, INTERVAL 1 DAY)),
            EXTRACT(YEAR FROM {{ selected_date }})
        )
      AND CAST(Mm AS INT64) IN (
            EXTRACT(MONTH FROM DATE_SUB({{ selected_date }}, INTERVAL 1 DAY)),
            EXTRACT(MONTH FROM {{ selected_date }})
        )
    GROUP BY 1, 2, 3, 4, 5
),

nx_movements AS (
    SELECT
        DATE(nx.Ngay) AS ngay,
        nx.Nguon AS nguon,
        nx.Ma_Kho AS ma_kho,
        CAST(nx.ID_Hang AS STRING) AS id_hang,
        SUM(
            CASE
                WHEN ht.Ma_Ct IN ('NK', 'NM', 'NL', 'NS', 'PN')
                    THEN COALESCE(CAST(nx.So_Luong_Theo_Dvt AS NUMERIC), 0)
                ELSE -COALESCE(CAST(nx.So_Luong_Theo_Dvt AS NUMERIC), 0)
            END
        ) AS so_luong_ton,
        SUM(
            CASE
                WHEN ht.Ma_Ct IN ('NK', 'NM', 'NL', 'NS', 'PN')
                    THEN COALESCE(CAST(nx.T_Tien1 AS NUMERIC), 0)
                ELSE -COALESCE(CAST(nx.T_Tien1 AS NUMERIC), 0)
            END
        ) AS gia_tri_ton
    FROM {{ ref('f_nhap_xuat') }} nx
    LEFT JOIN {{ ref('d_hach_toan') }} ht
        ON nx.Nguon = ht.Nguon
       AND SAFE_CAST(nx.ID_Nx AS INT64) = SAFE_CAST(ht.ID AS INT64)
    WHERE DATE(nx.Ngay) BETWEEN DATE_TRUNC(DATE_SUB({{ selected_date }}, INTERVAL 1 DAY), MONTH) AND {{ selected_date }}
      AND nx.ID_Dv >= 0
      AND nx.ID_Hang IS NOT NULL
    GROUP BY 1, 2, 3, 4
),

dc_movements AS (
    SELECT
        ngay,
        nguon,
        ma_kho,
        id_hang,
        SUM(so_luong_ton) AS so_luong_ton,
        SUM(gia_tri_ton) AS gia_tri_ton
    FROM (
        SELECT
            DATE(Ngay) AS ngay,
            Nguon AS nguon,
            Ma_KhoX AS ma_kho,
            CAST(ID_Hang AS STRING) AS id_hang,
            -COALESCE(CAST(So_luong AS NUMERIC), 0) AS so_luong_ton,
            -COALESCE(CAST(T_Tien1 AS NUMERIC), 0) AS gia_tri_ton
        FROM {{ ref('f_dieu_chuyen') }}
        WHERE DATE(Ngay) BETWEEN DATE_TRUNC(DATE_SUB({{ selected_date }}, INTERVAL 1 DAY), MONTH) AND {{ selected_date }}
          AND ID_Hang IS NOT NULL

        UNION ALL

        SELECT
            DATE(Ngay) AS ngay,
            Nguon AS nguon,
            Ma_KhoN AS ma_kho,
            CAST(ID_Hang AS STRING) AS id_hang,
            COALESCE(CAST(So_luong AS NUMERIC), 0) AS so_luong_ton,
            COALESCE(CAST(T_Tien1 AS NUMERIC), 0) AS gia_tri_ton
        FROM {{ ref('f_dieu_chuyen') }}
        WHERE DATE(Ngay) BETWEEN DATE_TRUNC(DATE_SUB({{ selected_date }}, INTERVAL 1 DAY), MONTH) AND {{ selected_date }}
          AND ID_Hang IS NOT NULL
    ) dc
    GROUP BY 1, 2, 3, 4
),

sales_movements AS (
    SELECT
        DATE(Ngay_PhieuThu) AS ngay,
        Ma_Cong_Ty AS nguon,
        Ma_Kho AS ma_kho,
        CAST(ID_Hang AS STRING) AS id_hang,
        -SUM(COALESCE(CAST(So_Luong AS NUMERIC), 0)) AS so_luong_ton,
        -SUM(COALESCE(CAST(Gia_Von AS NUMERIC), 0)) AS gia_tri_ton
    FROM {{ ref('f_doanh_thu') }}
    WHERE ID_Dv >= 0
      AND DATE(Ngay_PhieuThu) BETWEEN DATE_TRUNC(DATE_SUB({{ selected_date }}, INTERVAL 1 DAY), MONTH) AND {{ selected_date }}
    GROUP BY 1, 2, 3, 4
),

all_movements AS (
    SELECT * FROM nx_movements
    UNION ALL
    SELECT * FROM dc_movements
    UNION ALL
    SELECT * FROM sales_movements
),

ton_kho_ngay AS (
    SELECT
        mb.ngay,
        x.nguon,
        x.ma_kho,
        x.id_hang,
        SUM(x.so_luong_ton) AS ton_cuoi_ngay_sl,
        SUM(x.gia_tri_ton) AS ton_cuoi_ngay_gt
    FROM month_bounds mb
    JOIN (
        SELECT
            mb_inner.ngay,
            h.nguon,
            h.ma_kho,
            h.id_hang,
            h.so_luong_ton,
            h.gia_tri_ton
        FROM month_bounds mb_inner
        JOIN htk_base h
            ON h.nam = mb_inner.nam
           AND h.mm = mb_inner.mm

        UNION ALL

        SELECT
            mb_inner.ngay,
            mv.nguon,
            mv.ma_kho,
            mv.id_hang,
            mv.so_luong_ton,
            mv.gia_tri_ton
        FROM month_bounds mb_inner
        JOIN all_movements mv
            ON mv.ngay >= mb_inner.month_start
           AND mv.ngay <= mb_inner.ngay
    ) x
        ON x.ngay = mb.ngay
    GROUP BY 1, 2, 3, 4
),

all_keys AS (
    SELECT DISTINCT nguon, ma_kho, id_hang
    FROM ton_kho_ngay
),

all_days AS (
    SELECT k.nguon, k.ma_kho, k.id_hang, {{ selected_date }} AS ngay
    FROM all_keys k
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
                COALESCE(curr.ton_cuoi_ngay_gt, 0) > 0
                OR COALESCE(prev.ton_cuoi_ngay_gt, 0) > 0
),

so_ban AS (
    SELECT
        Ma_Cong_Ty as Nguon,
        Ma_Kho,
        ID_Hang,
                DATE(Ngay_PhieuThu) as ngay,
                SUM(CASE WHEN DATE(Ngay_PhieuThu) = {{ selected_date }} THEN So_Luong ELSE 0 END) AS so_luong_ban_today,
                SUM(CASE WHEN DATE(Ngay_PhieuThu) = {{ selected_date }} THEN Tien_PhieuThu1 ELSE 0 END) AS doanh_thu_today,
                SUM(CASE WHEN DATE(Ngay_PhieuThu) = {{ selected_date }} THEN Gia_Von ELSE 0 END) AS gia_von_today,
        
                SUM(CASE WHEN DATE(Ngay_PhieuThu) = DATE_SUB({{ selected_date }}, INTERVAL 1 DAY) THEN So_Luong ELSE 0 END) AS so_luong_ban_yesterday,
        
                SUM(CASE WHEN DATE(Ngay_PhieuThu) >= DATE_SUB({{ selected_date }}, INTERVAL 3 DAY) 
                                    AND DATE(Ngay_PhieuThu) < {{ selected_date }} THEN So_Luong ELSE 0 END) AS so_luong_ban_3d,
        
                SUM(CASE WHEN DATE(Ngay_PhieuThu) >= DATE_SUB({{ selected_date }}, INTERVAL 7 DAY) 
                                    AND DATE(Ngay_PhieuThu) < {{ selected_date }} THEN So_Luong ELSE 0 END) AS so_luong_ban_7d
    FROM {{ ref('f_doanh_thu') }}
    WHERE ID_Dv >= 0
            AND DATE(Ngay_PhieuThu) BETWEEN DATE_SUB({{ selected_date }}, INTERVAL 7 DAY) AND {{ selected_date }}
    GROUP BY 1, 2, 3, 4
),

ie as(
    select
        DATE(nx.Ngay) as ngay,
        nx.Nguon,
        nx.Ma_Kho,
        nx.ID_Hang,
        sum(case when ht.Ma_Ct IN ('NK','NM','NL','NS','PN') then nx.So_Luong_Theo_Dvt end) as So_luong_nhap,
        sum(case when ht.Ma_Ct not IN ('NK','NM','NL','NS','PN') then nx.So_Luong_Theo_Dvt end) as So_luong_xuat,
        sum(case when ht.Ma_Ct IN ('NK','NM','NL','NS','PN') then nx.T_Tien1 end) as Gia_tri_nhap,
        sum(case when ht.Ma_Ct not IN ('NK','NM','NL','NS','PN') then nx.T_Tien1 end) as Gia_tri_xuat
    from {{ ref('f_nhap_xuat') }} nx
    left join {{ ref('d_hach_toan') }} ht
        on nx.ID_Nx = ht.ID
       and nx.Nguon = ht.Nguon
    where nx.ID_Dv >= 0
      and DATE(nx.Ngay) = {{ selected_date }}
    group by 1,2,3,4
),


tf as(
    with base as(
        select
            DATE(Ngay) as ngay,
            Nguon,
            Ma_KhoX as Ma_Kho,
            ID_Hang,
            sum(-COALESCE(CAST(So_luong AS NUMERIC), 0)) as So_luong_chuyen,
            sum(-COALESCE(CAST(T_Tien1 AS NUMERIC), 0)) as Gia_tri_chuyen
        from {{ ref('f_dieu_chuyen') }}
        where DATE(Ngay) = {{ selected_date }}
        group by 1,2,3,4

        union all

        select
            DATE(Ngay) as ngay,
            Nguon,
            Ma_KhoN as Ma_Kho,
            ID_Hang,
            sum(COALESCE(CAST(So_luong AS NUMERIC), 0)) as So_luong_chuyen,
            sum(COALESCE(CAST(T_Tien1 AS NUMERIC), 0)) as Gia_tri_chuyen
        from {{ ref('f_dieu_chuyen') }}
        where DATE(Ngay) = {{ selected_date }}
        group by 1,2,3,4
    )

    select
        ngay,
        Nguon,
        Ma_Kho,
        ID_Hang,
        sum(So_luong_chuyen) as So_luong_chuyen,
        sum(Gia_tri_chuyen) as Gia_tri_chuyen
    from base
    group by 1,2,3,4
),

dp as(
    select Nguon
        ,Ma_Kho
        ,ID_Hang
        ,sum(Required_Qty) as coc_chua_tra
    from {{ ref('v_gold_deposit') }}
    where 1=1
    group by 1,2,3
),


final AS (
    SELECT
        tk.ngay,
        tk.nguon,
        tk.ma_kho,
        case when left(Tk_Hh, 3) = '152' then 'Nguyên liệu'
             when left(Tk_Hh, 3) in ('156', '156') then 'Thành phẩm'
             else 'Khác' end as Loai_hang,
        h.Nganh_hang,
        h.nhom_sp_nho,
        h.Ten_Hang,
        h.Ma_Hang,
        case when Nganh_hang = 'Tích trữ' then h.T_Luong else null end as ban_vi,
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
        coalesce(sb.so_luong_ban_today, 0) AS so_luong_ban_today,
        coalesce(sb.doanh_thu_today, 0) AS doanh_thu_today,
        coalesce(sb.so_luong_ban_yesterday, 0) AS so_luong_ban_yesterday,
        coalesce(sb.so_luong_ban_3d, 0) AS so_luong_ban_3d,
        coalesce(sb.so_luong_ban_7d, 0) AS so_luong_ban_7d,
        coalesce(dp.coc_chua_tra, 0) AS coc_chua_tra
    FROM ton_kho tk
    LEFT JOIN ie
        ON  tk.nguon   = ie.Nguon
        AND tk.ma_kho  = ie.Ma_Kho
        AND tk.id_hang = CAST(ie.ID_Hang AS STRING)
        AND tk.ngay    = ie.ngay
    LEFT JOIN tf
        ON  tk.nguon   = tf.Nguon
        AND tk.ma_kho  = tf.Ma_Kho
        AND tk.id_hang = CAST(tf.ID_Hang AS STRING)
        AND tk.ngay    = tf.ngay
    LEFT JOIN so_ban sb
        ON  tk.nguon   = sb.Nguon
        AND tk.ma_kho  = sb.Ma_Kho
        AND tk.id_hang = CAST(sb.ID_Hang AS STRING)
        AND tk.ngay    = sb.ngay
    LEFT JOIN dp
        ON  tk.nguon   = dp.Nguon
        AND tk.ma_kho  = dp.Ma_Kho
        AND tk.id_hang = CAST(dp.ID_Hang AS STRING)
    LEFT JOIN {{ ref('d_hang_1_agg') }} h
        ON  tk.nguon   = h.Ma_vung
        AND tk.id_hang = CAST(h.ID AS STRING)
)

SELECT DISTINCT *
FROM final
where 1=1
and left(Tk_Hh, 3) <> '153'
and left(Tk_Hh, 1) <> '2'