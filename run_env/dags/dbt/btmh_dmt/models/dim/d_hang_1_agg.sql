with base as(
  select h.ID
        ,Ma_vung
        ,ID_Key
        ,Ma_Hang
        ,safe_cast(dmnmm.ID as int64) as ID_ma_mau
        ,cast(dmnmm.Ma_NM as string) as ma_mau
        ,cast(dmnmm.Mo_Ta as string) as mo_ta_ma_mau
        ,Ma_Nhom
        ,Ten_Hang
        ,T_Luong
        ,ID_Dai_Chi
		,Tk_Hh
        ,CASE
            --Tiểu kim cát
            WHEN h.Ma_hang IN ('BTMVV49KD0-501001-001',
                              'BTMVV49KD0-501002-001',
                              'BTMVV49KD0-501003-001') then 'Tích trữ'

            -- Nguyên liệu
            WHEN h.Ma_hang = 'NLBAC' THEN 'Khác'
            WHEN h.Ma_nhom = 'NLVT' THEN 'Khác'

            -- Bạc tích lũy
            WHEN h.Ma_Nhom IN ('NTQ', 'NGB')
            THEN 'Bạc tích lũy'

            -- Tích trữ
            WHEN h.Ma_Nhom IN ('NL24', 'NLTT', 'KHS')
                OR LEFT(h.Ma_Nhom,3) IN ('KGB', 'TTS', 'TTV')
            THEN 'Tích trữ'

            -- BST
            WHEN LEFT(h.Ma_nhom,7) IN ('BTCHV10','BTBTV10','BTMAV10','BTLTV10')
                OR (LEFT(h.Ma_nhom,3) IN ('BTD','BTN') AND RIGHT(h.Ma_nhom,3) = 'V10')
            THEN 'Khác'

            -- TS Vàng ta
            WHEN RIGHT(h.Ma_nhom,2) IN ('PT','2D','3D','5G')
                OR LEFT(h.Ma_nhom,2) = 'VD'
                OR LEFT(h.Ma_nhom,4) IN ('QT24','24DV','QTLG','QTTV','QTDV')
                OR h.Ma_nhom = 'BTDVV24'
            THEN 'Vàng Ta'

            -- TS Vàng tây
            WHEN h.Ma_nhom = 'PLN1'
                OR LEFT(h.Ma_nhom,2) IN ('VT','CY','PH','NH','TK')
                OR LEFT(h.Ma_nhom,4) IN ('NCCN','NCCY','NCCK')
                OR LEFT(h.Ma_nhom,3) = 'KDD'
                OR LEFT(h.Ma_nhom,2) = 'KC'
                OR (LEFT(h.Ma_nhom,2) IN ('PY','CN') AND RIGHT(h.Ma_nhom,2) = '18')
                OR (LEFT(h.Ma_nhom,2) = 'CN' AND RIGHT(h.Ma_nhom,2) IN ('10','14'))
                OR (LEFT(h.Ma_nhom,2) = 'PY' AND RIGHT(h.Ma_nhom,2) = '10')
            THEN 'Vàng Tây'

            -- Hỗn hợp
            WHEN LEFT(h.Ma_nhom,2) IN ('PT','KH','BA','BX','BC','BY')
                OR LEFT(h.Ma_nhom,4) IN ('KDVD','QTTM','QTEV')
                OR h.Ma_hang IN (
                    'BTMVV49KD0-501004-001',
                    'BTMVV49KD0-501005-001'
                )
            THEN 'Khác'

            ELSE 'Khác'end as Nganh_hang

        ,CASE
            WHEN h.Ma_Nhom in ('NL24', 'NLTT', 'KHS') then 'KGB'
            -- Mã hàng cụ thể
            WHEN h.Ma_hang IN (
                'BTMVV49KD0-501001-001','BTMVV49KD0-501002-001','BTMVV49KD0-501003-001',
                'KCC01-001','KCT01-001','KTT01-001','KTC01-001','KBC01-001',
                'KBT01-001','KMT01-001','KMC01-001','KCC1C-001','KCT1C-001','KTT1C-001','KTC1C-001','KBC1C-001',
                'KBT1C-001','KMT1C-001','KMC1C-001' 
            ) THEN 'Quà tặng'
            
            -- Bạc tích lũy
            WHEN h.Ma_Nhom IN ('NTQ', 'NGB')
            THEN 'Bạc tích lũy'

            -- VT đá màu
            WHEN LEFT(h.Ma_nhom,2) = 'VT'
              OR h.Ma_nhom = 'PLN1'
            THEN 'VT đá màu'

            -- PC Ý
            WHEN (LEFT(h.Ma_nhom,2) IN ('PY','CN')) AND RIGHT(h.Ma_nhom,2) = '18'
            THEN 'PC Ý 18K'

            WHEN LEFT(h.Ma_nhom,2) = 'CN'
              AND RIGHT(h.Ma_nhom,2) IN ('10','14')
            THEN 'PC Ý 10K'

            WHEN LEFT(h.Ma_nhom,2) = 'PY'
                            AND RIGHT(h.Ma_nhom,2) = '10'
            THEN 'PC Ý 10K'

            -- Nhẫn cưới
            WHEN LEFT(h.Ma_nhom,2) = 'CY'
              OR LEFT(h.Ma_nhom,4) IN ('NCCN','NCCY')
            THEN 'Nhẫn Cưới'

            WHEN LEFT(h.Ma_nhom,4) = 'NCCK'
            THEN 'Nhẫn Cưới Kim Cương'

            -- Trang sức khác
            WHEN LEFT(h.Ma_nhom,2) = 'PH'
            THEN 'PC Hàn Quốc'

            WHEN LEFT(h.Ma_nhom,2) = 'NH'
            THEN 'TS Nhập khẩu'

            WHEN LEFT(h.Ma_nhom,2) = 'TK'
            THEN 'TS Kim cương'

            WHEN LEFT(h.Ma_nhom,2) = 'KC'
              OR LEFT(h.Ma_nhom,3) = 'KDD'
            THEN 'Kim cương viên'

            -- Vàng ta
            WHEN RIGHT(h.Ma_nhom,2) = 'PT'
            THEN 'PT'

            WHEN RIGHT(h.Ma_nhom,2) = '2D'
            THEN 'CN'

            WHEN RIGHT(h.Ma_nhom,2) = '3D'
            THEN '3D'

            WHEN RIGHT(h.Ma_nhom,2) = '5G'
            THEN 'CNC'

            WHEN LEFT(h.Ma_nhom,2) = 'VD'
            THEN '24K Đá màu'

            -- Quà tặng
            WHEN LEFT(h.Ma_nhom,4) IN ('QT24','24DV','QTLG','QTTV','QTDV')
              OR h.Ma_nhom = 'BTDVV24'
            THEN '24K Quà tặng'

            -- Tích trữ
            WHEN LEFT(h.Ma_nhom,3) = 'KGB'
              OR h.Ma_nhom IN ('KHS')
            THEN 'KGB'

            WHEN LEFT(h.Ma_nhom,3) = 'TTS'
            THEN 'SJC'

            WHEN LEFT(h.Ma_nhom,3) = 'TTV'
                        THEN 'VRTL'

            -- Phong thủy
            WHEN LEFT(h.Ma_nhom,2) IN ('PT','KH')
              OR LEFT(h.Ma_nhom,4) = 'KDVD'
            THEN 'Phong Thủy'

            -- Quà tặng khác
            WHEN LEFT(h.Ma_nhom,4) IN ('QTTM','QTEV')
            THEN 'Quà tặng'

            -- Bạc
            WHEN LEFT(h.Ma_nhom,2) IN ('BA','BX','BC','BY')
            THEN 'Bạc'

            -- BST
                        WHEN LEFT(h.Ma_nhom,5) IN ('BTCHV','BTBTV','BTMAV','BTLTV')
                            OR (LEFT(h.Ma_nhom,3) = 'BTD' AND LEFT(RIGHT(h.Ma_nhom,3),1) = 'V')
                            OR (LEFT(h.Ma_nhom,3) = 'BTN' AND LEFT(RIGHT(h.Ma_nhom,3),1) = 'V')
            THEN 'BST'

            -- Nguyên liệu
            WHEN h.Ma_nhom = 'NLVT'
              AND h.Ma_hang <> 'NLBAC'
            THEN 'NLVT'

            WHEN h.Ma_hang = 'NLBAC'
            THEN 'NLBAC'

            ELSE 'Khác'

            END AS nhom_sp_nho
  from {{ ref('d_hang_1') }} h left join {{ ref('d_nhom_ma_mau') }} dmnmm
    on safe_cast(h.ID_NMM as int64) = safe_cast(dmnmm.ID as int64)
)

,final as (
    select *
        ,CASE 
        WHEN nhom_sp_nho = 'BST' THEN 'Vàng Tây'
            WHEN Nganh_hang = 'Khác' THEN 'Khác'

            WHEN Nganh_hang = 'Tích trữ' 
                -- AND nhom_sp_nho IN ('KGB','SJC','VRTL') 
                THEN 'Vàng tích lũy'

            -- WHEN Nganh_hang = 'Tích trữ' 
            --     AND nhom_sp_nho = 'Tiểu kim cát' 
            --     THEN 'Vàng Trang sức 24K'

            WHEN Nganh_hang = 'Vàng Ta' THEN 'Vàng Trang sức 24K'

            WHEN Nganh_hang = 'Vàng Tây' THEN 'Vàng Tây'

            ELSE Nganh_hang
        END AS Nganhhang_fix,

        CASE 
            WHEN nhom_sp_nho = 'BST'
                THEN 'Vàng Tây_BST'

            WHEN Nganh_hang = 'Khác' AND nhom_sp_nho IN ('Khác','NLVT') 
                THEN 'Khác_Khác'
            WHEN Nganh_hang = 'Khác' AND nhom_sp_nho IN ('Bạc','NLBAC') 
                THEN 'Khác_Bạc'
            WHEN Nganh_hang = 'Khác' AND nhom_sp_nho = 'Phong Thủy' 
                THEN 'Khác_Phong Thủy'
            WHEN Nganh_hang = 'Khác' AND nhom_sp_nho = 'Quà tặng' 
                THEN 'Khác_Quà tặng'

            WHEN Nganh_hang = 'Tích trữ' AND nhom_sp_nho = 'KGB' 
                THEN 'Tích Trữ_KGB'
            WHEN Nganh_hang = 'Tích trữ' AND nhom_sp_nho = 'SJC' 
                THEN 'Tích Trữ_SJC'
            WHEN Nganh_hang = 'Tích trữ' AND nhom_sp_nho = 'VRTL' 
                THEN 'Tích Trữ_VRTL'
            WHEN nhom_sp_nho = 'Tiểu kim cát' 
                THEN 'Tích Trữ_Tiểu kim cát'

            WHEN Nganh_hang = 'Vàng Ta' AND nhom_sp_nho = '3D' 
                THEN 'Vàng 24K_3D'
            WHEN Nganh_hang = 'Vàng Ta' AND nhom_sp_nho = '24K Quà tặng' 
                THEN 'Vàng 24K_24K Quà tặng'
            WHEN Nganh_hang = 'Vàng Ta' AND nhom_sp_nho = 'CN' 
                THEN 'Vàng 24K_CN'
            WHEN Nganh_hang = 'Vàng Ta' AND nhom_sp_nho = 'CNC' 
                THEN 'Vàng 24K_CNC'
            WHEN Nganh_hang = 'Vàng Ta' AND nhom_sp_nho = 'PT' 
                THEN 'Vàng 24K_PT'
            WHEN Nganh_hang = 'Vàng Ta' AND nhom_sp_nho = '24K Đá màu' 
                THEN 'Vàng 24K_24K Đá màu'

            WHEN Nganh_hang = 'Vàng Tây' AND nhom_sp_nho = 'PC Ý 18K' 
                THEN 'Vàng Tây_PC Ý 18K'
            WHEN Nganh_hang = 'Vàng Tây' AND nhom_sp_nho = 'PC Ý 10K' 
                THEN 'Vàng Tây_PC Ý 10K'
            WHEN Nganh_hang = 'Vàng Tây' AND nhom_sp_nho = 'PC Hàn Quốc' 
                THEN 'Vàng Tây_PC Hàn Quốc'
            WHEN Nganh_hang = 'Vàng Tây' AND nhom_sp_nho = 'Nhẫn Cưới' 
                THEN 'Vàng Tây_Nhẫn cưới'
            WHEN Nganh_hang = 'Vàng Tây' AND nhom_sp_nho = 'Nhẫn Cưới Kim Cương' 
                THEN 'Vàng Tây_Nhẫn Cưới Kim Cương'
            WHEN Nganh_hang = 'Vàng Tây' AND nhom_sp_nho = 'TS Nhập khẩu' 
                THEN 'Vàng Tây_Trang sức nhập khẩu Hàn Quốc'
            WHEN Nganh_hang = 'Vàng Tây' AND nhom_sp_nho = 'VT đá màu' 
                THEN 'Vàng Tây_VT đá màu'
    WHEN Nganh_hang = 'Vàng Tây' AND nhom_sp_nho = 'TS Kim cương' 
                THEN 'Vàng Tây_TS Kim cương'
            WHEN Nganh_hang = 'Vàng Tây' AND nhom_sp_nho = 'Kim cương viên' 
                THEN 'Vàng Tây_Kim cương viên'
            when Ma_Hang in ('BTMVV49KD0-501005-001', 'BTMVV49KD0-501004-001') then 'Khác_Quà tặng'

            ELSE CONCAT(Nganh_hang,'_',nhom_sp_nho)
        END AS Dongsp_fix
    from base
)

select distinct *
from final