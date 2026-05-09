with vbmm as (
    select *
    from {{ source((var('d_hang_schema', 'stg_augges_225') | lower), 'dmvbmm') }}
),

bost as (
    select * from vbmm where cast(cDM as string) = 'BOST'
),
nhomct as (
    select * from vbmm where cast(cDM as string) = 'NHOMCT'
),
chungl as (
    select * from vbmm where cast(cDM as string) = 'CHUNGL'
),
gioit as (
    select * from vbmm where cast(cDM as string) = 'GIOIT'
),
hoatiet as (
    select * from vbmm where cast(cDM as string) = 'HOATIET'
),
loaida as (
    select * from vbmm where cast(cDM as string) = 'LOAIDA'
),
tendc as (
    select * from vbmm where cast(cDM as string) = 'TENDC'
),
hamlkl as (
    select * from vbmm where cast(cDM as string) = 'HAMLKL'
),

base as (
    select
        cast(dmnmm.Ma_NM as string) as MA_MAU,
        cast(dmnh.Ma_Nhom as string) as NHOM_HANG,
        cast(dmnh.Ten_Nhom as string) as TEN_NHOM_HANG,
        cast(dmnmm.ngay_tao_ma_mau as datetime) as NGAY_TAO_MA_MAU,
        cast(dmnmm.Anh_NM as string) as ANH_MA_MAU,
        cast(dmnmm.Ten_NM as string) as TEN_MA_MAU,
        cast(dmnmm.Mo_Ta as string) as MO_TA_MA_MAU,
        cast(bost.Ma as string) as MA_BO_SUU_TAP,
        cast(bost.Ten as string) as BO_SUU_TAP,
        cast(nhomct.Ma as string) as MA_NHOM_CHI_TIET,
        cast(nhomct.Ten as string) as TEN_NHOM_CHI_TIET,
        cast(chungl.Ma as string) as MA_CHUNG_LOAI,
        cast(chungl.Ten as string) as CHUNG_LOAI,
        cast(gioit.Ten as string) as GIOI_TINH_SAN_PHAM,
        cast(hoatiet.Ten as string) as HOA_TIET_MAT,
        cast(loaida.Ten as string) as LOAI_DA,
        cast(tendc.Ten as string) as TEN_DA_CHU,
        cast(hamlkl.Ma as string) as MA_HAM_LUONG_KIM_LOAI,
        cast(hamlkl.Ten as string) as HAM_LUONG_KIM_LOAI
    from {{ ref('d_hang') }} dmh
    left join {{ ref('d_nhom') }} dmnh
        on safe_cast(dmnh.ID as int64) = safe_cast(dmh.ID_Nhom as int64)
    left join {{ ref('d_nhom_ma_mau') }} dmnmm
        on safe_cast(dmnmm.ID as int64) = safe_cast(dmh.ID_NMM as int64)
    left join bost
        on safe_cast(dmnmm.ID_BoST as int64) = safe_cast(bost.ID_Stt as int64)
    left join nhomct
        on safe_cast(dmnmm.ID_NhomCt as int64) = safe_cast(nhomct.ID_Stt as int64)
    left join chungl
        on safe_cast(dmnmm.ID_ChungL as int64) = safe_cast(chungl.ID_Stt as int64)
    left join gioit
        on safe_cast(dmnmm.ID_GioiT as int64) = safe_cast(gioit.ID_Stt as int64)
    left join hoatiet
        on safe_cast(dmnmm.ID_HoaTiet as int64) = safe_cast(hoatiet.ID_Stt as int64)
    left join loaida
        on safe_cast(dmnmm.ID_LoaiDa as int64) = safe_cast(loaida.ID_Stt as int64)
    left join tendc
        on safe_cast(dmnmm.ID_TenDC as int64) = safe_cast(tendc.ID_Stt as int64)
    left join hamlkl
        on safe_cast(dmnmm.ID_HamLKL as int64) = safe_cast(hamlkl.ID_Stt as int64)
    where dmnmm.Ma_NM is not null
),

ma_mau_profile as (
    select
        MA_MAU,
        max(NHOM_HANG) as NHOM_HANG,
        max(TEN_NHOM_HANG) as TEN_NHOM_HANG,
        max(NGAY_TAO_MA_MAU) as NGAY_TAO_MA_MAU,
        max(ANH_MA_MAU) as ANH_MA_MAU,
        max(TEN_MA_MAU) as TEN_MA_MAU,
        max(MO_TA_MA_MAU) as MO_TA_MA_MAU,
        max(MA_BO_SUU_TAP) as MA_BO_SUU_TAP,
        max(BO_SUU_TAP) as BO_SUU_TAP,
        max(MA_NHOM_CHI_TIET) as MA_NHOM_CHI_TIET,
        max(TEN_NHOM_CHI_TIET) as TEN_NHOM_CHI_TIET,
        max(MA_CHUNG_LOAI) as MA_CHUNG_LOAI,
        max(CHUNG_LOAI) as CHUNG_LOAI,
        max(GIOI_TINH_SAN_PHAM) as GIOI_TINH_SAN_PHAM,
        max(HOA_TIET_MAT) as HOA_TIET_MAT,
        max(LOAI_DA) as LOAI_DA,
        max(TEN_DA_CHU) as TEN_DA_CHU,
        max(MA_HAM_LUONG_KIM_LOAI) as MA_HAM_LUONG_KIM_LOAI,
        max(HAM_LUONG_KIM_LOAI) as HAM_LUONG_KIM_LOAI
    from base
    group by MA_MAU
),

normalized as (
    select
        *,
        lower(
            trim(
                concat(
                    coalesce(MA_MAU, ''), ' ',
                    coalesce(NHOM_HANG, ''), ' ',
                    coalesce(TEN_NHOM_HANG, ''), ' ',
                    coalesce(TEN_MA_MAU, ''), ' ',
                    coalesce(MO_TA_MA_MAU, ''), ' ',
                    coalesce(MA_BO_SUU_TAP, ''), ' ',
                    coalesce(BO_SUU_TAP, ''), ' ',
                    coalesce(MA_NHOM_CHI_TIET, ''), ' ',
                    coalesce(TEN_NHOM_CHI_TIET, ''), ' ',
                    coalesce(MA_CHUNG_LOAI, ''), ' ',
                    coalesce(CHUNG_LOAI, ''), ' ',
                    coalesce(HOA_TIET_MAT, ''), ' ',
                    coalesce(LOAI_DA, ''), ' ',
                    coalesce(TEN_DA_CHU, ''), ' ',
                    coalesce(MA_HAM_LUONG_KIM_LOAI, ''), ' ',
                    coalesce(HAM_LUONG_KIM_LOAI, '')
                )
            )
        ) as SEARCH_TEXT,
        case
            when regexp_contains(lower(coalesce(LOAI_DA, '')), r'(kim cương|kim cuong|diamond|ruby|sapphire|emerald|cz|cvd|ngọc|ngoc|đá|da)') then true
            when regexp_contains(lower(coalesce(TEN_DA_CHU, '')), r'(kim cương|kim cuong|diamond|ruby|sapphire|emerald|cz|cvd|ngọc|ngoc|đá|da)') then true
            else false
        end as CO_TIN_HIEU_DA,
        case
            when regexp_contains(lower(coalesce(BO_SUU_TAP, '')), r'(cô dâu|co dau|hồi môn|hoi mon|đính hôn|dinh hon|cưới|cuoi)') then true
            when regexp_contains(lower(coalesce(TEN_NHOM_CHI_TIET, '')), r'(nhẫn cưới|nhan cuoi)') then true
            else false
        end as CO_TIN_HIEU_CUOI,
        case
            when regexp_contains(lower(coalesce(BO_SUU_TAP, '')), r'(trống đồng|trong dong|tứ linh|tu linh|tứ quý|tu quy|phong thủy|phong thuy|con giáp|con giap|bình an|binh an|thịnh vượng|thinh vuong|tài lộc|tai loc|kim gia bảo|kim gia bao|tiểu kim cát|tieu kim cat)') then true
            when regexp_contains(lower(coalesce(MO_TA_MA_MAU, '')), r'(trống đồng|trong dong|tứ linh|tu linh|tứ quý|tu quy|phong thủy|phong thuy|con giáp|con giap)') then true
            else false
        end as CO_TIN_HIEU_VAN_HOA,
        case
            when regexp_contains(lower(coalesce(BO_SUU_TAP, '')), r'(quà tặng|qua tang|valentine|love|trái tim|trai tim|chúc mừng|chuc mung|trẻ em|tre em)') then true
            when regexp_contains(lower(coalesce(TEN_NHOM_CHI_TIET, '')), r'(quà tặng|qua tang|khuyến mại|khuyen mai|biếu tặng|bieu tang)') then true
            else false
        end as CO_TIN_HIEU_QUA_TANG
    from ma_mau_profile
),

classified_step_1 as (
    select
        *,
        case
            when MA_NHOM_CHI_TIET in ('TT', 'AT') then 'Investment Metal'
            when MA_CHUNG_LOAI in ('SJ', 'KG', 'VR', 'NT', 'XV', 'MV', 'EV', 'EB', 'DB') then 'Investment Metal'
            when regexp_contains(coalesce(NHOM_HANG, ''), r'^(NL24|NLTT|KGB|TTS|TTV|TT)') then 'Investment Metal'
            when MA_NHOM_CHI_TIET in ('QT', '23', '24', '19') then 'Decorative Ornaments'
            when MA_CHUNG_LOAI in ('TV', 'TM', 'TH', 'TB', 'NV') then 'Decorative Ornaments'
            else 'Jewelry'
        end as TOP_SEGMENT_NEW,
        case
            when MA_HAM_LUONG_KIM_LOAI = '92' or regexp_contains(coalesce(HAM_LUONG_KIM_LOAI, ''), r'92\.5') then 'S925'
            when MA_NHOM_CHI_TIET in ('BA', 'BX', 'BY', 'BC', 'AT') then 'S925'
            when MA_HAM_LUONG_KIM_LOAI = '24' or regexp_contains(coalesce(HAM_LUONG_KIM_LOAI, ''), r'(24K|999\.9|99\.9)') then '24K'
            when MA_HAM_LUONG_KIM_LOAI = '23' or regexp_contains(coalesce(HAM_LUONG_KIM_LOAI, ''), r'23K') then '23K'
            when MA_HAM_LUONG_KIM_LOAI = '18' or regexp_contains(coalesce(HAM_LUONG_KIM_LOAI, ''), r'75\.0') then '18K'
            when MA_HAM_LUONG_KIM_LOAI = '14' or regexp_contains(coalesce(HAM_LUONG_KIM_LOAI, ''), r'58\.5') then '14K'
            when MA_HAM_LUONG_KIM_LOAI = '10' or regexp_contains(coalesce(HAM_LUONG_KIM_LOAI, ''), r'41\.7') then '10K'
            else 'Others'
        end as MATERIAL_NEW
    from normalized
),

classified_step_2 as (
    select
        *,
        case
            when TOP_SEGMENT_NEW = 'Investment Metal' then 'Investment Metal'
            when TOP_SEGMENT_NEW = 'Decorative Ornaments' then 'Others'
            when MA_NHOM_CHI_TIET in ('BA', 'BX', 'BY', 'BC') or MATERIAL_NEW = 'S925' then 'Silver Jewelry'
            when MA_NHOM_CHI_TIET in ('VD', 'VT', 'TK') or CO_TIN_HIEU_DA then 'Gemset Gold Jewelry'
            else 'Gold Jewelry'
        end as PRODUCT_SEGMENT_NEW,
        case
            when MA_CHUNG_LOAI in ('CK', 'CN', 'CY') and regexp_contains(SEARCH_TEXT, r'(đính hôn|dinh hon|engagement)') then 'Engagement Ring'
            when MA_CHUNG_LOAI in ('CK', 'CN', 'CY') then 'Wedding Ring'
            when MA_CHUNG_LOAI in ('N1', 'N2', 'NK') then 'Ring'
            when MA_CHUNG_LOAI in ('BT', 'KH') then 'Earring'
            when MA_CHUNG_LOAI in ('D1', 'CC') then 'Chain'
            when MA_CHUNG_LOAI in ('D2', 'TR') then 'Necklace'
            when MA_CHUNG_LOAI = 'MA' then 'Pendant'
            when MA_CHUNG_LOAI in ('LT', 'CT') then 'Bracelet'
            when MA_CHUNG_LOAI = 'LC' then 'Anklet'
            when MA_CHUNG_LOAI in ('VT', 'VX', 'KI', 'KX') then 'Bangle'
            when MA_CHUNG_LOAI in ('CH', 'DL') then 'Charm'
            when MA_CHUNG_LOAI in ('TV', 'TM') then 'Statue'
            when MA_CHUNG_LOAI in ('MV', 'XV', 'DB', 'SJ', 'VR', 'NT', 'EV', 'EB') then 'Coin/bar'
            when MA_CHUNG_LOAI = 'CO' then 'Combo'
            when regexp_contains(lower(coalesce(CHUNG_LOAI, '')), r'(viên đá|vien da|kim cương viên|kim cuong vien|đá màu|da mau)') then 'Blocking Stone'
            when MA_CHUNG_LOAI in ('TH', 'NV', 'TB') then 'Ornaments'
            else 'Others'
        end as JEWELRY_TYPE_NEW
    from classified_step_1
),

classified_step_3 as (
    select
        *,
        case
            when TOP_SEGMENT_NEW = 'Investment Metal' then 'Investment'
            when CO_TIN_HIEU_CUOI then 'Wedding'
            when CO_TIN_HIEU_QUA_TANG then 'Gifting'
            when CO_TIN_HIEU_VAN_HOA then 'VN Culture'
            when MA_NHOM_CHI_TIET in ('PH', 'BY', 'BC') then 'Fashion Daily'
            when PRODUCT_SEGMENT_NEW = 'Silver Jewelry' then 'Fashion Daily'
            when PRODUCT_SEGMENT_NEW in ('Gold Jewelry', 'Gemset Gold Jewelry') then 'Modern Classic'
            when TOP_SEGMENT_NEW = 'Decorative Ornaments' then 'Gifting'
            else 'Others'
        end as PRODUCT_LINE_NEW
    from classified_step_2
),

classified_step_4 as (
    select
        *,
        case
            when TOP_SEGMENT_NEW = 'Investment Metal' then 'Sales-Driven'
            when PRODUCT_LINE_NEW = 'Wedding' and JEWELRY_TYPE_NEW in ('Wedding Ring', 'Engagement Ring') then 'Signature'
            when PRODUCT_SEGMENT_NEW = 'Gemset Gold Jewelry'
                and (MATERIAL_NEW in ('10K', '14K', '18K') or MA_NHOM_CHI_TIET in ('VT', 'TK')) then 'Image'
            when PRODUCT_LINE_NEW in ('Gifting', 'VN Culture') then 'Sales-Driven'
            when PRODUCT_SEGMENT_NEW = 'Silver Jewelry' and PRODUCT_LINE_NEW = 'Fashion Daily' then 'Sales-Driven'
            when TOP_SEGMENT_NEW = 'Decorative Ornaments' then 'Sales-Driven'
            else 'Core'
        end as PRODUCT_CATEGORY_NEW
    from classified_step_3
),

final as (
    select
        MA_MAU,
        MA_MAU as `MA MAU`,
        NHOM_HANG,
        NHOM_HANG as `NHOM HANG`,
        TEN_NHOM_HANG,
        NGAY_TAO_MA_MAU,
        NGAY_TAO_MA_MAU as `NGAY TAO MA MAU`,
        ANH_MA_MAU,
        ANH_MA_MAU as `ANH MA MAU`,
        TEN_MA_MAU,
        MO_TA_MA_MAU,
        MA_BO_SUU_TAP,
        BO_SUU_TAP,
        MA_NHOM_CHI_TIET,
        TEN_NHOM_CHI_TIET as DANH_MUC_SAN_PHAM,
        TEN_NHOM_CHI_TIET as `DANH MUC SAN PHAM`,
        MA_CHUNG_LOAI,
        CHUNG_LOAI,
        GIOI_TINH_SAN_PHAM,
        HOA_TIET_MAT,
        LOAI_DA,
        TEN_DA_CHU,
        MA_HAM_LUONG_KIM_LOAI,
        HAM_LUONG_KIM_LOAI,
        TOP_SEGMENT_NEW,
        TOP_SEGMENT_NEW as `TOP SEGMENT NEW`,
        PRODUCT_SEGMENT_NEW,
        PRODUCT_SEGMENT_NEW as `PRODUCT SEGMENT NEW`,
        PRODUCT_LINE_NEW,
        PRODUCT_LINE_NEW as `PRODUCT LINE NEW`,
        PRODUCT_CATEGORY_NEW,
        PRODUCT_CATEGORY_NEW as `PRODUCT CATEGORY NEW`,
        JEWELRY_TYPE_NEW,
        JEWELRY_TYPE_NEW as `JEWELRY TYPE NEW`,
        MATERIAL_NEW,
        MATERIAL_NEW as `MATERIAL NEW`,
        coalesce(nullif(BO_SUU_TAP, ''), nullif(TEN_NHOM_CHI_TIET, '')) as COLLECTION_NEW,
        coalesce(nullif(BO_SUU_TAP, ''), nullif(TEN_NHOM_CHI_TIET, '')) as `COLLECTION NEW`,
        'tong_quat_hoa_tu_raw' as CLASSIFICATION_METHOD,
        'tong_quat_hoa_tu_raw' as `CLASSIFICATION METHOD`,
        case
            when JEWELRY_TYPE_NEW = 'Others' then true
            when PRODUCT_LINE_NEW = 'Others' then true
            when MATERIAL_NEW = 'Others' and TOP_SEGMENT_NEW <> 'Decorative Ornaments' then true
            else false
        end as CAN_XEM_XET,
        case
            when JEWELRY_TYPE_NEW = 'Others' then true
            when PRODUCT_LINE_NEW = 'Others' then true
            when MATERIAL_NEW = 'Others' and TOP_SEGMENT_NEW <> 'Decorative Ornaments' then true
            else false
        end as `CAN XEM XET`,
        case
            when JEWELRY_TYPE_NEW = 'Others' then 'thiếu_quy_tắc_chủng_loại_từ_raw'
            when PRODUCT_LINE_NEW = 'Others' then 'thiếu_quy_tắc_product_line_từ_raw'
            when MATERIAL_NEW = 'Others' and TOP_SEGMENT_NEW <> 'Decorative Ornaments' then 'thiếu_quy_tắc_hàm_lượng_từ_raw'
            else null
        end as LY_DO_XEM_XET,
        case
            when JEWELRY_TYPE_NEW = 'Others' then 'thiếu_quy_tắc_chủng_loại_từ_raw'
            when PRODUCT_LINE_NEW = 'Others' then 'thiếu_quy_tắc_product_line_từ_raw'
            when MATERIAL_NEW = 'Others' and TOP_SEGMENT_NEW <> 'Decorative Ornaments' then 'thiếu_quy_tắc_hàm_lượng_từ_raw'
            else null
        end as `LY DO XEM XET`,
        SEARCH_TEXT,
        current_timestamp() as UpdateTime
    from classified_step_4
)

select *
from final