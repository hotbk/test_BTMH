with base as (
    select
        cast(dmh.ID as int64) as ID_HANG,
        cast(dmh.Ma_Hang as string) as MA_HANG,
        cast(dmh.Ma_Vach as string) as MA_VACH,
        cast(dmh.Ten_Hang as string) as TEN_HANG,
        cast(dmh.Ten_HangE as string) as TEN_HANG_E,
        cast(dmh.Ma_Nhom as string) as MA_NHOM_HANG,
        cast(dmnh.Ten_Nhom as string) as TEN_NHOM_HANG,
        cast(dmh.Ham_Luong as string) as HAM_LUONG_HANG,
        cast(dmnmm.Ma_NM as string) as MA_MAU,
        cast(dmnmm.Ten_NM as string) as TEN_MA_MAU,
        cast(dmnmm.Mo_Ta as string) as MO_TA_MA_MAU,
        cast(dmh.ID_NMM as int64) as ID_NMM
    from {{ ref('d_hang') }} dmh
    left join {{ ref('d_nhom') }} dmnh
        on safe_cast(dmnh.ID as int64) = safe_cast(dmh.ID_Nhom as int64)
    left join {{ ref('d_nhom_ma_mau') }} dmnmm
        on safe_cast(dmnmm.ID as int64) = safe_cast(dmh.ID_NMM as int64)
),

final as (
    select
        b.ID_HANG,
        b.MA_HANG,
        b.MA_VACH,
        b.TEN_HANG,
        b.TEN_HANG_E,
        b.MA_NHOM_HANG,
        b.TEN_NHOM_HANG,
        b.HAM_LUONG_HANG,
        b.ID_NMM,
        b.MA_MAU,
        b.MA_MAU as `MA MAU`,
        b.TEN_MA_MAU,
        b.MO_TA_MA_MAU,
        c.PRODUCT_SEGMENT_NEW,
        c.`PRODUCT SEGMENT NEW`,
        c.PRODUCT_LINE_NEW,
        c.`PRODUCT LINE NEW`,
        c.PRODUCT_CATEGORY_NEW,
        c.`PRODUCT CATEGORY NEW`,
        c.JEWELRY_TYPE_NEW,
        c.`JEWELRY TYPE NEW`,
        c.MATERIAL_NEW,
        c.`MATERIAL NEW`,
        c.COLLECTION_NEW,
        c.`COLLECTION NEW`,
        c.CLASSIFICATION_METHOD,
        c.`CLASSIFICATION METHOD`,
        c.CAN_XEM_XET,
        c.`CAN XEM XET`,
        c.LY_DO_XEM_XET,
        c.`LY DO XEM XET`,
        current_timestamp() as UpdateTime
    from base b
    left join {{ ref('d_ma_mau_new') }} c
        on cast(c.MA_MAU as string) = cast(b.MA_MAU as string)
)

select *
from final