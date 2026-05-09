with src as (
    {% for s in var('dctkm_sources') %}
    select
        '{{ s.code }}' as Nguon,
        ID,
        ID_Dv,
        Tu_Ngay,
        Den_Ngay,
        Ky_Hieu,
        ListDay,
        Tu_Gio,
        Tu_Phut,
        Den_Gio,
        Den_Phut,
        cast(GT_HD as numeric) as GT_HD,
        Ck_Lan2,
        Ck_Lan2Sx,
        Sl_CSB,
        HD_NG,
        HD_All,
        Hang_HD,
        ListID_Sp,
        L_Sp,
        ListID_Dt,
        L_Dt,
        ListID_Kho,
        ListID_NK,
        Tyle_DS,
        Noi_Dung,
        Tt,
        Status,
        CSB1Lan,
        isNM,
        InActive,
        InsertDate,
        LastEdit,
        UserID,
        CSB1SN,
        MarkRow
    from {{ source(btmh_source_name(s), 'csb') }}
    {% if not loop.last %}
    union all
    {% endif %}
    {% endfor %}
)

select distinct * from src
