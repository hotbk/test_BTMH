select
    ID,
    Ma_Nhom,
    Ten_Nhom,
    Ten_NhomE,
    ID_NhomMe,
    Cap_Nhom,
    SubID,
    SoTT,
    current_timestamp() as UpdateTime
from {{ source(var('d_nhom_schema') | lower, 'dmnh') }}
