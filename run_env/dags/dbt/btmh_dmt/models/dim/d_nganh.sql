select
    ID,
    Ma_Nganh,
    Ten_Nganh,
    Ten_NganhE,
    current_timestamp() as UpdateTime
from {{ source(var('d_nganh_schema') | lower, 'dmnganh') }}

