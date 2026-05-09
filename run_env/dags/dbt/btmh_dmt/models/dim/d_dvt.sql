select
    ID,
    Ma_Dvt,
    Ten_Dvt,
    Ghi_Chu,
    current_timestamp() as UpdateTime
from {{ source(var('d_dvt_schema') | lower, 'dmdvt') }}
