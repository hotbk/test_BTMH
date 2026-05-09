select *
from {{ source('stg_ggs', 'vtt_exchange_rate') }}