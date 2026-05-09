
{% set dhang_sources = var(
  'dhang_sources',
  [
    { 'source': 'stg_augges_225', 'code': 'NY' },
    { 'source': 'stg_augges_224', 'code': 'SX' },
    { 'source': 'stg_augges_226', 'code': 'HD' },
    { 'source': 'stg_augges_227', 'code': 'BN' },
    { 'source': 'stg_augges_sg', 'code': 'SG' }
  ]
) %}

with src as (
	{% for s in dhang_sources %}
	select
		'{{ s.code }}' as Ma_vung,
		concat('{{ s.code }}', '|', cast(dh.ID as string)) as ID_Key,
		dh.*
	from (
		{{ btmh_d_hang_select(btmh_source_name(s)) }}
	) as dh
	{% if not loop.last %}
	union all
	{% endif %}
	{% endfor %}
)

select * from src
