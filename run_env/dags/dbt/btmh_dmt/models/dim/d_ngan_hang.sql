with src as (
	{% for s in var('dmnhang_sources') %}
	select
		'{{ s.code }}' as Nguon,
		ID_Dv,
		Ma_Nh,
		Ten_Nh as Ten_Nh,
		Ten_NhE,
		So_Tk,
		Noi_Mo,
		Dia_Chi,
		ID_Nhom,
		ID_Loai,
		Ghi_Chu,
		InsertDate,
		LastEdit,
		UserEdit,
		UserID,
		Inactive,
		Ky_Hieu,
		Thu_Huong as Thu_Huong,
		current_timestamp() as UpdateTime
	from {{ source(btmh_source_name(s), 'dmnhang') }}
	{% if not loop.last %}
	union all
	{% endif %}
	{% endfor %}
)

select distinct * from src
