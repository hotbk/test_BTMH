

with src as (
	select
		cast(id_khach_hang as int64) as ma_khach_hang,
		ho_ten_khach_hang,
		cast(ngay_sinh as string) as ngay_sinh,
		cast(gioi_tinh as string) as gioi_tinh_raw,
		cast(cccd_cmt as string) as cccd_cmt,
		cast(dien_thoai as string) as dien_thoai,
		cast(email as string) as email,
		cast(tinh_thanhpho as string) as tinh_thanhpho,
		cast(quan_huyen as string) as quan_huyen,
		cast(coalesce(ngay_sua, ngay_tao) as timestamp) as source_updatetime
	from `btmh-airflow-dbt-lab-2026`.`stg_221_khach_hang`.`stg_khach_hang`

	
),

birth_parsed as (
	select
		*,
		case
			when safe.parse_date('%Y-%m-%d', ngay_sinh) is not null
				then safe.parse_date('%Y-%m-%d', ngay_sinh)
			when length(ngay_sinh) = 8 and strpos(ngay_sinh, '/') > 0
				then safe.parse_date('%e/%m/%Y', ngay_sinh)
			when length(ngay_sinh) = 8 and strpos(ngay_sinh, '/') = 0
				then safe.parse_date('%d%m%Y', ngay_sinh)
			when length(ngay_sinh) = 10
				then safe.parse_date('%d/%m/%Y', ngay_sinh)
			else null
		end as clean_dob
	from src
),

final as (
	select
		ma_khach_hang,

		-- 1) Giới tính 
		case
			when gioi_tinh_raw is null then 'KHÁC'
			when lower(trim(gioi_tinh_raw)) in ('', 'null', 'none') then 'KHÁC'
			when lower(trim(gioi_tinh_raw)) in ('khác', 'khac', 'other') then 'KHÁC'
			when lower(trim(gioi_tinh_raw)) in ('nam', 'male', 'm') then 'NAM'
			when lower(trim(gioi_tinh_raw)) in ('nữ', 'nu', 'female', 'f') then 'NỮ'
			-- Handle common mojibake / corrupted female values (e.g. N?, N÷, Ná»¯, Nỏằ�, ...)
			when regexp_contains(lower(trim(gioi_tinh_raw)), r'^n')
				and lower(trim(gioi_tinh_raw)) not in ('nam', 'null', 'none')
				then 'NỮ'
			else 'KHÁC'
		end as gioi_tinh,

		-- 2) Nhóm tuổi (age = floor(datediff(current_date, clean_dob)/365.25))
		case
			when floor(date_diff(current_date(), clean_dob, day) / 365.25) between 16 and 20 then '16-20T'
			when floor(date_diff(current_date(), clean_dob, day) / 365.25) between 21 and 25 then '21-25T'
			when floor(date_diff(current_date(), clean_dob, day) / 365.25) between 26 and 30 then '26-30T'
			when floor(date_diff(current_date(), clean_dob, day) / 365.25) between 31 and 35 then '31-35T'
			when floor(date_diff(current_date(), clean_dob, day) / 365.25) between 36 and 40 then '36-40T'
			when floor(date_diff(current_date(), clean_dob, day) / 365.25) between 41 and 45 then '41-45T'
			when floor(date_diff(current_date(), clean_dob, day) / 365.25) between 46 and 50 then '46-50T'
			when floor(date_diff(current_date(), clean_dob, day) / 365.25) between 51 and 55 then '51-55T'
			when floor(date_diff(current_date(), clean_dob, day) / 365.25) between 56 and 60 then '56-60T'
			else 'NHÓM KHÁC'
		end as nhom_tuoi,

		-- 3) Có CCCD
		case
			when regexp_contains(
				coalesce(
					regexp_replace(cccd_cmt, '-', ''),
					regexp_extract(ho_ten_khach_hang, r'(\d{9,12})')
				),
				r'^\d+$'
			) then 1
			else 0
		end as co_cccd,

		-- 4) Có Email
		case when email is not null then 1 else 0 end as co_email,

		-- 5) Có SĐT
		case
			when length(regexp_replace(dien_thoai, r'\D', '')) >= 10 then 1
			else 0
		end as co_dt,

		-- 6) Địa chỉ
		trim(tinh_thanhpho) as tinh,
		trim(quan_huyen) as quan,

		-- 7) UpdateTime
		source_updatetime as UpdateTime,
		dien_thoai as Dien_thoai,
		cccd_cmt,
		email
	from birth_parsed
)

select *
from final