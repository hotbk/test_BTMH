{% set src_schema = (var('d_hang_schema', 'stg_augges_225') | lower) %}

with
h as (
	select *
	from {{ source(src_schema, 'dmh') }}
),

nmm as (
	select *
	from {{ source(src_schema, 'dmnmm') }}
),

nh as (
	select *
	from {{ source(src_schema, 'dmnh') }}
),

vbmm as (
	select *
	from {{ source(src_schema, 'dmvbmm') }}
),

nl as (
	select *
	from vbmm
	where cDM = 'NHOMLON'
),

gt as (
	select *
	from vbmm
	where cDM = 'GIOIT'
),

hl as (
	select *
	from vbmm
	where cDM = 'HAMLKL'
),

vbtg as (
	select *
	from {{ source(src_schema, 'dmvbtg') }}
),

qttg as (
	select *
	from {{ source(src_schema, 'dmqttg') }}
),

dth as (
	select *
	from {{ source(src_schema, 'dmdt') }}
),

ggs as (
	select *
	from {{ source('ggs', 'ggs_co_cau') }}
),

tt as (
	select
		Ty_Gia
	from {{ source(src_schema, 'dmtt') }}
	where Ma_Tt = 'USD'
	qualify row_number() over (order by Ma_Tt) = 1
),

joined as (
	select
		h.ID,
		h.ID_Nhom,
		h.Ten_HangN as ID_Dai_Chi,
		ggs.ten_mo_ta as Ten_Dai_Chi,
		ggs.dai_chi as Dai_Chi,
		h.Ma_Vach,
		h.Ma_Hang,
		h.Ma_Tong,
		nh.Ma_Nhom,
		nl.Ma as Ma_Nhom_Lon,
		h.Ten_Hang,
		h.Ten_HangE,
		h.Ten_HangN,
		h.Ten_Hang1 as Ten_hang_in_tren_tem,
		gt.Ten as Gioi_Tinh,
		hl.Ten as Ham_Luong,
		h.Ghi_Chu as Ghi_chu1,
		h.Ten_Hang2 as Ghi_chu2,
		h.ID_NMM,
		h.ID_Dvt,
		h.Chat_Lieu,
		h.ID_Nganh,
		h.ID_Sx,
		h.So_Seri,
		cast(h.Tk_Hh as string) as Tk_Hh,
		h.T_Luong,
		h.The_Tich,
		h.T_LuongTT,
		h.Vi_Tri as Ten_In_hoa_don,
		h.Luu_Kho,
		h.ID_DtH,
		h.ID_VBTG,
		h.Ty_Gia as Ty_gia_vang_mua,
		h.Tyle_Lai,
		h.Tien_Lai,
		h.Gia_ban,
		h.Gia_BL,
		h.Gia_Ban1,
		h.Gia_Ban2,
		h.Gia_Ban3,
		h.Gia_Ban4,
		h.Gia_Ban5,
		h.Gia_Ban6,
		h.Gia_Ban7,
		h.Gia_Ban8,
		h.Gia_Ban9,
		h.Gia_Ban10,
		h.Gia_Ban11,
		h.Gia_Ban12,
		h.Tyle_GBL,
		h.TyLe_CLGB as TienCong_TrungGian,
		h.Sl_Max as TyGiaNT_TrungGian,
		h.Sl_Min,
		vbtg.TyGia_Ban,
		vbtg.TyGia_Mua,
		qttg.Ma_QTTG,
		qttg.He_So1,
		qttg.He_So2,
		qttg.He_So3,
		qttg.He_So4,
		qttg.He_So5,
		qttg.He_So6,
		cast(h.Ngay_Nhap as date) as Ngay_Nhap,
		h.LastEdit as UpdateTime,
		cast(dth.Ma_Dt as string) as Ma_NCC,
		tt.Ty_Gia as Ty_Gia_USD
	from h
	left join nmm
		on h.ID_NMM = nmm.ID
	left join nh
		on h.ID_Nhom = nh.ID
	left join nl
		on nmm.ID_NHOMLON = nl.ID_Stt
	left join gt
		on nmm.ID_GIOIT = gt.ID_Stt
	left join hl
		on nmm.ID_HamLKL = hl.ID_Stt
	left join vbtg
		on vbtg.ID = h.ID_VBTG
	left join qttg
		on qttg.ID = h.ID_QTTG
	left join dth
		on h.ID_DtH = dth.ID
	left join ggs
		on cast(h.Ten_HangN as string) = cast(ggs.id_nhom_hang_theo_co_cau as string)
	cross join tt
),

calc as (
	select
		j.* except (
			Ty_Gia_USD,
			T_Luong,
			The_Tich,
			T_LuongTT,
			He_So1,
			He_So2,
			He_So4,
			Sl_Min,
			Luu_Kho,
			Gia_Ban1,
			Gia_Ban2,
			Gia_Ban3,
			Gia_Ban4,
			Gia_Ban5,
			Gia_Ban7,
			Gia_Ban8,
			Gia_Ban9,
			Gia_Ban10
		),

		coalesce(safe_cast(j.Ty_Gia_USD as bignumeric), 0) as ty_gia_usd,
		coalesce(safe_cast(j.TyGia_Ban as bignumeric), 0) as ty_gia_ban_vang,

		coalesce(safe_cast(j.T_Luong as bignumeric), 0) as t_luong,
		coalesce(safe_cast(j.The_Tich as bignumeric), 0) as the_tich,
		coalesce(safe_cast(j.T_LuongTT as bignumeric), 0) as t_luong_tt,

		coalesce(safe_cast(j.He_So1 as bignumeric), 0) as he_so_1,
		coalesce(safe_cast(j.He_So2 as bignumeric), 0) as he_so_2,
		coalesce(safe_cast(j.He_So4 as bignumeric), 0) as he_so_4,

		coalesce(safe_cast(j.Sl_Min as bignumeric), 0) as sl_min,
		coalesce(safe_cast(j.Luu_Kho as bignumeric), 0) as luu_kho,
		safe_cast(j.Luu_Kho as bignumeric) as ty_gia_nt_mua,

		coalesce(safe_cast(j.Gia_Ban1 as bignumeric), 0) as gia_ban_1,
		coalesce(safe_cast(j.Gia_Ban2 as bignumeric), 0) as gia_ban_2,
		coalesce(safe_cast(j.Gia_Ban3 as bignumeric), 0) as gia_ban_3,
		coalesce(safe_cast(j.Gia_Ban4 as bignumeric), 0) as gia_ban_4,
		coalesce(safe_cast(j.Gia_Ban5 as bignumeric), 0) as gia_ban_5,
		coalesce(safe_cast(j.Gia_Ban7 as bignumeric), 0) as gia_ban_7,
		coalesce(safe_cast(j.Gia_Ban8 as bignumeric), 0) as gia_ban_8,
		coalesce(safe_cast(j.Gia_Ban9 as bignumeric), 0) as gia_ban_9,
		coalesce(safe_cast(j.Gia_Ban10 as bignumeric), 0) as gia_ban_10,

		(coalesce(safe_cast(j.T_Luong as bignumeric), 0)
		 + coalesce(safe_cast(j.The_Tich as bignumeric), 0)
		 + coalesce(safe_cast(j.T_LuongTT as bignumeric), 0)
		) as tong_trong_luong_tinh
	from joined j
)

select
	ID,
	ID_Nhom,
	ID_Dai_Chi,
	Ten_Dai_Chi,
	Dai_Chi,
	Ma_Vach,
	Ma_Hang,
	Ma_Tong,
	Ma_Nhom,
	Ma_Nhom_Lon,
	Ten_Hang,
	Ten_HangE,
	Ten_HangN,
	Ten_hang_in_tren_tem,
	Gioi_Tinh,
	Ham_Luong,
	Ghi_chu1,
	Ghi_chu2,
	ID_NMM,
	ID_Dvt,
	Chat_Lieu,
	ID_Nganh,
	ID_Sx,
	So_Seri,
	Tk_Hh,

	t_luong as T_Luong,
	the_tich as The_Tich,
	t_luong_tt as T_LuongTT,
	tong_trong_luong_tinh as Tong_TL,

	Ten_In_hoa_don,
	ty_gia_nt_mua as Ty_gia_NT_mua,
	ID_DtH,
	ID_VBTG,
	Ty_gia_vang_mua,
	safe_cast(Tyle_Lai as bignumeric) as Tyle_Lai,
	Tien_Lai,
	safe_cast(Gia_ban as bignumeric) as Gia_ban,
	safe_cast(Gia_BL as bignumeric) as Gia_BL,

	gia_ban_1 as Tien_da_ban,
	gia_ban_2 as Tien_da_ban_NT,
	gia_ban_3 as Tien_da_mua,
	gia_ban_4 as Tien_da_mua_NT,
	gia_ban_5 as Tien_cong_ban,
	safe_cast(Gia_Ban6 as bignumeric) as Tien_cong_ban_NT,
	gia_ban_7 as Tien_cong_mua,
	gia_ban_8 as Tien_cong_mua_NT,
	gia_ban_9 as Ban_TronGoi,
	gia_ban_10 as Ban_TronGoi_NT,
	safe_cast(Gia_Ban11 as bignumeric) as Mua_TronGoi,
	safe_cast(Gia_Ban12 as bignumeric) as Mua_TronGoi_NT,
	safe_cast(Tyle_GBL as bignumeric) as Hao_Gia_cong,
	TienCong_TrungGian,
	safe_cast(TyGiaNT_TrungGian as bignumeric) as TyGiaNT_TrungGian,
	sl_min as Ty_gia_vang_TrungGian,
	TyGia_Ban,
	TyGia_Mua,
	Ma_QTTG,

	he_so_1 as He_So1,
	he_so_2 as He_So2,
	safe_cast(He_So3 as bignumeric) as He_So3,
	he_so_4 as He_So4,
	safe_cast(He_So5 as bignumeric) as He_So5,
	safe_cast(He_So6 as bignumeric) as He_So6,

	Ngay_Nhap,
	UpdateTime,
	Ma_NCC,

	case
		when (substr(Ma_Nhom, 1, 2) = 'VT') or (Ma_Nhom = 'PLN1') then 'VT đá màu'
		when (substr(Ma_Nhom, 1, 2) in ('PY', 'CN')) and (substr(Ma_Nhom, -2, 2) = '18') then 'PC Ý 18K'
		when (
			((substr(Ma_Nhom, 1, 2) = 'CN') and (substr(Ma_Nhom, -2, 2) in ('10', '14')))
			or ((substr(Ma_Nhom, 1, 2) = 'PY') and (strpos(Ma_Nhom, '10') > 0))
		) then 'PC Ý 10K'
		when (substr(Ma_Nhom, 1, 2) = 'CY') or (substr(Ma_Nhom, 1, 4) in ('NCCN', 'NCCY')) then 'Nhẫn Cưới'
		when substr(Ma_Nhom, 1, 4) = 'NCCK' then 'Nhẫn Cưới Kim Cương'
		when substr(Ma_Nhom, 1, 2) = 'PH' then 'PC Hàn Quốc'
		when substr(Ma_Nhom, 1, 2) = 'NH' then 'TS Nhập khẩu'
		when substr(Ma_Nhom, 1, 2) = 'TK' then 'TS Kim cương'
		when (substr(Ma_Nhom, 1, 2) = 'KC') or (substr(Ma_Nhom, 1, 3) = 'KDD') then 'Kim cương viên'
		when substr(Ma_Nhom, -2, 2) = 'PT' then 'PT'
		when substr(Ma_Nhom, -2, 2) = '2D' then 'CN'
		when substr(Ma_Nhom, -2, 2) = '3D' then '3D'
		when substr(Ma_Nhom, -2, 2) = '5G' then 'CNC'
		when substr(Ma_Nhom, 1, 2) = 'VD' then '24K Đá màu'
		when (substr(Ma_Nhom, 1, 4) in ('QT24', '24DV', 'QTLG', 'QTTV', 'QTDV')) or (Ma_Nhom = 'BTDVV24') then '24K Quà tặng'
		when (substr(Ma_Nhom, 1, 3) = 'KGB') or (Ma_Nhom in ('NL24', 'NLTT', 'KHS')) then 'KGB'
		when substr(Ma_Nhom, 1, 3) = 'TTS' then 'SJC'
		when substr(Ma_Nhom, 1, 3) = 'TTV' then 'VRTL'
		when (substr(Ma_Nhom, 1, 2) = 'PT') or (substr(Ma_Nhom, 1, 4) = 'KDVD') or (substr(Ma_Nhom, 1, 2) = 'KH') then 'Phong Thủy'
		when (substr(Ma_Nhom, 1, 4) in ('QTTM', 'QTEV')) or (Ma_Hang in ('BTMVV49KD0-501004-001', 'BTMVV49KD0-501005-001')) then 'Quà tặng'
		when substr(Ma_Nhom, 1, 2) in ('BA', 'BX', 'BC', 'BY') then 'Bạc'
		when Ma_Hang in ('BTMVV49KD0-501001-001', 'BTMVV49KD0-501002-001', 'BTMVV49KD0-501003-001') then 'Tiểu kim cát'
		when (
			substr(Ma_Nhom, 1, 7) in (
				'BTCHV10', 'BTBTV10', 'BTMAV10', 'BTLTV10',
				'BTCHV14', 'BTBTV14', 'BTMAV14', 'BTLTV14'
			)
			or ((substr(Ma_Nhom, 1, 3) = 'BTD') and (substr(Ma_Nhom, -3, 3) in ('V10', 'V14')))
			or ((substr(Ma_Nhom, 1, 3) = 'BTN') and (substr(Ma_Nhom, -3, 3) in ('V10', 'V14')))
		) then 'BST'
		when (Ma_Nhom = 'NLVT') and (Ma_Hang != 'NLBAC') then 'NLVT'
		when Ma_Hang = 'NLBAC' then 'NLBAC'
		else 'Khác'
	end as dong_san_pham,

	case
		when Ma_QTTG in ('GB-10K-ORD', 'GB-14K-ORD', 'GB-18K-ORD') then
			he_so_1 * he_so_2 * ty_gia_ban_vang * t_luong + gia_ban_5 + gia_ban_1 + gia_ban_2 * ty_gia_usd
		when Ma_QTTG = 'GB-24VD' then
			t_luong * ty_gia_ban_vang + gia_ban_5 + gia_ban_1 + gia_ban_2 * ty_gia_usd
		when Ma_QTTG = 'GB-BA-TG' then
			gia_ban_9 + gia_ban_5 + gia_ban_1 + gia_ban_2 * ty_gia_usd
		when Ma_QTTG = 'GB-BAC' then
			t_luong * ty_gia_ban_vang + gia_ban_5 + gia_ban_1
		when substr(Ma_QTTG, 1, 8) = 'GB-CN-CC' then
			tong_trong_luong_tinh * (ty_gia_ban_vang + he_so_1 * he_so_2) + gia_ban_1 + gia_ban_2 * ty_gia_usd
		when Ma_QTTG = 'GB-CN-PT' then
			tong_trong_luong_tinh * (ty_gia_ban_vang + he_so_1 * he_so_2) + gia_ban_1 + gia_ban_2 * ty_gia_usd
		when Ma_QTTG = 'GB-CN-TG' then
			gia_ban_9 + gia_ban_10 * ty_gia_usd
		when Ma_QTTG = 'GB-KGB' then
			t_luong * ty_gia_ban_vang
		when substr(Ma_QTTG, 1, 7) = 'GB-NC-1' then
			tong_trong_luong_tinh * (sl_min + he_so_1)
		when substr(Ma_QTTG, 1, 8) = 'GB-NC-CC' then
			tong_trong_luong_tinh * (ty_gia_ban_vang + he_so_1 * he_so_2) + gia_ban_1 + gia_ban_2 * ty_gia_usd
		when Ma_QTTG = 'GB-NC-PT' then
			tong_trong_luong_tinh * (ty_gia_ban_vang + he_so_1 * he_so_2) + gia_ban_1 + gia_ban_2 * ty_gia_usd
		when substr(Ma_QTTG, 1, 9) = 'GB-NCKC-1' then
			he_so_1 * (t_luong * sl_min + gia_ban_7 + gia_ban_8 * luu_kho + gia_ban_3 + gia_ban_4 * luu_kho)
		when substr(Ma_QTTG, 1, 7) = 'GB-NH-1' then
			tong_trong_luong_tinh * (sl_min + he_so_1)
		when substr(Ma_QTTG, 1, 7) = 'GB-PH-1' then
			sl_min * tong_trong_luong_tinh * he_so_1
		when Ma_QTTG = 'GB-PH-TG' then
			gia_ban_9 + gia_ban_10 * ty_gia_usd
		when Ma_QTTG = 'GB-PT' then
			gia_ban_9 + gia_ban_10 * ty_gia_usd
		when substr(Ma_QTTG, 1, 7) = 'GB-PY-1' then
			tong_trong_luong_tinh * (sl_min + he_so_1)
		when substr(Ma_QTTG, 1, 8) = 'GB-PY-CC' then
			tong_trong_luong_tinh * (ty_gia_ban_vang + he_so_1 * he_so_2)
		when Ma_QTTG = 'GB-PY-PT' then
			tong_trong_luong_tinh * (ty_gia_ban_vang + he_so_1 * he_so_2)
		when Ma_QTTG = 'GB-SJC' then
			t_luong * ty_gia_ban_vang
		when Ma_QTTG = 'GB-TK-TG' then
			gia_ban_9 + gia_ban_10 * ty_gia_usd + gia_ban_1 + gia_ban_2 * ty_gia_usd
		when Ma_QTTG = 'GB-VRTL' then
			t_luong * ty_gia_ban_vang
		when substr(Ma_QTTG, 1, 7) = 'GB-VT-1' then
			he_so_1
			* (
				t_luong * he_so_2 * sl_min
				+ gia_ban_3
				+ (
					gia_ban_7
					+ coalesce(
						safe_divide(
							t_luong
							* coalesce(safe_cast(Tyle_GBL as bignumeric), 0)
							* coalesce(safe_cast(Ty_gia_vang_mua as bignumeric), 0),
							nullif(he_so_2, 0)
						),
						0
					)
				)
			)
			+ he_so_4
		when Ma_QTTG = 'GB-VT-TG' then
			gia_ban_9 + gia_ban_10 * ty_gia_usd
		when Ma_QTTG = 'TRONGOI-GB' then
			gia_ban_9 + gia_ban_10 * ty_gia_usd
		when Ma_QTTG = 'TTXVV24K-GIABAN' then
			t_luong * ty_gia_ban_vang
		else cast(0 as bignumeric)
	end as Gia_Ban_TT

from calc
