"""Quick BigQuery sanity checks for r_ton_kho_ngay inputs.

Reads:
- dwh_fact.f_ton_kho
- dwh_mart.r_ton_kho_ngay

This script is read-only.
"""

from __future__ import annotations

from google.cloud import bigquery


def main() -> None:
    project = "btmh-dwh-485609"
    client = bigquery.Client(project=project)

    def fetch_one(sql: str) -> dict:
        rows = list(client.query(sql).result())
        if not rows:
            return {}
        return dict(rows[0])

    def fetch_all(sql: str) -> list[dict]:
        return [dict(r) for r in client.query(sql).result()]

    f_summary = fetch_one(
        f"""
        select
          max(ngay) as max_ngay,
          count(*) as row_cnt,
          sum(so_luong_ton) as sum_sl_ton,
          sum(gia_tri_ton) as sum_gia_tri
        from `{project}.dwh_fact.f_ton_kho`
        """
    )
    print("f_ton_kho summary:", f_summary)

    max_ngay = f_summary.get("max_ngay")
    if max_ngay:
        by_nguon = fetch_all(
            f"""
            select
              nguon,
              count(*) as row_cnt,
              sum(so_luong_ton) as sum_sl_ton,
              sum(gia_tri_ton) as sum_gia_tri
            from `{project}.dwh_fact.f_ton_kho`
            where ngay = date('{max_ngay}')
            group by 1
            order by sum_sl_ton desc
            limit 20
            """
        )
        print(f"f_ton_kho by nguon @ {max_ngay}:")
        for r in by_nguon:
            print("  ", r)

    try:
                r_summary = fetch_one(
                        f"""
                        select
                            max(Ngay) as max_ngay,
                            count(*) as row_cnt,
                            sum(Sl_Ton) as sum_sl_ton,
                            sum(Gia_Tri) as sum_gia_tri
                        from `{project}.dwh_mart.r_ton_kho_ngay`
                        """
                )
                print("r_ton_kho_ngay summary:", r_summary)

                r_max_day = r_summary.get("max_ngay")
                if r_max_day:
                        print(f"r_ton_kho_ngay by Nguon @ {r_max_day}:")
                        rows = fetch_all(
                                f"""
                                select
                                    Nguon,
                                    count(*) as row_cnt,
                                    countif(Sl_Ton = 0) as rows_sl_ton_0,
                                    countif(Sl_Ton > 0) as rows_sl_ton_pos,
                                    sum(Sl_Ton) as sum_sl_ton,
                                    sum(Gia_Tri) as sum_gia_tri,
                                    sum(Trong_luong_ton) as sum_trong_luong_ton,
                                    sum(Tong_trong_luong_ton) as sum_tong_trong_luong_ton
                                from `{project}.dwh_mart.r_ton_kho_ngay`
                                where Ngay = date('{r_max_day}')
                                group by 1
                                order by sum_sl_ton desc
                                """
                        )
                        for r in rows:
                                print("  ", r)

                        print("Sample rows where Sl_Ton > 0:")
                        samples = fetch_all(
                                f"""
                                select
                                    Nguon, Ngay, Ma_Kho, Nhom_hang,
                                    Sl_Ton, Gia_Tri, T_Luong, Ten_Dai_Chi
                                from `{project}.dwh_mart.r_ton_kho_ngay`
                                where Ngay = date('{r_max_day}')
                                    and Sl_Ton > 0
                                limit 20
                                """
                        )
                        for s in samples:
                                print("  ", s)
    except Exception as e:  # pragma: no cover
        print("r_ton_kho_ngay query failed:", type(e).__name__, str(e)[:400])


if __name__ == "__main__":
    main()
