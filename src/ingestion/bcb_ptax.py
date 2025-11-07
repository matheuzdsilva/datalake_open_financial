from datetime import datetime, date
import requests
from src.utils.io import write_raw

BCB_SERIES = {
    "dolar_ptax_compra": 1,
    "dolar_ptax_venda": 10813
}

def fetch_bcb_series(serie_id: int, start: date, end: date):
    data_inicial = start.strftime("%d/%m/%Y")
    data_final = end.strftime("%d/%m/%Y")
    url = (
        f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{serie_id}/dados"
        f"?formato=json&dataInicial={data_inicial}&dataFinal={data_final}"
    )

    headers = {
        "Accept": "application/json, text/plain, */*",
        "User-Agent": "OpenMacroLake/1.0"
    }

    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.json()

def run_bcb_ingestion():
    start_date = date(2024, 1, 1)
    end_date = date.today()

    all_data = []
    for nome, serie_id in BCB_SERIES.items():
        print(f"[INFO] Coletando série {nome} ({serie_id}) do BCB de {start_date} a {end_date}")
        dados = fetch_bcb_series(serie_id, start_date, end_date)
        for d in dados:
            d["indicador"] = nome
            d["fonte"] = "bcb"
            d["data_ingestao"] = datetime.utcnow().isoformat()
        all_data.extend(dados)

    write_raw(all_data, "bcb", "cambio")

if __name__ == "__main__":
    run_bcb_ingestion()