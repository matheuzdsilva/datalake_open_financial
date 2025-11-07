import os
import requests
from datetime import datetime
from src.utils.io import write_raw
from dotenv import load_dotenv

load_dotenv()

EIA_API_KEY = os.getenv("EIA_API_KEY")
EIA_BASE_URL = "https://api.eia.gov/v2"

def fetch_brent(api_key: str):
    if not api_key:
        raise EnvironmentError("Defina a variável EIA_API_KEY no ambiente ou no .env")

    url = f"{EIA_BASE_URL}/petroleum/pri/spt/data/?frequency=weekly&data[0]=value&facets[series][]=RBRTE&start=2024-01-01&api_key={api_key}"

    headers = {
        "Accept": "application/json",
        "User-Agent": "OpenMacroLake/1.0"
    }

    print(f"[INFO] Requisitando dados do Brent (EIA API v2)...")
    resp = requests.get(url, headers=headers, timeout=30)
    if resp.status_code == 404:
        print(f"[ERROR] Endpoint não encontrado: {url}")
    resp.raise_for_status()
    return resp.json()

def normalize_eia(data: dict):
    if "response" not in data or "data" not in data["response"]:
        print("[WARN] Estrutura inesperada da resposta EIA.")
        return []

    registros = data["response"]["data"]

    rows = []
    for r in registros:
        try:
            rows.append({
                "data_ref": r["period"],
                "valor": float(r["value"]),
                "indicador": "brent_spot_usd_bbl",
                "fonte": "eia",
                "data_ingestao": datetime.utcnow().isoformat()
            })
        except Exception as e:
            print(f"[WARN] Erro ao converter registro: {e}")
            continue
    print(f"[INFO] {len(rows)} registros normalizados da EIA.")
    return rows

def run_eia_ingestion():
    raw = fetch_brent(EIA_API_KEY)
    rows = normalize_eia(raw)
    if rows:
        write_raw(rows, "eia", "petroleo")
        print(f"[INFO] Dados do petróleo Brent salvos com sucesso.")
    else:
        print("[WARN] Nenhum dado EIA salvo.")

if __name__ == "__main__":
    run_eia_ingestion()