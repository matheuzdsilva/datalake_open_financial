import requests
from datetime import datetime, date
from src.utils.io import write_raw

IBGE_BASE_URL = "https://servicodados.ibge.gov.br/api/v3/agregados"
AGREGADO_IPCA = 1737
VARIAVEL_IPCA = 63
LOCALIDADE_BRASIL = "N1[all]"

def build_periodo_str(start: date, end: date) -> str:
    return f"{start.strftime('%Y%m')}-{end.strftime('%Y%m')}"

def fetch_ipca_2024_plus():
    start = date(2024, 1, 1)
    end = date.today()
    periodos = build_periodo_str(start, end)
    url = (
        f"{IBGE_BASE_URL}/{AGREGADO_IPCA}/periodos/{periodos}/"
        f"variaveis/{VARIAVEL_IPCA}?localidades={LOCALIDADE_BRASIL}"
    )
    headers = {
        "Accept": "application/json",
        "User-Agent": "OpenMacroLake/1.0"
    }
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.json()

def normalize_ipca(raw_json: list):
    if not raw_json:
        print("[WARN] JSON vazio.")
        return []

    if isinstance(raw_json, list):
        item = raw_json[0]
    else:
        item = raw_json

    if "resultados" not in item:
        print(f"[WARN] Estrutura inesperada: {item.keys()}")
        return []

    resultados = item["resultados"][0]["series"][0]["serie"]

    rows = []
    for periodo, valor in resultados.items():
        try:
            ano = int(periodo[:4])
            mes = int(periodo[4:6])
            valor_float = float(valor.replace(",", "."))
            rows.append({
                "ano": ano,
                "mes": mes,
                "periodo": periodo,
                "valor": valor_float,
                "indicador": "ipca_mensal",
                "fonte": "ibge",
                "data_ingestao": datetime.utcnow().isoformat()
            })
        except Exception as e:
            print(f"[WARN] Erro ao converter {periodo}='{valor}': {e}")
            continue

    print(f"[INFO] IPCA normalizado com {len(rows)} registros.")
    return rows

def run_ipca_ingestion():
    raw = fetch_ipca_2024_plus()
    rows = normalize_ipca(raw)
    if rows:
        write_raw(rows, "ibge", "ipca")
        print(f"[INFO] IPCA: {len(rows)} registros salvos.")
    else:
        print("[WARN] Nenhum registro salvo para IPCA.")

if __name__ == "__main__":
    run_ipca_ingestion()