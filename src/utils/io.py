import json
import os
from datetime import datetime

def write_raw(obj, source: str, dataset: str):
    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    base_path = os.path.join("data", "raw", source, dataset)
    os.makedirs(base_path, exist_ok=True)
    file_path = os.path.join(base_path, f"{dataset}_{timestamp}.json")

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

    print(f"[INFO] Arquivo salvo em {file_path}")
    return file_path
