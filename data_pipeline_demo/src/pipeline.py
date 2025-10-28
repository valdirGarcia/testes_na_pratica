from __future__ import annotations
from pathlib import Path
import pandas as pd

ALLOWED_STATES = {"SP","RJ","MG","PR"}

def extract(csv_path: str | Path) -> pd.DataFrame:
    """
    Lê o CSV bruto e retorna um DataFrame.
    """
    return pd.read_csv(csv_path)

def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpa e valida os dados:
      - normaliza UF
      - converte tipos (data e numérico)
      - remove linhas com dados ausentes/errados
      - remove gastos negativos
      - garante id único (mantém a última ocorrência)
    """
    out = df.copy()

    # normalização / tipagem
    out["state"] = out["state"].astype(str).str.upper()
    out["signup_date"] = pd.to_datetime(out["signup_date"], errors="coerce", utc=False)
    out["spending"] = pd.to_numeric(out["spending"], errors="coerce")

    # contrato de domínio + nulidade
    out = out[out["state"].isin(ALLOWED_STATES)]
    out = out.dropna(subset=["customer_id", "name", "signup_date", "spending"])

    # regras de negócio
    out = out[out["spending"] >= 0]

    # unicidade
    out = out.sort_index().drop_duplicates(subset=["customer_id"], keep="last")

    # dtypes finais (amigáveis a testes)
    out = out.astype({"customer_id": "int64"})
    return out.reset_index(drop=True)

def compute_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega métricas por UF.
    """
    by_state = (
        df.groupby("state", as_index=False)
          .agg(customers=("customer_id","nunique"),
               total_spending=("spending","sum"),
               avg_spending=("spending","mean"))
    )
    return by_state

def load(df: pd.DataFrame, out_path: str | Path) -> Path:
    """
    Grava CSV (idempotente) e retorna o caminho salvo.
    """
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    return out_path

def run_pipeline(project_root: str | Path) -> dict:
    root = Path(project_root)
    raw = root / "data" / "raw" / "customers.csv"
    processed_clean = root / "data" / "processed" / "customers_clean.csv"
    processed_features = root / "data" / "processed" / "spending_by_state.csv"

    df = extract(raw)
    clean = transform(df)
    feats = compute_features(clean)

    load(clean, processed_clean)
    load(feats, processed_features)

    return {
        "clean_path": str(processed_clean),
        "features_path": str(processed_features),
        "clean_rows": len(clean),
        "features_rows": len(feats),
    }

if __name__ == "__main__":
    root = Path(__file__).resolve().parents[1]
    stats = run_pipeline(root)
    print("Pipeline finalizado:", stats)
