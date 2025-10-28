from pathlib import Path
import pandas as pd
import pytest

from src.pipeline import extract, transform, compute_features, load, ALLOWED_STATES

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw" / "customers.csv"

def test_extract_basic_contract():
    df = extract(RAW)
    assert isinstance(df, pd.DataFrame)
    assert set(df.columns) == {"customer_id","name","signup_date","state","spending"}
    assert len(df) == 10

def test_transform_contract_and_rules():
    df = extract(RAW)
    clean = transform(df)

    # Esquema / tipos
    assert set(clean.columns) == {"customer_id","name","signup_date","state","spending"}
    assert pd.api.types.is_integer_dtype(clean["customer_id"])
    assert pd.api.types.is_datetime64_any_dtype(clean["signup_date"])
    assert pd.api.types.is_numeric_dtype(clean["spending"])

    # Regras e domínios
    assert clean["spending"].ge(0).all()
    assert clean["state"].isin(ALLOWED_STATES).all()
    assert clean["customer_id"].is_unique

    # Com os dados de exemplo, esperamos 6 linhas válidas
    assert len(clean) == 6

def test_compute_features_consistency():
    df = extract(RAW)
    clean = transform(df)
    feats = compute_features(clean)

    assert set(feats.columns) == {"state","customers","total_spending","avg_spending"}
    # Soma de 'customers' bate com ids únicos
    assert feats["customers"].sum() == clean["customer_id"].nunique()
    # Média * clientes ~= total (tolerância numérica)
    approx_total = (feats["avg_spending"] * feats["customers"]).sum()
    assert approx_total == pytest.approx(feats["total_spending"].sum(), rel=1e-6, abs=1e-6)

def test_end_to_end_smoke(tmp_path):
    df = extract(RAW)
    clean = transform(df)
    out_clean = tmp_path / "clean.csv"
    out_feats = tmp_path / "features.csv"
    load(clean, out_clean)
    feats = compute_features(clean)
    load(feats, out_feats)

    assert out_clean.exists() and out_clean.stat().st_size > 0
    assert out_feats.exists() and out_feats.stat().st_size > 0
