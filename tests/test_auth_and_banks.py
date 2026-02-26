import importlib
import sys
from pathlib import Path

import pandas as pd
from fastapi.testclient import TestClient


FIXTURES = Path(__file__).parent / "fixtures"


def load_main_module(db_path: Path):
    import os

    os.environ["EXTRACTO_DB_PATH"] = str(db_path)
    os.environ["ENABLE_MAIN_FILE_ENDPOINT"] = "false"

    if "main" in sys.modules:
        return importlib.reload(sys.modules["main"])
    import main

    return main


def _sample_df():
    return pd.DataFrame(
        [
            {
                "fecha": "01/01/24",
                "descripcion": "SALDO INICIAL",
                "debito": None,
                "credito": 100.0,
                "saldo": 100.0,
                "control": None,
                "control_diff": None,
                "categoria": "Saldo inicial",
            }
        ]
    )


def test_auth_flow_uses_sqlite_storage(tmp_path):
    mod = load_main_module(tmp_path / "extracto-test.db")
    client = TestClient(mod.app)

    register = client.post("/auth/register", json={"email": "user@example.com", "password": "123456"})
    assert register.status_code == 200

    login = client.post("/auth/login", json={"email": "user@example.com", "password": "123456"})
    assert login.status_code == 200
    token = login.json()["token"]

    me = client.get("/auth/me", headers={"Authorization": f"Bearer {token}"})
    assert me.status_code == 200
    assert me.json()["is_paid"] is False

    pay = client.post(
        "/billing/pay",
        json={"amount": 10},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert pay.status_code == 200

    me_after = client.get("/auth/me", headers={"Authorization": f"Bearer {token}"})
    assert me_after.status_code == 200
    assert me_after.json()["is_paid"] is True

    logout = client.post("/auth/logout", headers={"Authorization": f"Bearer {token}"})
    assert logout.status_code == 200

    me_logged_out = client.get("/auth/me", headers={"Authorization": f"Bearer {token}"})
    assert me_logged_out.status_code == 401


def test_bank_extract_routing_with_pdf_fixtures(tmp_path, monkeypatch):
    mod = load_main_module(tmp_path / "extracto-test-routing.db")

    monkeypatch.setattr(mod, "extract_macro_table", lambda _bytes: _sample_df())
    monkeypatch.setattr(mod, "extract_nacion_table", lambda _bytes: _sample_df())
    monkeypatch.setattr(mod, "extract_santander_table", lambda _bytes: _sample_df())

    macro_bytes = (FIXTURES / "macro_sample.pdf").read_bytes()
    nacion_bytes = (FIXTURES / "nacion_sample.pdf").read_bytes()
    santander_bytes = (FIXTURES / "santander_sample.pdf").read_bytes()

    _, macro_label = mod._extract_bank_dataframe("macro", macro_bytes)
    _, nacion_label = mod._extract_bank_dataframe("nacion", nacion_bytes)
    _, santander_label = mod._extract_bank_dataframe("santander", santander_bytes)

    assert macro_label == "Macro"
    assert nacion_label == "Nación"
    assert santander_label == "Santander"
