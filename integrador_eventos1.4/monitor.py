"""
monitor.py — Monitoração do Event Log Builder.

Grava dados de uso em monitoring/monitoring.xlsx com duas abas:
  sessoes  — uma linha por sessão
  arquivos — uma linha por arquivo processado

ID_Execucao formato: EXC-YYYYMMDD-NNN (ex: EXC-20250310-001)
"""

import getpass
from pathlib import Path
from datetime import datetime
from threading import Lock
import pandas as pd

MONITOR_DIR  = Path("monitoring")
MONITOR_FILE = MONITOR_DIR / "monitoring.xlsx"
_lock        = Lock()

CABECALHOS = {
    "sessoes": [
        "ID_Execucao", "Responsavel", "Data_Execucao", "Tempo_Sessao",
        "Fontes_Integradas", "Total_Fontes", "Atividades_Selecionadas",
        "N_Linhas_Total", "N_Cases_Total", "Criptografou", "Concluiu_Export",
    ],
    "arquivos": [
        "ID_Execucao", "Responsavel", "Data_Execucao",
        "Fonte", "Arquivo", "Formato", "Tamanho_MB", "Linhas",
    ],
}

# Mapas em memória (válidos enquanto o app estiver rodando)
_id_map:     dict[str, str] = {}  # session_id -> ID_Execucao
_inicio_map: dict[str, str] = {}  # session_id -> timestamp inicio


# ── Setup ─────────────────────────────────────────────────
def inicializar():
    MONITOR_DIR.mkdir(exist_ok=True)
    if not MONITOR_FILE.exists():
        with pd.ExcelWriter(MONITOR_FILE, engine="openpyxl") as writer:
            for aba, cols in CABECALHOS.items():
                pd.DataFrame(columns=cols).to_excel(writer, sheet_name=aba, index=False)


# ── ID legível ────────────────────────────────────────────
def _gerar_id_execucao() -> str:
    """Gera EXC-YYYYMMDD-NNN. Chamar DENTRO do lock."""
    hoje = datetime.now().strftime("%Y%m%d")
    try:
        df = pd.read_excel(MONITOR_FILE, sheet_name="sessoes", dtype=str).fillna("")
        if not df.empty and "ID_Execucao" in df.columns:
            seq = len(df[df["ID_Execucao"].str.startswith(f"EXC-{hoje}-")]) + 1
        else:
            seq = 1
    except Exception:
        seq = 1
    return f"EXC-{hoje}-{seq:03d}"


def _get_id(session_id: str) -> str:
    return _id_map.get(session_id, session_id[:8])


# ── Leitura / escrita ─────────────────────────────────────
def _ler_todas() -> dict[str, pd.DataFrame]:
    abas = {}
    for aba, cols in CABECALHOS.items():
        try:
            abas[aba] = pd.read_excel(MONITOR_FILE, sheet_name=aba, dtype=str).fillna("")
        except Exception:
            abas[aba] = pd.DataFrame(columns=cols)
    return abas


def _salvar_todas(abas: dict[str, pd.DataFrame]):
    with pd.ExcelWriter(MONITOR_FILE, engine="openpyxl") as writer:
        for aba, df in abas.items():
            df.to_excel(writer, sheet_name=aba, index=False)


def _agora_data() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _tempo_sessao(inicio_str: str) -> str:
    try:
        inicio   = datetime.strptime(inicio_str, "%Y-%m-%d %H:%M:%S")
        segundos = int((datetime.now() - inicio).total_seconds())
        h = segundos // 3600
        m = (segundos % 3600) // 60
        s = segundos % 60
        return f"{h:02d}:{m:02d}:{s:02d}"
    except Exception:
        return ""


def _usuario() -> str:
    try:
        return getpass.getuser()
    except Exception:
        return ""


# ── Eventos ───────────────────────────────────────────────

def log_session_inicio(session_id: str):
    try:
        _inicio_map[session_id] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with _lock:
            id_exec = _gerar_id_execucao()
            _id_map[session_id] = id_exec
            abas = _ler_todas()
            nova = pd.DataFrame([{
                "ID_Execucao":             id_exec,
                "Responsavel":             _usuario(),
                "Data_Execucao":           _agora_data(),
                "Tempo_Sessao":            "",
                "Fontes_Integradas":       "",
                "Total_Fontes":            "0",
                "Atividades_Selecionadas": "",
                "N_Linhas_Total":          "",
                "N_Cases_Total":           "",
                "Criptografou":            "não",
                "Concluiu_Export":         "não",
            }])
            abas["sessoes"] = pd.concat([abas["sessoes"], nova], ignore_index=True)
            _salvar_todas(abas)
    except Exception:
        pass


def log_etapa(session_id: str, etapa: str):
    pass  # mantido para compatibilidade com app.py


def log_file(session_id: str, source_name: str, file_path: str,
             rows: int, cols: int):
    try:
        p       = Path(file_path)
        size_mb = round(p.stat().st_size / (1024 * 1024), 2) if p.exists() else ""
        fmt     = p.suffix.lower().lstrip(".")
        id_exec = _get_id(session_id)

        with _lock:
            abas = _ler_todas()

            nova = pd.DataFrame([{
                "ID_Execucao":  id_exec,
                "Responsavel":  _usuario(),
                "Data_Execucao":_agora_data(),
                "Fonte":        source_name,
                "Arquivo":      p.name,
                "Formato":      fmt,
                "Tamanho_MB":   size_mb,
                "Linhas":       rows,
            }])
            abas["arquivos"] = pd.concat([abas["arquivos"], nova], ignore_index=True)

            mask = abas["sessoes"]["ID_Execucao"] == id_exec
            if mask.any():
                atual = abas["sessoes"].loc[mask, "Fontes_Integradas"].values[0]
                nomes = [n.strip() for n in atual.split(";") if n.strip()]
                if source_name not in nomes:
                    nomes.append(source_name)
                abas["sessoes"].loc[mask, "Fontes_Integradas"] = "; ".join(nomes)
                abas["sessoes"].loc[mask, "Total_Fontes"]      = str(len(nomes))

            _salvar_todas(abas)
    except Exception:
        pass


def log_error(session_id: str, step: str, source_name: str,
              mensagem: str, tipo_erro: str = "geral"):
    pass  # mantido para compatibilidade com app.py


def log_export(session_id: str, estado: dict, opcao_export: str):
    try:
        exp     = estado.get("export", {})
        sources = estado.get("sources", [])
        cripto  = estado.get("colunas_cripto", [])
        id_exec = _get_id(session_id)

        todas_ativs = []
        for s in sources:
            for a in s.get("activities_selected", []):
                if a not in todas_ativs:
                    todas_ativs.append(a)

        with _lock:
            abas = _ler_todas()
            mask = abas["sessoes"]["ID_Execucao"] == id_exec
            if not mask.any():
                return
            abas["sessoes"].loc[mask, "Tempo_Sessao"]            = _tempo_sessao(_inicio_map.get(session_id, ""))
            abas["sessoes"].loc[mask, "Fontes_Integradas"]       = "; ".join(s["source_name"] for s in sources)
            abas["sessoes"].loc[mask, "Total_Fontes"]            = str(len(sources))
            abas["sessoes"].loc[mask, "Atividades_Selecionadas"] = "; ".join(todas_ativs)
            abas["sessoes"].loc[mask, "N_Linhas_Total"]          = str(exp.get("row_count", ""))
            abas["sessoes"].loc[mask, "N_Cases_Total"]           = str(exp.get("unique_cases", ""))
            abas["sessoes"].loc[mask, "Criptografou"]            = "sim" if cripto else "não"
            abas["sessoes"].loc[mask, "Concluiu_Export"]         = "sim"
            _salvar_todas(abas)
    except Exception:
        pass


def log_session_fim(session_id: str, estado: dict):
    try:
        sources  = estado.get("sources", [])
        id_exec  = _get_id(session_id)
        concluiu = "sim" if estado.get("export", {}).get("csv_path") or \
                            estado.get("export", {}).get("sql_path") else "não"

        todas_ativs = []
        for s in sources:
            for a in s.get("activities_selected", []):
                if a not in todas_ativs:
                    todas_ativs.append(a)

        with _lock:
            abas = _ler_todas()
            mask = abas["sessoes"]["ID_Execucao"] == id_exec
            if not mask.any():
                return
            abas["sessoes"].loc[mask, "Tempo_Sessao"]            = _tempo_sessao(_inicio_map.get(session_id, ""))
            abas["sessoes"].loc[mask, "Total_Fontes"]            = str(len(sources))
            abas["sessoes"].loc[mask, "Atividades_Selecionadas"] = "; ".join(todas_ativs)
            abas["sessoes"].loc[mask, "Concluiu_Export"]         = concluiu
            _salvar_todas(abas)
    except Exception:
        pass
