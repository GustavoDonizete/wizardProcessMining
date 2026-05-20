"""
monitor.py — Monitoração do Event Log Builder.

Grava dados de uso em monitoring/monitoring.xlsx com duas abas:
  sessao — uma linha por sessão
  files  — uma linha por arquivo processado
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
    "sessao": [
        "ID_Sessao", "Nome_Base", "Responsavel", "Data_Execucao",
        "Timestamp_Inicio", "Timestamp_Fim", "Tempo_Sessao",
        "Colunas_Criptografadas", "Ultima_Etapa", "Concluiu_Export",
        "Total_Fontes", "Atividades_Selecionadas",
        "N_Linhas_Total", "N_Cases_Total", "N_Atividades_Total",
    ],
    "files": [
        "ID_Sessao", "Timestamp", "Nome_Source", "Nome_File",
        "Formato", "Size_KB", "Rows", "Cols",
    ],
}


# ── Setup ─────────────────────────────────────────────────
def inicializar():
    """Cria monitoring/ e monitoring.xlsx se não existirem."""
    MONITOR_DIR.mkdir(exist_ok=True)
    if not MONITOR_FILE.exists():
        with pd.ExcelWriter(MONITOR_FILE, engine="openpyxl") as writer:
            for aba, cols in CABECALHOS.items():
                pd.DataFrame(columns=cols).to_excel(writer, sheet_name=aba, index=False)


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


def _agora() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _tempo_sessao(inicio_str: str) -> str:
    """Calcula duração no formato HH:MM:SS."""
    try:
        inicio  = datetime.strptime(inicio_str, "%Y-%m-%d %H:%M:%S")
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
    """Cria a linha da sessão no momento do início."""
    try:
        agora = _agora()
        with _lock:
            abas = _ler_todas()
            nova = pd.DataFrame([{
                "ID_Sessao":              session_id,
                "Nome_Base":             "",
                "Responsavel":           _usuario(),
                "Data_Execucao":         agora[:10],
                "Timestamp_Inicio":      agora,
                "Timestamp_Fim":         "",
                "Tempo_Sessao":          "",
                "Colunas_Criptografadas":"",
                "Ultima_Etapa":          "step1",
                "Concluiu_Export":       "não",
                "Total_Fontes":          "0",
                "Atividades_Selecionadas":"",
                "N_Linhas_Total":        "",
                "N_Cases_Total":         "",
                "N_Atividades_Total":    "",
            }])
            abas["sessao"] = pd.concat([abas["sessao"], nova], ignore_index=True)
            _salvar_todas(abas)
    except Exception:
        pass


def log_etapa(session_id: str, etapa: str):
    """Atualiza a última etapa concluída."""
    try:
        with _lock:
            abas = _ler_todas()
            mask = abas["sessao"]["ID_Sessao"] == session_id
            if mask.any():
                abas["sessao"].loc[mask, "Ultima_Etapa"] = etapa
                _salvar_todas(abas)
    except Exception:
        pass


def log_file(session_id: str, source_name: str, file_path: str,
             rows: int, cols: int):
    """Registra um arquivo processado e atualiza Nome_Base na sessão."""
    try:
        p       = Path(file_path)
        size_kb = round(p.stat().st_size / 1024, 1) if p.exists() else ""
        fmt     = p.suffix.lower().lstrip(".")

        with _lock:
            abas = _ler_todas()

            # Linha em files
            nova = pd.DataFrame([{
                "ID_Sessao":   session_id,
                "Timestamp":   _agora(),
                "Nome_Source": source_name,
                "Nome_File":   p.name,
                "Formato":     fmt,
                "Size_KB":     size_kb,
                "Rows":        rows,
                "Cols":        cols,
            }])
            abas["files"] = pd.concat([abas["files"], nova], ignore_index=True)

            # Atualiza Nome_Base na sessão (acumula com ;)
            mask = abas["sessao"]["ID_Sessao"] == session_id
            if mask.any():
                atual = abas["sessao"].loc[mask, "Nome_Base"].values[0]
                nomes = [n.strip() for n in atual.split(";") if n.strip()]
                if source_name not in nomes:
                    nomes.append(source_name)
                abas["sessao"].loc[mask, "Nome_Base"] = "; ".join(nomes)

            _salvar_todas(abas)
    except Exception:
        pass


def log_error(session_id: str, step: str, source_name: str,
              mensagem: str, tipo_erro: str = "geral"):
    """Registra um erro atualizando a última etapa (sem aba separada)."""
    try:
        with _lock:
            abas = _ler_todas()
            mask = abas["sessao"]["ID_Sessao"] == session_id
            if mask.any():
                abas["sessao"].loc[mask, "Ultima_Etapa"] = step + "_erro"
                _salvar_todas(abas)
    except Exception:
        pass


def log_export(session_id: str, estado: dict, opcao_export: str):
    """Fecha a sessão com todos os dados da exportação."""
    try:
        exp     = estado.get("export", {})
        sources = estado.get("sources", [])
        cripto  = estado.get("colunas_cripto", [])

        # Atividades únicas de todas as fontes
        todas_ativs = []
        for s in sources:
            for a in s.get("activities_selected", []):
                if a not in todas_ativs:
                    todas_ativs.append(a)

        nomes_bases = "; ".join(s["source_name"] for s in sources)

        with _lock:
            abas = _ler_todas()
            mask = abas["sessao"]["ID_Sessao"] == session_id
            if not mask.any():
                return

            inicio_str = abas["sessao"].loc[mask, "Timestamp_Inicio"].values[0]
            fim        = _agora()

            abas["sessao"].loc[mask, "Nome_Base"]              = nomes_bases
            abas["sessao"].loc[mask, "Timestamp_Fim"]          = fim
            abas["sessao"].loc[mask, "Tempo_Sessao"]           = _tempo_sessao(inicio_str)
            abas["sessao"].loc[mask, "Colunas_Criptografadas"] = "; ".join(cripto) if cripto else ""
            abas["sessao"].loc[mask, "Ultima_Etapa"]           = "step6"
            abas["sessao"].loc[mask, "Concluiu_Export"]        = "sim"
            abas["sessao"].loc[mask, "Total_Fontes"]           = str(len(sources))
            abas["sessao"].loc[mask, "Atividades_Selecionadas"]= "; ".join(todas_ativs)
            abas["sessao"].loc[mask, "N_Linhas_Total"]         = str(exp.get("row_count", ""))
            abas["sessao"].loc[mask, "N_Cases_Total"]          = str(exp.get("unique_cases", ""))
            abas["sessao"].loc[mask, "N_Atividades_Total"]     = str(exp.get("unique_activities", ""))

            _salvar_todas(abas)
    except Exception:
        pass


def log_session_fim(session_id: str, estado: dict):
    """Fecha sessão sem exportação (abandono ou nova sessão)."""
    try:
        sources     = estado.get("sources", [])
        todas_ativs = []
        for s in sources:
            for a in s.get("activities_selected", []):
                if a not in todas_ativs:
                    todas_ativs.append(a)

        with _lock:
            abas = _ler_todas()
            mask = abas["sessao"]["ID_Sessao"] == session_id
            if not mask.any():
                return

            inicio_str = abas["sessao"].loc[mask, "Timestamp_Inicio"].values[0]
            fim        = _agora()
            concluiu   = "sim" if estado.get("export", {}).get("csv_path") or \
                                  estado.get("export", {}).get("sql_path") else "não"

            abas["sessao"].loc[mask, "Timestamp_Fim"]           = fim
            abas["sessao"].loc[mask, "Tempo_Sessao"]            = _tempo_sessao(inicio_str)
            abas["sessao"].loc[mask, "Concluiu_Export"]         = concluiu
            abas["sessao"].loc[mask, "Total_Fontes"]            = str(len(sources))
            abas["sessao"].loc[mask, "Atividades_Selecionadas"] = "; ".join(todas_ativs)

            _salvar_todas(abas)
    except Exception:
        pass
