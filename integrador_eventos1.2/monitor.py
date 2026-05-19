"""
monitor.py — Monitoração do Event Log Builder.

Grava dados de uso em monitoring/monitoring.xlsx (uma aba por categoria).
Chamado pelo app.py — sem impacto no front-end.

Abas do Excel:
  sessions  — uma linha por sessão
  files     — uma linha por arquivo processado
  exports   — uma linha por Event Log exportado
  errors    — uma linha por erro registrado
  params    — parâmetros de benchmark (preencher manualmente)
"""

from pathlib import Path
from datetime import datetime
from threading import Lock
import pandas as pd

MONITOR_DIR  = Path("monitoring")
MONITOR_FILE = MONITOR_DIR / "monitoring.xlsx"
_lock        = Lock()

CABECALHOS = {
    "sessions": [
        "session_id", "data_inicio", "data_fim", "duracao_minutos",
        "ultima_etapa_concluida", "concluiu_export",
        "total_fontes", "total_atividades_selecionadas",
    ],
    "files": [
        "session_id", "timestamp", "source_name", "file_name",
        "file_format", "file_size_kb", "rows", "cols",
    ],
    "exports": [
        "session_id", "timestamp", "total_eventos", "unique_cases",
        "unique_activities", "num_fontes_integradas", "formato_export",
        "criptografou", "colunas_cripto",
        "horas_economizadas", "valor_gerado_reais",
    ],
    "errors": [
        "session_id", "timestamp", "step", "source_name",
        "tipo_erro", "mensagem",
    ],
    "params": [
        "benchmark_horas_por_fonte", "custo_hora_reais",
        "nome_equipe", "data_inicio_uso",
    ],
}

PARAMS_EXEMPLO = {
    "benchmark_horas_por_fonte": "",
    "custo_hora_reais":          "",
    "nome_equipe":               "",
    "data_inicio_uso":           datetime.now().strftime("%Y-%m-%d"),
}


# ── Setup ─────────────────────────────────────────────────
def inicializar():
    """Cria monitoring/ e monitoring.xlsx se não existirem."""
    MONITOR_DIR.mkdir(exist_ok=True)
    if not MONITOR_FILE.exists():
        with pd.ExcelWriter(MONITOR_FILE, engine="openpyxl") as writer:
            for aba, cols in CABECALHOS.items():
                df = pd.DataFrame(columns=cols)
                if aba == "params":
                    df = pd.concat([df, pd.DataFrame([PARAMS_EXEMPLO])],
                                   ignore_index=True)
                df.to_excel(writer, sheet_name=aba, index=False)


# ── Leitura / escrita ─────────────────────────────────────
def _ler_todas() -> dict[str, pd.DataFrame]:
    """Lê todas as abas do Excel. Não usa lock (deve ser chamado dentro de lock)."""
    abas = {}
    for aba, cols in CABECALHOS.items():
        try:
            abas[aba] = pd.read_excel(MONITOR_FILE, sheet_name=aba, dtype=str).fillna("")
        except Exception:
            abas[aba] = pd.DataFrame(columns=cols)
    return abas


def _salvar_todas(abas: dict[str, pd.DataFrame]):
    """Salva todas as abas. Não usa lock (deve ser chamado dentro de lock)."""
    with pd.ExcelWriter(MONITOR_FILE, engine="openpyxl") as writer:
        for aba, df in abas.items():
            df.to_excel(writer, sheet_name=aba, index=False)


def _agora() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _ler_params() -> dict:
    """Lê parâmetros de benchmark. Thread-safe."""
    try:
        with _lock:
            df = pd.read_excel(MONITOR_FILE, sheet_name="params", dtype=str).fillna("")
        return df.iloc[0].to_dict() if not df.empty else {}
    except Exception:
        return {}


# ── Eventos ───────────────────────────────────────────────

def log_session_inicio(session_id: str):
    """Registra o início de uma sessão."""
    try:
        with _lock:
            abas = _ler_todas()
            nova = pd.DataFrame([{
                "session_id":                    session_id,
                "data_inicio":                   _agora(),
                "data_fim":                      "",
                "duracao_minutos":               "",
                "ultima_etapa_concluida":        "step1",
                "concluiu_export":               "não",
                "total_fontes":                  "0",
                "total_atividades_selecionadas": "0",
            }])
            abas["sessions"] = pd.concat([abas["sessions"], nova], ignore_index=True)
            _salvar_todas(abas)
    except Exception:
        pass


def log_etapa(session_id: str, etapa: str):
    """Atualiza a última etapa concluída da sessão."""
    try:
        with _lock:
            abas = _ler_todas()
            mask = abas["sessions"]["session_id"] == session_id
            if mask.any():
                abas["sessions"].loc[mask, "ultima_etapa_concluida"] = etapa
                _salvar_todas(abas)
    except Exception:
        pass


def log_file(session_id: str, source_name: str, file_path: str,
             rows: int, cols: int):
    """Registra um arquivo processado."""
    try:
        p       = Path(file_path)
        size_kb = round(p.stat().st_size / 1024, 1) if p.exists() else ""
        fmt     = p.suffix.lower().lstrip(".")
        with _lock:
            abas = _ler_todas()
            nova = pd.DataFrame([{
                "session_id":   session_id,
                "timestamp":    _agora(),
                "source_name":  source_name,
                "file_name":    p.name,
                "file_format":  fmt,
                "file_size_kb": size_kb,
                "rows":         rows,
                "cols":         cols,
            }])
            abas["files"] = pd.concat([abas["files"], nova], ignore_index=True)
            _salvar_todas(abas)
    except Exception:
        pass


def log_error(session_id: str, step: str, source_name: str,
              mensagem: str, tipo_erro: str = "geral"):
    """Registra um erro."""
    try:
        with _lock:
            abas = _ler_todas()
            nova = pd.DataFrame([{
                "session_id":  session_id,
                "timestamp":   _agora(),
                "step":        step,
                "source_name": source_name,
                "tipo_erro":   tipo_erro,
                "mensagem":    mensagem,
            }])
            abas["errors"] = pd.concat([abas["errors"], nova], ignore_index=True)
            _salvar_todas(abas)
    except Exception:
        pass


def log_export(session_id: str, estado: dict, opcao_export: str):
    """Registra exportação e fecha a sessão — tudo em uma única operação de escrita."""
    try:
        exp     = estado.get("export", {})
        sources = estado.get("sources", [])
        cripto  = estado.get("colunas_cripto", [])
        params  = _ler_params()  # lê fora do lock principal

        fmt_map  = {"1": "CSV", "2": "SQL", "3": "CSV+SQL"}
        fmt      = fmt_map.get(str(opcao_export), opcao_export)
        n_fontes = len(sources)
        total_ativs = sum(len(s.get("activities_selected", [])) for s in sources)

        # Cálculo de soft money
        horas = valor = ""
        try:
            bench = float(params.get("benchmark_horas_por_fonte", "") or "")
            custo = float(params.get("custo_hora_reais", "") or "")
            horas = round(bench * n_fontes, 2)
            valor = round(horas * custo, 2)
        except (ValueError, TypeError):
            pass

        with _lock:
            abas = _ler_todas()

            # Linha de exportação
            nova_exp = pd.DataFrame([{
                "session_id":            session_id,
                "timestamp":             _agora(),
                "total_eventos":         exp.get("row_count", ""),
                "unique_cases":          exp.get("unique_cases", ""),
                "unique_activities":     exp.get("unique_activities", ""),
                "num_fontes_integradas": n_fontes,
                "formato_export":        fmt,
                "criptografou":          "sim" if cripto else "não",
                "colunas_cripto":        "; ".join(cripto) if cripto else "",
                "horas_economizadas":    horas,
                "valor_gerado_reais":    valor,
            }])
            abas["exports"] = pd.concat([abas["exports"], nova_exp], ignore_index=True)

            # Atualiza sessão
            mask = abas["sessions"]["session_id"] == session_id
            if mask.any():
                inicio_str = abas["sessions"].loc[mask, "data_inicio"].values[0]
                try:
                    inicio  = datetime.strptime(inicio_str, "%Y-%m-%d %H:%M:%S")
                    duracao = round((datetime.now() - inicio).total_seconds() / 60, 1)
                except Exception:
                    duracao = ""
                abas["sessions"].loc[mask, "data_fim"]                       = _agora()
                abas["sessions"].loc[mask, "duracao_minutos"]                = str(duracao)
                abas["sessions"].loc[mask, "ultima_etapa_concluida"]         = "step6"
                abas["sessions"].loc[mask, "concluiu_export"]                = "sim"
                abas["sessions"].loc[mask, "total_fontes"]                   = str(n_fontes)
                abas["sessions"].loc[mask, "total_atividades_selecionadas"]  = str(total_ativs)

            _salvar_todas(abas)

    except Exception:
        pass


def log_session_fim(session_id: str, estado: dict):
    """Fecha sessão sem exportação (ex: clicou em Nova Sessão antes de exportar)."""
    try:
        sources     = estado.get("sources", [])
        total_ativs = sum(len(s.get("activities_selected", [])) for s in sources)
        concluiu    = "sim" if estado.get("export", {}).get("csv_path") or \
                               estado.get("export", {}).get("sql_path") else "não"
        with _lock:
            abas = _ler_todas()
            mask = abas["sessions"]["session_id"] == session_id
            if not mask.any():
                return
            inicio_str = abas["sessions"].loc[mask, "data_inicio"].values[0]
            try:
                inicio  = datetime.strptime(inicio_str, "%Y-%m-%d %H:%M:%S")
                duracao = round((datetime.now() - inicio).total_seconds() / 60, 1)
            except Exception:
                duracao = ""
            abas["sessions"].loc[mask, "data_fim"]                       = _agora()
            abas["sessions"].loc[mask, "duracao_minutos"]                = str(duracao)
            abas["sessions"].loc[mask, "concluiu_export"]                = concluiu
            abas["sessions"].loc[mask, "total_fontes"]                   = str(len(sources))
            abas["sessions"].loc[mask, "total_atividades_selecionadas"]  = str(total_ativs)
            _salvar_todas(abas)
    except Exception:
        pass
