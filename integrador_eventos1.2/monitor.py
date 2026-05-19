"""
monitor.py — Monitoração do Event Log Builder.

Grava dados de uso em monitoring.xlsx (uma aba por categoria).
Chamado pelo app.py — sem impacto no front-end.

Abas do Excel:
  sessions  — uma linha por sessão concluída
  files     — uma linha por arquivo processado
  exports   — uma linha por Event Log exportado
  errors    — uma linha por erro registrado
  params    — parâmetros de benchmark (preencher manualmente)
"""

from pathlib import Path
from datetime import datetime
from threading import Lock
import pandas as pd

# ── Configuração ──────────────────────────────────────────
MONITOR_DIR  = Path("monitoring")
MONITOR_FILE = MONITOR_DIR / "monitoring.xlsx"
_lock        = Lock()   # evita escrita simultânea

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


# ── Leitura / escrita do Excel ────────────────────────────
def _ler_aba(aba: str) -> pd.DataFrame:
    try:
        return pd.read_excel(MONITOR_FILE, sheet_name=aba, dtype=str).fillna("")
    except Exception:
        return pd.DataFrame(columns=CABECALHOS[aba])


def _salvar_todas(abas: dict[str, pd.DataFrame]):
    """Reescreve o Excel completo preservando todas as abas."""
    with pd.ExcelWriter(MONITOR_FILE, engine="openpyxl") as writer:
        for aba, df in abas.items():
            df.to_excel(writer, sheet_name=aba, index=False)


def _append_linha(aba: str, linha: dict):
    """Adiciona uma linha na aba indicada e salva o arquivo."""
    try:
        with _lock:
            abas = {a: _ler_aba(a) for a in CABECALHOS}
            nova = pd.DataFrame([linha])
            abas[aba] = pd.concat([abas[aba], nova], ignore_index=True)
            _salvar_todas(abas)
    except Exception:
        pass  # monitoração nunca deve quebrar o app


def _ler_params() -> dict:
    """Retorna os parâmetros de benchmark da aba params."""
    try:
        df = _ler_aba("params")
        if df.empty:
            return {}
        return df.iloc[0].to_dict()
    except Exception:
        return {}


def _agora() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# ── Eventos chamados pelo app.py ──────────────────────────

def log_session_inicio(session_id: str):
    """Registra o início de uma sessão."""
    _append_linha("sessions", {
        "session_id":                 session_id,
        "data_inicio":                _agora(),
        "data_fim":                   "",
        "duracao_minutos":            "",
        "ultima_etapa_concluida":     "step1",
        "concluiu_export":            "não",
        "total_fontes":               0,
        "total_atividades_selecionadas": 0,
    })


def log_etapa(session_id: str, etapa: str):
    """Atualiza a última etapa concluída da sessão."""
    try:
        with _lock:
            abas = {a: _ler_aba(a) for a in CABECALHOS}
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
        _append_linha("files", {
            "session_id":  session_id,
            "timestamp":   _agora(),
            "source_name": source_name,
            "file_name":   p.name,
            "file_format": fmt,
            "file_size_kb": size_kb,
            "rows":        rows,
            "cols":        cols,
        })
    except Exception:
        pass


def log_error(session_id: str, step: str, source_name: str,
              mensagem: str, tipo_erro: str = "geral"):
    """Registra um erro."""
    _append_linha("errors", {
        "session_id":  session_id,
        "timestamp":   _agora(),
        "step":        step,
        "source_name": source_name,
        "tipo_erro":   tipo_erro,
        "mensagem":    mensagem,
    })


def log_export(session_id: str, estado: dict, opcao_export: str):
    """Registra uma exportação e atualiza a sessão com dados finais."""
    try:
        exp     = estado.get("export", {})
        sources = estado.get("sources", [])
        cripto  = estado.get("colunas_cripto", [])
        params  = _ler_params()

        fmt_map  = {"1": "CSV", "2": "SQL", "3": "CSV+SQL"}
        fmt      = fmt_map.get(str(opcao_export), opcao_export)
        n_fontes = len(sources)

        # Cálculo de soft money (só se params preenchidos)
        horas = valor = ""
        try:
            bench = float(params.get("benchmark_horas_por_fonte", "") or "")
            custo = float(params.get("custo_hora_reais", "") or "")
            horas = round(bench * n_fontes, 2)
            valor = round(horas * custo, 2)
        except (ValueError, TypeError):
            pass

        _append_linha("exports", {
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
        })

        # Atualiza a linha da sessão com dados finais
        total_ativs = sum(
            len(s.get("activities_selected", [])) for s in sources
        )
        try:
            with _lock:
                abas = {a: _ler_aba(a) for a in CABECALHOS}
                mask = abas["sessions"]["session_id"] == session_id
                if mask.any():
                    inicio_str = abas["sessions"].loc[mask, "data_inicio"].values[0]
                    try:
                        inicio   = datetime.strptime(inicio_str, "%Y-%m-%d %H:%M:%S")
                        duracao  = round((datetime.now() - inicio).total_seconds() / 60, 1)
                    except Exception:
                        duracao = ""
                    abas["sessions"].loc[mask, "data_fim"]                    = _agora()
                    abas["sessions"].loc[mask, "duracao_minutos"]             = str(duracao)
                    abas["sessions"].loc[mask, "ultima_etapa_concluida"]      = "step6"
                    abas["sessions"].loc[mask, "concluiu_export"]             = "sim"
                    abas["sessions"].loc[mask, "total_fontes"]                = str(n_fontes)
                    abas["sessions"].loc[mask, "total_atividades_selecionadas"] = str(total_ativs)
                    _salvar_todas(abas)
        except Exception:
            pass

    except Exception:
        pass


def log_session_fim(session_id: str, estado: dict):
    """Atualiza a sessão com dados finais sem exportação (ex: nova sessão)."""
    try:
        sources     = estado.get("sources", [])
        total_ativs = sum(len(s.get("activities_selected", [])) for s in sources)
        concluiu    = "sim" if estado.get("export", {}).get("csv_path") or \
                               estado.get("export", {}).get("sql_path") else "não"
        with _lock:
            abas = {a: _ler_aba(a) for a in CABECALHOS}
            mask = abas["sessions"]["session_id"] == session_id
            if not mask.any():
                return
            inicio_str = abas["sessions"].loc[mask, "data_inicio"].values[0]
            try:
                inicio  = datetime.strptime(inicio_str, "%Y-%m-%d %H:%M:%S")
                duracao = round((datetime.now() - inicio).total_seconds() / 60, 1)
            except Exception:
                duracao = ""
            abas["sessions"].loc[mask, "data_fim"]                      = _agora()
            abas["sessions"].loc[mask, "duracao_minutos"]               = str(duracao)
            abas["sessions"].loc[mask, "concluiu_export"]               = concluiu
            abas["sessions"].loc[mask, "total_fontes"]                  = str(len(sources))
            abas["sessions"].loc[mask, "total_atividades_selecionadas"] = str(total_ativs)
            _salvar_todas(abas)
    except Exception:
        pass
