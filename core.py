"""
core.py — Motor completo do Event Log Builder.
Contém: profiling, exportação e validação. Sem dependências de UI.
"""

import io
from typing import Optional
import pandas as pd
import numpy as np
from pathlib import Path
from dateutil import parser as dateutil_parser


# ══════════════════════════════════════════════════════════
#  UTILS
# ══════════════════════════════════════════════════════════

def _carregar_csv(caminho: str) -> pd.DataFrame:
    """
    Carrega CSV detectando separador e encoding automaticamente.
    Tenta encodings: UTF-8, UTF-8-BOM, Latin-1, CP1252.
    Tenta separadores: auto (python engine), ; , , \\t, |
    Retorna o primeiro DataFrame com mais de 1 coluna.
    """
    encodings  = ["utf-8", "utf-8-sig", "latin-1", "cp1252"]
    separators = [None, ";", ",", "\t", "|"]  # None = auto-detect

    for enc in encodings:
        for sep in separators:
            try:
                kwargs = dict(encoding=enc, on_bad_lines="skip", low_memory=False)
                if sep is None:
                    kwargs["sep"]    = None
                    kwargs["engine"] = "python"
                else:
                    kwargs["sep"] = sep
                df = pd.read_csv(caminho, **kwargs)
                if len(df.columns) > 1:
                    return df
            except Exception:
                continue

    raise ValueError(
        "Não foi possível detectar o separador do CSV. "
        "Verifique se o arquivo está correto (separadores aceitos: vírgula, ponto-e-vírgula, tab ou pipe)."
    )


def carregar_dataframe(caminho: str) -> pd.DataFrame:
    """Carrega CSV, Excel, Parquet ou JSON automaticamente."""
    ext = Path(caminho).suffix.lower()
    if ext == ".csv":
        return _carregar_csv(caminho)
    loaders = {
        ".xlsx":    lambda p: pd.read_excel(p),
        ".xls":     lambda p: pd.read_excel(p),
        ".parquet": lambda p: pd.read_parquet(p),
        ".json":    lambda p: pd.read_json(p),
    }
    if ext not in loaders:
        raise ValueError(f"Formato '{ext}' não suportado. Use CSV, Excel, Parquet ou JSON.")
    return loaders[ext](caminho)


def parsear_timestamps(serie: pd.Series, fmt: Optional[str] = None) -> pd.Series:
    """Converte coluna para datetime. Tenta fmt informado, depois inferência."""
    try:
        return pd.to_datetime(serie, format=fmt, errors="coerce")
    except Exception:
        return pd.to_datetime(serie, infer_datetime_format=True, errors="coerce")


# ══════════════════════════════════════════════════════════
#  PROFILER
# ══════════════════════════════════════════════════════════

_NOMES_CASE = {"id", "cod", "num", "key", "contrato", "processo", "pedido", "ticket", "protocolo"}
_NOMES_TS   = {"data", "date", "dt", "time", "timestamp", "inicio", "fim", "start", "end", "criacao", "abertura"}
_NOMES_ACT  = {"status", "etapa", "fase", "atividade", "activity", "evento", "event", "tipo", "type", "situacao", "estado"}


def _score_case_id(df: pd.DataFrame, col: str) -> tuple[float, str]:
    serie = df[col]
    n = len(serie)
    card = serie.nunique() / n
    nulos = serie.isna().mean()
    nome_ok = any(p in col.lower() for p in _NOMES_CASE)
    score = round(0.5 * card + 0.3 * (1 - nulos) + 0.2 * nome_ok, 3)
    just = (f"'{col}' tem {card*100:.0f}% de valores únicos e {nulos*100:.0f}% de nulos"
            + (" — nome sugere identificador." if nome_ok else "."))
    return score, just


def _score_timestamp(df: pd.DataFrame, col: str) -> tuple[float, str]:
    serie = df[col]
    nome_ok = any(p in col.lower() for p in _NOMES_TS)
    ja_datetime = pd.api.types.is_datetime64_any_dtype(serie)
    if not ja_datetime:
        try:
            amostra = serie.dropna().astype(str).head(100)
            pd.to_datetime(amostra, errors="raise")
            parseavel = True
        except Exception:
            parseavel = False
    else:
        parseavel = True
    if not parseavel and not ja_datetime:
        return 0.0, ""
    score = round(0.6 * (1.0 if ja_datetime or parseavel else 0) + 0.4 * nome_ok, 3)
    just = (f"'{col}' " + ("já é datetime" if ja_datetime else "é parseável como data")
            + (" — nome confirma." if nome_ok else "."))
    return score, just


def _score_activity(df: pd.DataFrame, col: str) -> tuple[float, str]:
    if not pd.api.types.is_object_dtype(df[col]):
        return 0.0, ""
    serie = df[col]
    n_uniq = serie.nunique()
    nulos = serie.isna().mean()
    card_ok = 2 <= n_uniq <= 200
    nome_ok = any(p in col.lower() for p in _NOMES_ACT)
    if not card_ok:
        return 0.0, ""
    score = round(0.5 * card_ok + 0.3 * (1 - nulos) + 0.2 * nome_ok, 3)
    just = (f"'{col}' tem {n_uniq} valores únicos — baixa cardinalidade"
            + (" e nome sugere etapa do processo." if nome_ok else "."))
    return score, just


def profile_dataframe(df: pd.DataFrame) -> dict:
    """
    Analisa colunas e retorna candidatas ranqueadas por papel.
    Retorna: { "case_id": [...], "timestamp": [...], "activity": [...] }
    Cada item: { "col": str, "score": float, "justificativa": str }
    """
    result = {"case_id": [], "timestamp": [], "activity": []}

    for col in df.columns:
        s_cid, j_cid = _score_case_id(df, col)
        if s_cid > 0.4:
            result["case_id"].append({"col": col, "score": s_cid, "justificativa": j_cid})

        s_ts, j_ts = _score_timestamp(df, col)
        if s_ts > 0.3:
            result["timestamp"].append({"col": col, "score": s_ts, "justificativa": j_ts})

        s_act, j_act = _score_activity(df, col)
        if s_act > 0.3:
            result["activity"].append({"col": col, "score": s_act, "justificativa": j_act})

    for papel in result:
        result[papel] = sorted(result[papel], key=lambda x: x["score"], reverse=True)[:5]

    return result


def get_unique_values(df: pd.DataFrame, col: str) -> list:
    """Retorna valores únicos ordenados de uma coluna (para seleção de atividades)."""
    return sorted(df[col].dropna().astype(str).unique().tolist())


# ══════════════════════════════════════════════════════════
#  EXPORTAÇÃO
# ══════════════════════════════════════════════════════════

def _transformar_fonte(source: dict) -> pd.DataFrame:
    """Carrega e transforma uma fonte no formato Event Log."""
    df = carregar_dataframe(source["file_path"])
    df = df[df[source["activity_col"]].astype(str).isin(source["activities_selected"])].copy()

    ts_start = parsear_timestamps(df[source["timestamp_start_col"]], source.get("timestamp_start_format"))
    ts_end = pd.NaT
    if source.get("timestamp_end_col"):
        ts_end = parsear_timestamps(df[source["timestamp_end_col"]], source.get("timestamp_end_format"))

    return pd.DataFrame({
        "Case_ID":         df[source["case_id_col"]].astype(str),
        "Activity":        df[source["activity_col"]].astype(str),
        "Timestamp_Start": ts_start,
        "Timestamp_End":   ts_end if source.get("timestamp_end_col") else pd.NaT,
        "Source":          source["source_name"],
    })


def construir_event_log(sources: list) -> tuple[pd.DataFrame, list]:
    """Empilha todas as fontes. Retorna (DataFrame, lista_de_erros)."""
    partes, erros = [], []
    for s in sources:
        try:
            partes.append(_transformar_fonte(s))
        except Exception as e:
            erros.append({"source_name": s["source_name"], "message": str(e)})
    if not partes:
        raise ValueError("Nenhuma fonte processada com sucesso.")
    df = pd.concat(partes, ignore_index=True)
    return df.sort_values("Timestamp_Start", na_position="last").reset_index(drop=True), erros


def exportar_csv_bytes(event_log: pd.DataFrame) -> bytes:
    """Retorna Event Log como bytes UTF-8 para download."""
    buf = io.StringIO()
    event_log.to_csv(buf, index=False, encoding="utf-8")
    return buf.getvalue().encode("utf-8")


def exportar_sql_str(sources: list) -> str:
    """Gera query ANSI SQL com UNION ALL entre as fontes."""
    blocos = []
    for s in sources:
        tabela = Path(s["file_path"]).stem
        ativs  = ", ".join(f"'{a}'" for a in s["activities_selected"])
        ts_end = (f"CAST({s['timestamp_end_col']} AS TIMESTAMP)"
                  if s.get("timestamp_end_col") else "NULL")
        blocos.append(
            f"-- Fonte: {s['source_name']}\n"
            f"SELECT\n"
            f"    CAST({s['case_id_col']} AS VARCHAR)            AS Case_ID,\n"
            f"    CAST({s['activity_col']} AS VARCHAR)           AS Activity,\n"
            f"    CAST({s['timestamp_start_col']} AS TIMESTAMP)  AS Timestamp_Start,\n"
            f"    {ts_end}                                       AS Timestamp_End,\n"
            f"    '{s['source_name']}' AS Source\n"
            f"FROM {tabela}\n"
            f"WHERE CAST({s['activity_col']} AS VARCHAR) IN ({ativs})"
        )
    return "-- Event Log Query (ANSI SQL)\n\n" + "\nUNION ALL\n\n".join(blocos) + "\nORDER BY Timestamp_Start;\n"


# ══════════════════════════════════════════════════════════
#  VALIDAÇÃO CRUZADA
# ══════════════════════════════════════════════════════════

def _ok(msg):    return {"status": "ok",    "msg": msg}
def _aviso(msg): return {"status": "aviso", "msg": msg}
def _erro(msg):  return {"status": "erro",  "msg": msg}


def _checar_ordem_temporal(df: pd.DataFrame) -> list:
    fontes = df["Source"].unique().tolist()
    if len(fontes) < 2:
        return [_ok("Apenas uma fonte — ordem temporal não aplicável.")]
    medias = {f: df.loc[df["Source"] == f, "Timestamp_Start"].dropna().mean() for f in fontes}
    medias = {f: v for f, v in medias.items() if pd.notna(v)}
    ordem = sorted(medias, key=lambda f: medias[f])
    resultados = [_ok(f"Ordem cronológica sugerida: {' → '.join(ordem)}")]
    for i in range(len(ordem) - 1):
        fa, fb = ordem[i], ordem[i + 1]
        max_a = df.loc[df["Source"] == fa, "Timestamp_Start"].max()
        min_b = df.loc[df["Source"] == fb, "Timestamp_Start"].min()
        if pd.notna(max_a) and pd.notna(min_b) and max_a > min_b:
            resultados.append(_aviso(f"Sobreposição entre '{fa}' e '{fb}'."))
    return resultados


def _checar_case_id(df: pd.DataFrame) -> list:
    fontes = df["Source"].unique().tolist()
    if len(fontes) < 2:
        return [_ok("Apenas uma fonte — compatibilidade não aplicável.")]
    ids = {f: set(df.loc[df["Source"] == f, "Case_ID"].unique()) for f in fontes}
    resultados = []
    for i, fa in enumerate(fontes):
        for fb in fontes[i + 1:]:
            inter = ids[fa] & ids[fb]
            pct = len(inter) / len(ids[fa]) * 100 if ids[fa] else 0
            msg = f"'{fa}' ↔ '{fb}': {pct:.0f}% de Case_IDs em comum."
            if pct < 30:   resultados.append(_aviso(msg + " Baixa sobreposição."))
            elif pct == 100: resultados.append(_aviso(msg + " Sobreposição total — fontes podem ser redundantes."))
            else:          resultados.append(_ok(msg))
    return resultados


def _checar_timestamps(df: pd.DataFrame) -> list:
    col = df["Timestamp_Start"].dropna()
    if col.empty:
        return [_erro("Nenhum Timestamp_Start válido.")]
    resultados = []
    nulos = df["Timestamp_Start"].isna().sum()
    if nulos:
        fn = _erro if nulos / len(df) > 0.1 else _aviso
        resultados.append(fn(f"{nulos:,} timestamp(s) nulo(s) — verifique o formato original."))
    else:
        resultados.append(_ok("Todos os timestamps parseados com sucesso."))
    if (col.dt.microsecond != 0).any():
        resultados.append(_aviso("Sub-segundos detectados — granularidade inconsistente."))
    else:
        resultados.append(_ok("Granularidade consistente."))
    return resultados


def _checar_atividades(df: pd.DataFrame) -> list:
    ativs = sorted(df["Activity"].dropna().unique().tolist())
    preview = ", ".join(str(a) for a in ativs[:8])
    sufixo  = f" (+{len(ativs)-8})" if len(ativs) > 8 else ""
    resultados = [_ok(f"{len(ativs)} atividade(s) únicas: {preview}{sufixo}")]
    dups = df.duplicated(subset=["Case_ID", "Activity", "Timestamp_Start"], keep=False).sum()
    if dups:
        resultados.append(_erro(f"{dups:,} evento(s) duplicado(s) detectado(s)."))
    else:
        resultados.append(_ok("Nenhum evento duplicado."))
    return resultados


def run_validation(event_log: pd.DataFrame) -> dict:
    """Executa as 4 checagens e retorna dict estruturado para o template."""
    sections = [
        {"title": "1. Ordem Temporal",            "results": _checar_ordem_temporal(event_log)},
        {"title": "2. Compatibilidade de Case_ID", "results": _checar_case_id(event_log)},
        {"title": "3. Alinhamento de Timestamps",  "results": _checar_timestamps(event_log)},
        {"title": "4. Coerência de Atividades",    "results": _checar_atividades(event_log)},
    ]
    summary = {"ok": 0, "aviso": 0, "erro": 0}
    for sec in sections:
        for r in sec["results"]:
            summary[r["status"]] += 1
    return {"sections": sections, "summary": summary}
