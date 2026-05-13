"""
core.py — Motor do Event Log Builder.
Contém: carregamento, exportação e criptografia MD5. Sem dependências de UI.
"""

import io
import re
import hashlib
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
    separators = [None, ";", ",", "\t", "|"]

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
                    unnamed = [c for c in df.columns if str(c).startswith("Unnamed:")]
                    df = df.drop(columns=[c for c in unnamed if df[c].isna().mean() > 0.9])
                    for col in df.select_dtypes(include="object").columns:
                        df[col] = df[col].apply(
                            lambda x: x.encode("utf-8", errors="replace").decode("utf-8") if isinstance(x, str) else x
                        )
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


def get_unique_values(df: pd.DataFrame, col: str) -> list:
    """Retorna valores únicos ordenados de uma coluna (para seleção de atividades)."""
    return sorted(df[col].dropna().astype(str).unique().tolist())


# ══════════════════════════════════════════════════════════
#  EXPORTAÇÃO
# ══════════════════════════════════════════════════════════

# Tipos de referência disponíveis para mapeamento de colunas
TIPOS_REFERENCIA = [
    "Case_ID",
    "Atividade",
    "Timestamp_Inicio",
    "Timestamp_Fim",
    "Texto",
    "Número",
    "Data",
    "Outro",
]

# Tipos que podem ser incluídos como colunas extras na saída
TIPOS_EXTRAS = {"Texto", "Número", "Data", "Outro"}

# Tipos obrigatórios (mínimo necessário para construir o event log)
TIPOS_OBRIGATORIOS = {"Case_ID", "Atividade", "Timestamp_Inicio"}


def _transformar_fonte(source: dict) -> pd.DataFrame:
    """
    Carrega e transforma uma fonte no formato Event Log.
    source deve conter:
      - file_path
      - source_name
      - mapeamento: { nome_coluna: { "tipo": str, "incluir": bool } }
      - activities_selected: list[str]
    """
    df = carregar_dataframe(source["file_path"])
    mapa = source["mapeamento"]

    # Localiza colunas por tipo
    col_case     = next((c for c, v in mapa.items() if v["tipo"] == "Case_ID"), None)
    col_ativ     = next((c for c, v in mapa.items() if v["tipo"] == "Atividade"), None)
    col_ts_start = next((c for c, v in mapa.items() if v["tipo"] == "Timestamp_Inicio"), None)
    col_ts_end   = next((c for c, v in mapa.items() if v["tipo"] == "Timestamp_Fim"), None)

    if not all([col_case, col_ativ, col_ts_start]):
        raise ValueError(f"Fonte '{source['source_name']}': mapeamento incompleto (Case_ID, Atividade e Timestamp_Inicio são obrigatórios).")

    # Filtra atividades selecionadas
    df = df[df[col_ativ].astype(str).isin(source["activities_selected"])].copy()

    ts_start = parsear_timestamps(df[col_ts_start])
    ts_end   = parsear_timestamps(df[col_ts_end]) if col_ts_end else pd.NaT

    resultado = pd.DataFrame({
        "Case_ID":         df[col_case].astype(str),
        "Activity":        df[col_ativ].astype(str),
        "Timestamp_Start": ts_start,
        "Timestamp_End":   ts_end if col_ts_end else pd.NaT,
        "Source":          source["source_name"],
    })

    # Colunas extras marcadas para incluir
    for col, info in mapa.items():
        if info["tipo"] in TIPOS_EXTRAS and info.get("incluir", False):
            resultado[col] = df[col].values

    return resultado


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


def exportar_sql_str(sources: list) -> str:
    """Gera query ANSI SQL com UNION ALL entre as fontes."""
    blocos = []
    for s in sources:
        mapa     = s["mapeamento"]
        tabela   = Path(s["file_path"]).stem
        col_case = next((c for c, v in mapa.items() if v["tipo"] == "Case_ID"), "")
        col_ativ = next((c for c, v in mapa.items() if v["tipo"] == "Atividade"), "")
        col_ts_s = next((c for c, v in mapa.items() if v["tipo"] == "Timestamp_Inicio"), "")
        col_ts_e = next((c for c, v in mapa.items() if v["tipo"] == "Timestamp_Fim"), None)
        ativs    = ", ".join(f"'{a}'" for a in s["activities_selected"])
        ts_end   = f"CAST({col_ts_e} AS TIMESTAMP)" if col_ts_e else "NULL"

        # Colunas extras
        extras = ""
        for col, info in mapa.items():
            if info["tipo"] in TIPOS_EXTRAS and info.get("incluir", False):
                extras += f"    {col},\n"

        blocos.append(
            f"-- Fonte: {s['source_name']}\n"
            f"SELECT\n"
            f"    CAST({col_case} AS VARCHAR)           AS Case_ID,\n"
            f"    CAST({col_ativ} AS VARCHAR)           AS Activity,\n"
            f"    CAST({col_ts_s} AS TIMESTAMP)         AS Timestamp_Start,\n"
            f"    {ts_end}                              AS Timestamp_End,\n"
            f"    '{s['source_name']}' AS Source"
            + (f",\n{extras.rstrip(',\\n')}" if extras else "") + "\n"
            f"FROM {tabela}\n"
            f"WHERE CAST({col_ativ} AS VARCHAR) IN ({ativs})"
        )
    return "-- Event Log Query (ANSI SQL)\n\n" + "\nUNION ALL\n\n".join(blocos) + "\nORDER BY Timestamp_Start;\n"


# ══════════════════════════════════════════════════════════
#  CRIPTOGRAFIA MD5
# ══════════════════════════════════════════════════════════

def md5_valor(valor: str) -> str:
    """Aplica MD5 sobre o valor textual. Mesmo valor → mesmo hash."""
    return hashlib.md5(str(valor).encode("utf-8")).hexdigest()


def criptografar_event_log(
    event_log: pd.DataFrame,
    colunas: list[str],
) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    """
    Aplica MD5 nas colunas indicadas do Event Log.
    Retorna:
      - event_log com os valores substituídos por hash
      - dict { nome_coluna: DataFrame com colunas [valor_original, hash_md5] }
    """
    el = event_log.copy()
    de_para: dict[str, pd.DataFrame] = {}

    for col in colunas:
        if col not in el.columns:
            continue
        valores_unicos = el[col].dropna().astype(str).unique()
        mapa = {v: md5_valor(v) for v in valores_unicos}
        de_para[col] = pd.DataFrame(
            list(mapa.items()), columns=["valor_original", "hash_md5"]
        ).sort_values("valor_original").reset_index(drop=True)
        el[col] = el[col].astype(str).map(lambda x: mapa.get(x, x))

    return el, de_para


def exportar_depara_bytes(de_para: dict[str, pd.DataFrame]) -> bytes:
    """
    Gera um arquivo Excel com uma aba por coluna criptografada.
    Retorna bytes prontos para download.
    """
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        for col, df in de_para.items():
            aba = col[:31]  # Excel limita nomes de aba a 31 chars
            df.to_excel(writer, sheet_name=aba, index=False)
    return buf.getvalue()
