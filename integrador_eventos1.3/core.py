"""
core.py — Motor do Event Log Builder.
Suporta arquivos de até 1.5 GB via:
  - Conversão automática para Parquet após upload (cache em disco)
  - Leitura em chunks para operações sobre arquivos grandes
"""

import io
import hashlib
from typing import Optional
import pandas as pd
from pathlib import Path

# Tamanho de chunk para leitura em partes (linhas)
CHUNK_SIZE = 100_000

# ══════════════════════════════════════════════════════════
#  DETECÇÃO DE SEPARADOR / ENCODING (CSV)
# ══════════════════════════════════════════════════════════

def _detectar_csv_params(caminho: str) -> dict:
    """
    Detecta encoding e separador lendo apenas as primeiras linhas do CSV.
    Retorna kwargs prontos para pd.read_csv.
    """
    encodings  = ["utf-8", "utf-8-sig", "latin-1", "cp1252"]
    separators = [None, ";", ",", "\t", "|"]

    for enc in encodings:
        for sep in separators:
            try:
                kwargs = dict(encoding=enc, on_bad_lines="skip", low_memory=False, nrows=50)
                if sep is None:
                    kwargs["sep"]    = None
                    kwargs["engine"] = "python"
                else:
                    kwargs["sep"] = sep
                df = pd.read_csv(caminho, **kwargs)
                if len(df.columns) > 1:
                    # Remove colunas Unnamed quase vazias
                    unnamed = [c for c in df.columns if str(c).startswith("Unnamed:")]
                    df = df.drop(columns=[c for c in unnamed if df[c].isna().mean() > 0.9])
                    if len(df.columns) > 1:
                        # Retorna params sem nrows para uso real
                        result = dict(encoding=enc, on_bad_lines="skip", low_memory=False)
                        if sep is None:
                            result["sep"]    = None
                            result["engine"] = "python"
                        else:
                            result["sep"] = sep
                        return result
            except Exception:
                continue

    raise ValueError(
        "Não foi possível detectar o separador do CSV. "
        "Verifique se o arquivo está correto (separadores aceitos: vírgula, ponto-e-vírgula, tab ou pipe)."
    )


# ══════════════════════════════════════════════════════════
#  CACHE PARQUET
# ══════════════════════════════════════════════════════════

def _caminho_parquet(caminho_original: str) -> str:
    """Retorna o caminho do Parquet cacheado correspondente ao arquivo original."""
    p = Path(caminho_original)
    return str(p.parent / (p.stem + "_cache.parquet"))


def converter_para_parquet(caminho: str) -> str:
    """
    Converte qualquer formato suportado para Parquet e salva ao lado do original.
    Retorna o caminho do Parquet gerado.
    Pula a conversão se o Parquet já existir e for mais recente que o original.
    """
    parquet_path = _caminho_parquet(caminho)
    orig = Path(caminho)
    cache = Path(parquet_path)

    # Usa cache se já existe e está atualizado
    if cache.exists() and cache.stat().st_mtime >= orig.stat().st_mtime:
        return parquet_path

    ext = orig.suffix.lower()

    if ext == ".parquet":
        return caminho  # já é parquet, sem conversão

    if ext == ".csv":
        params = _detectar_csv_params(caminho)
        # Lê e converte em chunks para não explodir a memória
        chunks = []
        for chunk in pd.read_csv(caminho, chunksize=CHUNK_SIZE, **params):
            # Remove colunas Unnamed quase vazias no primeiro chunk
            if not chunks:
                unnamed = [c for c in chunk.columns if str(c).startswith("Unnamed:")]
                drop = [c for c in unnamed if c in chunk.columns]
                chunk = chunk.drop(columns=drop)
            chunks.append(chunk)
        df = pd.concat(chunks, ignore_index=True)

    elif ext in (".xlsx", ".xls"):
        df = pd.read_excel(caminho)

    elif ext == ".json":
        df = pd.read_json(caminho)

    else:
        raise ValueError(f"Formato '{ext}' não suportado.")

    df.to_parquet(parquet_path, index=False, engine="pyarrow")
    return parquet_path


# ══════════════════════════════════════════════════════════
#  CARREGAMENTO PÚBLICO
# ══════════════════════════════════════════════════════════

def carregar_dataframe(caminho: str) -> pd.DataFrame:
    """
    Carrega o arquivo completo em DataFrame.
    Sempre passa pelo cache Parquet primeiro.
    Use apenas para profiling inicial (step2) — para transformação use _transformar_fonte.
    """
    parquet = converter_para_parquet(caminho)
    return pd.read_parquet(parquet, engine="pyarrow")


def parsear_timestamps(serie: pd.Series, fmt: Optional[str] = None) -> pd.Series:
    """Converte coluna para datetime."""
    try:
        return pd.to_datetime(serie, format=fmt, errors="coerce")
    except Exception:
        return pd.to_datetime(serie, infer_datetime_format=True, errors="coerce")


def get_unique_values(df: pd.DataFrame, col: str) -> list:
    """
    Retorna valores únicos ordenados de uma coluna.
    Aceita tanto um DataFrame já carregado quanto um caminho de arquivo.
    Para arquivos grandes, lê direto do Parquet cacheado sem carregar tudo.
    """
    if isinstance(df, str):
        # Recebeu caminho de arquivo
        parquet = converter_para_parquet(df)
        valores = set()
        for chunk in pd.read_parquet(parquet, engine="pyarrow", columns=[col]).pipe(
            lambda x: [x.iloc[i:i+CHUNK_SIZE] for i in range(0, len(x), CHUNK_SIZE)]
        ):
            valores.update(chunk[col].dropna().astype(str).unique().tolist())
        return sorted(valores)

    return sorted(df[col].dropna().astype(str).unique().tolist())


def get_unique_values_from_path(file_path: str, col: str) -> list:
    """Lê valores únicos de uma coluna diretamente do Parquet, em chunks."""
    parquet = converter_para_parquet(file_path)
    df_col  = pd.read_parquet(parquet, engine="pyarrow", columns=[col])
    return sorted(df_col[col].dropna().astype(str).unique().tolist())


# ══════════════════════════════════════════════════════════
#  EXPORTAÇÃO
# ══════════════════════════════════════════════════════════

TIPOS_REFERENCIA = [
    "Case_ID", "Atividade", "Timestamp_Inicio", "Timestamp_Fim",
    "Texto", "Número", "Data", "Outro",
]

TIPOS_EXTRAS      = {"Texto", "Número", "Data", "Outro"}
TIPOS_OBRIGATORIOS = {"Case_ID", "Atividade", "Timestamp_Inicio"}


def _transformar_fonte(source: dict) -> pd.DataFrame:
    """
    Lê o Parquet cacheado da fonte em chunks, filtra atividades
    e retorna DataFrame no formato Event Log.
    """
    mapa = source["mapeamento"]

    col_case     = next((c for c, v in mapa.items() if v["tipo"] == "Case_ID"), None)
    col_ativ     = next((c for c, v in mapa.items() if v["tipo"] == "Atividade"), None)
    col_ts_start = next((c for c, v in mapa.items() if v["tipo"] == "Timestamp_Inicio"), None)
    col_ts_end   = next((c for c, v in mapa.items() if v["tipo"] == "Timestamp_Fim"), None)

    if not all([col_case, col_ativ, col_ts_start]):
        raise ValueError(
            f"Fonte '{source['source_name']}': mapeamento incompleto "
            "(Case_ID, Atividade e Timestamp_Inicio são obrigatórios)."
        )

    # Colunas extras que devem ir para a saída
    cols_extras = [
        col for col, info in mapa.items()
        if info["tipo"] in TIPOS_EXTRAS and info.get("incluir", False)
    ]

    # Colunas a ler do Parquet (só o necessário)
    cols_ler = list({col_case, col_ativ, col_ts_start} |
                    ({col_ts_end} if col_ts_end else set()) |
                    set(cols_extras))

    atividades = set(source["activities_selected"])
    parquet    = converter_para_parquet(source["file_path"])

    partes = []
    df_full = pd.read_parquet(parquet, engine="pyarrow", columns=cols_ler)

    # Processa em chunks
    for i in range(0, len(df_full), CHUNK_SIZE):
        chunk = df_full.iloc[i : i + CHUNK_SIZE].copy()
        chunk = chunk[chunk[col_ativ].astype(str).isin(atividades)]
        if chunk.empty:
            continue

        ts_start = parsear_timestamps(chunk[col_ts_start])
        ts_end   = parsear_timestamps(chunk[col_ts_end]) if col_ts_end else pd.NaT

        parte = pd.DataFrame({
            "Case_ID":         chunk[col_case].astype(str).values,
            "Activity":        chunk[col_ativ].astype(str).values,
            "Timestamp_Start": ts_start.values,
            "Timestamp_End":   ts_end.values if col_ts_end else pd.NaT,
            "Source":          source["source_name"],
        })

        for col in cols_extras:
            parte[col] = chunk[col].values

        partes.append(parte)

    if not partes:
        return pd.DataFrame(columns=["Case_ID", "Activity", "Timestamp_Start",
                                     "Timestamp_End", "Source"] + cols_extras)

    return pd.concat(partes, ignore_index=True)


def construir_event_log(sources: list) -> tuple[pd.DataFrame, list]:
    """Empilha todas as fontes em chunks. Retorna (DataFrame, erros)."""
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
        mapa        = s["mapeamento"]
        tabela      = Path(s["file_path"]).stem
        col_case    = next((c for c, v in mapa.items() if v["tipo"] == "Case_ID"), "")
        col_ativ    = next((c for c, v in mapa.items() if v["tipo"] == "Atividade"), "")
        col_ts_s    = next((c for c, v in mapa.items() if v["tipo"] == "Timestamp_Inicio"), "")
        col_ts_e    = next((c for c, v in mapa.items() if v["tipo"] == "Timestamp_Fim"), None)
        ativs       = ", ".join(f"'{a}'" for a in s["activities_selected"])
        ts_end      = f"CAST({col_ts_e} AS TIMESTAMP)" if col_ts_e else "NULL"
        source_name = s["source_name"]

        extras = ""
        for col, info in mapa.items():
            if info["tipo"] in TIPOS_EXTRAS and info.get("incluir", False):
                extras += f"    {col},\n"
        extras_sql = (",\n" + extras.rstrip(",\n")) if extras else ""

        blocos.append(
            f"-- Fonte: {source_name}\n"
            f"SELECT\n"
            f"    CAST({col_case} AS VARCHAR)    AS Case_ID,\n"
            f"    CAST({col_ativ} AS VARCHAR)    AS Activity,\n"
            f"    CAST({col_ts_s} AS TIMESTAMP)  AS Timestamp_Start,\n"
            f"    {ts_end}                       AS Timestamp_End,\n"
            f"    '{source_name}' AS Source"
            f"{extras_sql}\n"
            f"FROM {tabela}\n"
            f"WHERE CAST({col_ativ} AS VARCHAR) IN ({ativs})"
        )
    return (
        "-- Event Log Query (ANSI SQL)\n\n"
        + "\nUNION ALL\n\n".join(blocos)
        + "\nORDER BY Timestamp_Start;\n"
    )


# ══════════════════════════════════════════════════════════
#  CRIPTOGRAFIA MD5
# ══════════════════════════════════════════════════════════

def md5_valor(valor: str) -> str:
    return hashlib.md5(str(valor).encode("utf-8")).hexdigest()


def criptografar_event_log(
    event_log: pd.DataFrame,
    colunas: list[str],
) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    """Aplica MD5 nas colunas indicadas. Mesmo valor → mesmo hash."""
    el = event_log.copy()
    de_para: dict[str, pd.DataFrame] = {}

    for col in colunas:
        if col not in el.columns:
            continue
        valores_unicos = el[col].dropna().astype(str).unique()
        mapa_hash = {v: md5_valor(v) for v in valores_unicos}
        de_para[col] = pd.DataFrame(
            list(mapa_hash.items()), columns=["valor_original", "hash_md5"]
        ).sort_values("valor_original").reset_index(drop=True)
        el[col] = el[col].astype(str).map(lambda x: mapa_hash.get(x, x))

    return el, de_para


def exportar_depara_bytes(de_para: dict[str, pd.DataFrame]) -> bytes:
    """Gera Excel com uma aba por coluna criptografada."""
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        for col, df in de_para.items():
            df.to_excel(writer, sheet_name=col[:31], index=False)
    return buf.getvalue()
