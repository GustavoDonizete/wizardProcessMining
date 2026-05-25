"""
app.py — Servidor Flask do Event Log Builder.

Execute com:
    pip install -r requirements.txt
    python app.py
Acesse: http://localhost:8080
"""

import os
import uuid
from pathlib import Path

from flask import (Flask, render_template, request, redirect,
                   url_for, session, jsonify, send_file, flash)
from werkzeug.utils import secure_filename

import core
import monitor

# ── Configuração ──────────────────────────────────────────
app = Flask(__name__)
app.secret_key = "elb-simple-2024"
app.config["MAX_CONTENT_LENGTH"] = 1536 * 1024 * 1024  # 1.5 GB
app.config["JSON_AS_ASCII"] = False
app.jinja_env.filters["basename"] = os.path.basename

UPLOAD_DIR   = Path("uploads")
EXTENSOES_OK = {".csv", ".xlsx", ".xls", ".parquet", ".json"}
ESTADOS: dict[str, dict] = {}

monitor.inicializar()


# ── Helpers ───────────────────────────────────────────────
def _sid() -> str:
    if "sid" not in session:
        sid = str(uuid.uuid4())
        session["sid"] = sid
        monitor.log_session_inicio(sid)
    return session["sid"]

def _estado() -> dict:
    sid = _sid()
    if sid not in ESTADOS:
        ESTADOS[sid] = {"sources": [], "pending": {}, "export": {}}
    return ESTADOS[sid]

def _upload_dir() -> Path:
    d = UPLOAD_DIR / _sid()
    d.mkdir(parents=True, exist_ok=True)
    return d


# ── Rota inicial ──────────────────────────────────────────
@app.route("/")
def index():
    return redirect(url_for("step1"))


# ── PASSO 1: Upload ───────────────────────────────────────
@app.route("/step1", methods=["GET"])
def step1():
    return render_template("step1.html", step=1, fontes=_estado()["sources"])

@app.route("/step1", methods=["POST"])
def step1_post():
    estado  = _estado()
    arquivo = request.files.get("arquivo")
    nome    = request.form.get("source_name", "").strip()

    if not arquivo or not arquivo.filename:
        flash("Selecione um arquivo.", "warning")
        return redirect(url_for("step1"))
    if not nome:
        flash("Informe o nome da fonte.", "warning")
        return redirect(url_for("step1"))
    if Path(arquivo.filename).suffix.lower() not in EXTENSOES_OK:
        flash("Formato não suportado. Use CSV, Excel, Parquet ou JSON.", "danger")
        return redirect(url_for("step1"))

    caminho = str(_upload_dir() / secure_filename(arquivo.filename))
    arquivo.save(caminho)

    try:
        core.converter_para_parquet(caminho)
        df = core.carregar_dataframe(caminho)
        monitor.log_file(_sid(), nome, caminho, len(df), len(df.columns))
    except Exception as e:
        monitor.log_error(_sid(), "step1", nome, str(e))
        flash(f"Erro ao ler arquivo: {e}", "danger")
        return redirect(url_for("step1"))

    estado["pending"] = {
        "source_name": nome,
        "file_path":   caminho,
        "colunas":     list(df.columns),
        "shape":       {"rows": len(df), "cols": len(df.columns)},
        "mapeamento":  {},
    }
    return redirect(url_for("step2"))


# ── PASSO 2: Mapeamento de colunas ────────────────────────
@app.route("/step2", methods=["GET"])
def step2():
    p = _estado().get("pending", {})
    if not p:
        flash("Inicie pelo upload.", "warning")
        return redirect(url_for("step1"))
    return render_template("step2.html", step=2,
                           source_name=p["source_name"],
                           shape=p["shape"],
                           colunas=p["colunas"],
                           tipos=core.TIPOS_REFERENCIA,
                           tipos_extras=list(core.TIPOS_EXTRAS))

@app.route("/step2", methods=["POST"])
def step2_post():
    estado = _estado()
    p = estado.get("pending", {})
    mapeamento = {}
    for col in p["colunas"]:
        tipo    = request.form.get(f"tipo_{col}", "Outro").strip()
        incluir = request.form.get(f"incluir_{col}") == "on"
        mapeamento[col] = {"tipo": tipo, "incluir": incluir}

    tipos_mapeados = {v["tipo"] for v in mapeamento.values()}
    faltando = core.TIPOS_OBRIGATORIOS - tipos_mapeados
    if faltando:
        monitor.log_error(_sid(), "step2", p.get("source_name",""), f"Mapeamento incompleto: {faltando}")
        flash(f"Mapeamento incompleto. Faltam: {', '.join(faltando)}.", "warning")
        return redirect(url_for("step2"))

    p["mapeamento"] = mapeamento
    monitor.log_etapa(_sid(), "step2")
    return redirect(url_for("step3"))


# ── PASSO 3: Seleção de atividades ────────────────────────
@app.route("/step3", methods=["GET"])
def step3():
    p = _estado().get("pending", {})
    if not p.get("mapeamento"):
        flash("Faça o mapeamento de colunas primeiro.", "warning")
        return redirect(url_for("step2"))
    col_ativ = next((c for c, v in p["mapeamento"].items() if v["tipo"] == "Atividade"), None)
    if not col_ativ:
        flash("Nenhuma coluna mapeada como Atividade.", "warning")
        return redirect(url_for("step2"))
    return render_template("step3.html", step=3,
                           source_name=p["source_name"],
                           activity_col=col_ativ)

@app.route("/step3", methods=["POST"])
def step3_post():
    estado     = _estado()
    p          = estado.get("pending", {})
    atividades = request.form.getlist("activities")
    if not atividades:
        flash("Selecione pelo menos uma atividade.", "warning")
        return redirect(url_for("step3"))
    estado["sources"].append({
        "source_name":         p["source_name"],
        "file_path":           p["file_path"],
        "mapeamento":          p["mapeamento"],
        "activities_selected": atividades,
    })
    estado["pending"] = {}
    monitor.log_etapa(_sid(), "step3")
    return redirect(url_for("step4"))


# ── PASSO 4: Resumo ───────────────────────────────────────
@app.route("/step4", methods=["GET"])
def step4():
    estado = _estado()
    if not estado["sources"]:
        flash("Nenhuma fonte configurada.", "warning")
        return redirect(url_for("step1"))
    return render_template("step4.html", step=4, fontes=estado["sources"])

@app.route("/step4", methods=["POST"])
def step4_post():
    if request.form.get("acao") == "add":
        return redirect(url_for("step1"))
    monitor.log_etapa(_sid(), "step4")
    return redirect(url_for("step5"))


# ── PASSO 5: Criptografia ─────────────────────────────────
@app.route("/step5", methods=["GET"])
def step5():
    estado = _estado()
    if not estado["sources"]:
        flash("Nenhuma fonte configurada.", "warning")
        return redirect(url_for("step1"))
    colunas_event_log = ["Case_ID", "Activity", "Timestamp_Start", "Timestamp_End", "Source"]
    for s in estado["sources"]:
        for col, info in s["mapeamento"].items():
            if info["tipo"] in core.TIPOS_EXTRAS and info.get("incluir", False):
                if col not in colunas_event_log:
                    colunas_event_log.append(col)
    return render_template("step5.html", step=5, colunas_event_log=colunas_event_log)

@app.route("/step5", methods=["POST"])
def step5_post():
    estado = _estado()
    estado["colunas_cripto"] = request.form.getlist("colunas_cripto")
    monitor.log_etapa(_sid(), "step5")
    return redirect(url_for("step6"))


# ── PASSO 6: Exportação ───────────────────────────────────
@app.route("/step6", methods=["GET"])
def step6():
    return render_template("step6.html", step=6, export=_estado().get("export", {}))

@app.route("/step6", methods=["POST"])
def step6_post():
    estado = _estado()
    opcao  = request.form.get("export_option", "1")
    try:
        event_log, erros = core.construir_event_log(estado["sources"])
        for e in erros:
            monitor.log_error(_sid(), "step6", e["source_name"], e["message"])
            flash(f"Aviso na fonte '{e['source_name']}': {e['message']}", "warning")

        upload_dir     = _upload_dir()
        colunas_cripto = estado.get("colunas_cripto", [])
        de_para        = {}

        if colunas_cripto:
            event_log, de_para = core.criptografar_event_log(event_log, colunas_cripto)
            depara_path = str(upload_dir / "de_para.xlsx")
            Path(depara_path).write_bytes(core.exportar_depara_bytes(de_para))
            estado["export"]["depara_path"] = depara_path

        csv_path = sql_path = None
        if opcao in ("1", "3"):
            csv_path = str(upload_dir / "event_log.csv")
            event_log.to_csv(csv_path, index=False, encoding="utf-8")
        if opcao in ("2", "3"):
            sql_path = str(upload_dir / "event_log_query.sql")
            Path(sql_path).write_text(core.exportar_sql_str(estado["sources"]), encoding="utf-8")

        estado["export"].update({
            "csv_path":          csv_path,
            "sql_path":          sql_path,
            "row_count":         len(event_log),
            "unique_cases":      int(event_log["Case_ID"].nunique()),
            "unique_activities": int(event_log["Activity"].nunique()),
            "colunas_cripto":    colunas_cripto,
        })

        monitor.log_export(_sid(), estado, opcao)
        monitor.log_session_fim(_sid(), estado)
        flash(f"Event Log gerado com {len(event_log):,} eventos!", "success")

    except Exception as e:
        monitor.log_error(_sid(), "step6", "", str(e))
        flash(f"Erro na exportação: {e}", "danger")

    return redirect(url_for("step6"))


# ── API AJAX ──────────────────────────────────────────────
@app.route("/api/activities")
def api_activities():
    col = request.args.get("col", "").strip()
    if not col:
        return jsonify({"error": "Parâmetro 'col' ausente."}), 400
    p  = _estado().get("pending", {})
    fp = p.get("file_path")
    if not fp or not Path(fp).exists():
        return jsonify({"error": "Arquivo não encontrado."}), 404
    try:
        return jsonify({"values": core.get_unique_values_from_path(fp, col)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Downloads ─────────────────────────────────────────────
@app.route("/download/csv")
def download_csv():
    path = _estado().get("export", {}).get("csv_path")
    if not path or not Path(path).exists():
        flash("CSV não encontrado.", "warning"); return redirect(url_for("step6"))
    return send_file(path, as_attachment=True, download_name="event_log.csv", mimetype="text/csv")

@app.route("/download/sql")
def download_sql():
    path = _estado().get("export", {}).get("sql_path")
    if not path or not Path(path).exists():
        flash("SQL não encontrado.", "warning"); return redirect(url_for("step6"))
    return send_file(path, as_attachment=True, download_name="event_log_query.sql", mimetype="text/plain")

@app.route("/download/depara")
def download_depara():
    path = _estado().get("export", {}).get("depara_path")
    if not path or not Path(path).exists():
        flash("De-para não encontrado.", "warning"); return redirect(url_for("step6"))
    return send_file(path, as_attachment=True,
                     download_name="de_para_criptografia.xlsx",
                     mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


# ── Nova sessão ───────────────────────────────────────────
@app.route("/nova-sessao")
def nova_sessao():
    if "sid" in session:
        monitor.log_session_fim(session["sid"], _estado())
    session.clear()
    return redirect(url_for("step1"))


# ── Entry point ───────────────────────────────────────────
if __name__ == "__main__":
    UPLOAD_DIR.mkdir(exist_ok=True)
    app.run(debug=True, port=8080)
