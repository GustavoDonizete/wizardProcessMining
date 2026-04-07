"""
app.py — Servidor Flask completo do Event Log Builder (versão simples).

Execute com:
    pip install -r requirements.txt
    python app.py
Acesse: http://localhost:5000
"""

import os
import io
import uuid
from pathlib import Path

from flask import (Flask, render_template, request, redirect,
                   url_for, session, jsonify, send_file, flash)
from werkzeug.utils import secure_filename

import core

# ── Configuração ──────────────────────────────────────────
app = Flask(__name__)
app.secret_key = "elb-simple-2024"
app.config["MAX_CONTENT_LENGTH"] = 100 * 1024 * 1024  # 100 MB

app.jinja_env.filters["basename"] = os.path.basename

UPLOAD_DIR     = Path("uploads")
EXTENSOES_OK   = {".csv", ".xlsx", ".xls", ".parquet", ".json"}

# Estado em memória: { sid: { sources, pending, export } }
ESTADOS: dict[str, dict] = {}


# ── Helpers ───────────────────────────────────────────────
def _sid() -> str:
    if "sid" not in session:
        session["sid"] = str(uuid.uuid4())
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
    estado = _estado()
    return render_template("step1.html", step=1, fontes=estado["sources"])

@app.route("/step1", methods=["POST"])
def step1_post():
    estado  = _estado()
    arquivo = request.files.get("arquivo")
    nome    = request.form.get("source_name", "").strip()

    if not arquivo or not arquivo.filename:
        flash("Selecione um arquivo.", "warning"); return redirect(url_for("step1"))
    if not nome:
        flash("Informe o nome da fonte.", "warning"); return redirect(url_for("step1"))
    if Path(arquivo.filename).suffix.lower() not in EXTENSOES_OK:
        flash("Formato não suportado. Use CSV, Excel, Parquet ou JSON.", "danger")
        return redirect(url_for("step1"))

    caminho = str(_upload_dir() / secure_filename(arquivo.filename))
    arquivo.save(caminho)

    try:
        df      = core.carregar_dataframe(caminho)
        profile = core.profile_dataframe(df)
    except Exception as e:
        flash(f"Erro ao ler arquivo: {e}", "danger"); return redirect(url_for("step1"))

    estado["pending"] = {
        "source_name": nome,
        "file_path":   caminho,
        "colunas":     list(df.columns),
        "shape":       {"rows": len(df), "cols": len(df.columns)},
        "profile":     profile,
    }
    return redirect(url_for("step2"))


# ── PASSO 2: Colunas ──────────────────────────────────────
@app.route("/step2", methods=["GET"])
def step2():
    p = _estado().get("pending", {})
    if not p:
        flash("Inicie pelo upload.", "warning"); return redirect(url_for("step1"))
    return render_template("step2.html", step=2,
                           source_name=p["source_name"],
                           shape=p["shape"],
                           profile=p["profile"],
                           colunas=p["colunas"])

@app.route("/step2", methods=["POST"])
def step2_post():
    estado = _estado()
    p = estado.get("pending", {})

    case_id_col  = request.form.get("case_id_col", "").strip()
    ts_start_col = request.form.get("timestamp_start_col", "").strip()
    activity_col = request.form.get("activity_col", "").strip()
    ts_end_col   = request.form.get("timestamp_end_col", "").strip() or None

    if not all([case_id_col, ts_start_col, activity_col]):
        flash("Selecione Case_ID, Timestamp_Start e Activity.", "warning")
        return redirect(url_for("step2"))

    p.update({
        "case_id_col":           case_id_col,
        "timestamp_start_col":   ts_start_col,
        "timestamp_start_format": None,
        "timestamp_end_col":     ts_end_col,
        "timestamp_end_format":  None,
        "activity_col":          activity_col,
    })
    return redirect(url_for("step3"))


# ── PASSO 3: Atividades ───────────────────────────────────
@app.route("/step3", methods=["GET"])
def step3():
    p = _estado().get("pending", {})
    if not p.get("activity_col"):
        flash("Configure as colunas primeiro.", "warning"); return redirect(url_for("step2"))
    return render_template("step3.html", step=3,
                           source_name=p["source_name"],
                           activity_col=p["activity_col"])

@app.route("/step3", methods=["POST"])
def step3_post():
    estado     = _estado()
    p          = estado.get("pending", {})
    atividades = request.form.getlist("activities")

    if not atividades:
        flash("Selecione pelo menos uma atividade.", "warning")
        return redirect(url_for("step3"))

    estado["sources"].append({
        "source_name":           p["source_name"],
        "file_path":             p["file_path"],
        "case_id_col":           p["case_id_col"],
        "activity_col":          p["activity_col"],
        "activities_selected":   atividades,
        "timestamp_start_col":   p["timestamp_start_col"],
        "timestamp_start_format":p.get("timestamp_start_format"),
        "timestamp_end_col":     p.get("timestamp_end_col"),
        "timestamp_end_format":  p.get("timestamp_end_format"),
    })
    estado["pending"] = {}
    return redirect(url_for("step4"))


# ── PASSO 4: Resumo ───────────────────────────────────────
@app.route("/step4", methods=["GET"])
def step4():
    estado = _estado()
    if not estado["sources"]:
        flash("Nenhuma fonte configurada.", "warning"); return redirect(url_for("step1"))
    return render_template("step4.html", step=4, fontes=estado["sources"])

@app.route("/step4", methods=["POST"])
def step4_post():
    if request.form.get("acao") == "add":
        return redirect(url_for("step1"))
    return redirect(url_for("step5"))


# ── PASSO 5: Exportar ─────────────────────────────────────
@app.route("/step5", methods=["GET"])
def step5():
    return render_template("step5.html", step=5, export=_estado().get("export", {}))

@app.route("/step5", methods=["POST"])
def step5_post():
    estado  = _estado()
    opcao   = request.form.get("export_option", "1")

    try:
        event_log, erros = core.construir_event_log(estado["sources"])
        for e in erros:
            flash(f"Aviso na fonte '{e['source_name']}': {e['message']}", "warning")

        upload_dir = _upload_dir()
        csv_path = sql_path = None

        if opcao in ("1", "3"):
            csv_path = str(upload_dir / "event_log.csv")
            event_log.to_csv(csv_path, index=False, encoding="utf-8")

        if opcao in ("2", "3"):
            sql_path = str(upload_dir / "event_log_query.sql")
            Path(sql_path).write_text(core.exportar_sql_str(estado["sources"]), encoding="utf-8")

        # Salva parquet temporário para validação
        el_path = str(upload_dir / "event_log_temp.parquet")
        event_log.to_parquet(el_path, index=False)

        estado["export"] = {
            "csv_path":          csv_path,
            "sql_path":          sql_path,
            "el_temp_path":      el_path,
            "row_count":         len(event_log),
            "unique_cases":      int(event_log["Case_ID"].nunique()),
            "unique_activities": int(event_log["Activity"].nunique()),
        }
        flash(f"Event Log gerado com {len(event_log):,} eventos!", "success")
    except Exception as e:
        flash(f"Erro na exportação: {e}", "danger")

    return redirect(url_for("step5"))


# ── PASSO 6: Validação ────────────────────────────────────
@app.route("/step6")
def step6():
    estado   = _estado()
    el_path  = estado.get("export", {}).get("el_temp_path")

    if not el_path or not Path(el_path).exists():
        flash("Execute a exportação antes de validar.", "warning")
        return redirect(url_for("step5"))

    import pandas as pd
    event_log = pd.read_parquet(el_path)
    validacao = core.run_validation(event_log)

    return render_template("step6.html", step=6,
                           validacao=validacao,
                           export=estado["export"])


# ── API AJAX ──────────────────────────────────────────────
@app.route("/api/activities")
def api_activities():
    col = request.args.get("col", "").strip()
    if not col:
        return jsonify({"error": "Parâmetro 'col' ausente."}), 400
    p = _estado().get("pending", {})
    fp = p.get("file_path")
    if not fp or not Path(fp).exists():
        return jsonify({"error": "Arquivo não encontrado."}), 404
    try:
        df = core.carregar_dataframe(fp)
        return jsonify({"values": core.get_unique_values(df, col)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Downloads ─────────────────────────────────────────────
@app.route("/download/csv")
def download_csv():
    path = _estado().get("export", {}).get("csv_path")
    if not path or not Path(path).exists():
        flash("Arquivo CSV não encontrado.", "warning"); return redirect(url_for("step5"))
    return send_file(path, as_attachment=True, download_name="event_log.csv", mimetype="text/csv")

@app.route("/download/sql")
def download_sql():
    path = _estado().get("export", {}).get("sql_path")
    if not path or not Path(path).exists():
        flash("Arquivo SQL não encontrado.", "warning"); return redirect(url_for("step5"))
    return send_file(path, as_attachment=True, download_name="event_log_query.sql", mimetype="text/plain")


# ── Entry point ───────────────────────────────────────────
if __name__ == "__main__":
    UPLOAD_DIR.mkdir(exist_ok=True)
    app.run(debug=True, port=5001)
