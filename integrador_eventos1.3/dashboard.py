"""
dashboard.py — Dashboard de monitoração do Event Log Builder.

Execute com:
    python dashboard.py
Acesse: http://localhost:8081
"""

from pathlib import Path
from datetime import datetime

from flask import Flask, render_template_string, send_file, jsonify
import pandas as pd

app = Flask(__name__)

MONITOR_DIR = Path("monitoring")
ARQUIVOS = {
    "sessions": MONITOR_DIR / "sessions.csv",
    "files":    MONITOR_DIR / "files.csv",
    "errors":   MONITOR_DIR / "errors.csv",
    "exports":  MONITOR_DIR / "exports.csv",
}


# ── Helpers ───────────────────────────────────────────────
def _ler(chave: str) -> pd.DataFrame:
    p = ARQUIVOS[chave]
    if not p.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(p, dtype=str).fillna("")
    except Exception:
        return pd.DataFrame()


def _num(val):
    try: return int(float(str(val)))
    except: return 0


# ── Dashboard HTML ────────────────────────────────────────
TEMPLATE = """
<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>ELB Monitor</title>
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css"/>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.3/dist/chart.umd.min.js"></script>
  <style>
    :root{--orange:#f97316;--orange-dark:#ea580c;--orange-light:#fff7ed;--orange-border:#fed7aa;
      --gray-50:#f9fafb;--gray-100:#f3f4f6;--gray-200:#e5e7eb;--gray-500:#6b7280;--gray-900:#111827}
    body{background:var(--gray-50);font-family:'Segoe UI',system-ui,sans-serif}
    .topbar{background:#fff;border-bottom:1px solid var(--gray-200);height:56px;padding:0 1.5rem;
      display:flex;align-items:center;justify-content:space-between;
      position:sticky;top:0;z-index:100;box-shadow:0 1px 3px rgba(0,0,0,.05)}
    .brand{display:flex;align-items:center;gap:.6rem;font-weight:700;color:var(--gray-900);text-decoration:none}
    .brand-icon{width:32px;height:32px;border-radius:8px;
      background:linear-gradient(135deg,var(--orange),var(--orange-dark));
      display:flex;align-items:center;justify-content:center;color:#fff}
    .wrap{max-width:1100px;margin:0 auto;padding:2rem 1.5rem}
    .stat-card{background:#fff;border:1px solid var(--gray-200);border-radius:12px;
      padding:1.25rem 1.5rem;box-shadow:0 1px 4px rgba(0,0,0,.04)}
    .stat-num{font-size:2rem;font-weight:800;color:var(--orange);line-height:1}
    .stat-lbl{font-size:.75rem;color:var(--gray-500);text-transform:uppercase;letter-spacing:.06em;margin-top:.3rem}
    .stat-sub{font-size:.8rem;color:var(--gray-500);margin-top:.2rem}
    .section-title{font-size:1rem;font-weight:700;color:var(--gray-900);margin-bottom:1rem;
      padding-bottom:.5rem;border-bottom:2px solid var(--gray-200)}
    .elb-card{background:#fff;border:1px solid var(--gray-200);border-radius:12px;
      box-shadow:0 1px 4px rgba(0,0,0,.04);overflow:hidden}
    .elb-card-header{background:var(--gray-50);border-bottom:1px solid var(--gray-200);
      padding:.75rem 1rem;font-size:.88rem;font-weight:600;color:var(--gray-900);
      display:flex;align-items:center;justify-content:space-between}
    .badge-orange{background:var(--orange-light);color:var(--orange-dark);border-radius:20px;
      padding:.2rem .65rem;font-size:.75rem;font-weight:600}
    .badge-red{background:#fef2f2;color:#991b1b;border-radius:20px;padding:.2rem .65rem;font-size:.75rem;font-weight:600}
    .badge-green{background:#ecfdf5;color:#065f46;border-radius:20px;padding:.2rem .65rem;font-size:.75rem;font-weight:600}
    .tbl{width:100%;font-size:.83rem;border-collapse:collapse}
    .tbl th{font-size:.72rem;font-weight:700;text-transform:uppercase;letter-spacing:.05em;
      color:var(--orange);padding:.6rem 1rem;background:var(--gray-50);border-bottom:1px solid var(--gray-200)}
    .tbl td{padding:.65rem 1rem;border-bottom:1px solid var(--gray-100);color:var(--gray-900)}
    .tbl tr:last-child td{border-bottom:none}
    .tbl tr:hover td{background:var(--gray-50)}
    .btn-dl{display:inline-flex;align-items:center;gap:.35rem;font-size:.82rem;font-weight:600;
      padding:.35rem .85rem;border-radius:8px;border:1.5px solid var(--orange);
      color:var(--orange);text-decoration:none;transition:.15s}
    .btn-dl:hover{background:var(--orange);color:#fff}
    canvas{max-height:220px}
  </style>
</head>
<body>

<div class="topbar">
  <a href="/" class="brand">
    <div class="brand-icon">
      <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5">
        <polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/>
      </svg>
    </div>
    ELB Monitor
  </a>
  <div style="display:flex;gap:.75rem;align-items:center">
    <span style="font-size:.8rem;color:var(--gray-500)">Atualizado: {{ agora }}</span>
    <a href="/download/all" class="btn-dl">↓ Exportar tudo</a>
  </div>
</div>

<div class="wrap">

  <!-- KPIs -->
  <div class="row g-3 mb-4">
    <div class="col-6 col-md-3">
      <div class="stat-card">
        <div class="stat-num">{{ kpi.total_sessoes }}</div>
        <div class="stat-lbl">Sessões</div>
        <div class="stat-sub">{{ kpi.sessoes_completas }} concluídas</div>
      </div>
    </div>
    <div class="col-6 col-md-3">
      <div class="stat-card">
        <div class="stat-num">{{ kpi.total_arquivos }}</div>
        <div class="stat-lbl">Arquivos processados</div>
        <div class="stat-sub">{{ kpi.formatos }}</div>
      </div>
    </div>
    <div class="col-6 col-md-3">
      <div class="stat-card">
        <div class="stat-num">{{ kpi.total_eventos }}</div>
        <div class="stat-lbl">Eventos gerados</div>
        <div class="stat-sub">{{ kpi.total_exports }} exportações</div>
      </div>
    </div>
    <div class="col-6 col-md-3">
      <div class="stat-card">
        <div class="stat-num" style="color:{% if kpi.total_erros > 0 %}#ef4444{% else %}var(--orange){% endif %}">
          {{ kpi.total_erros }}
        </div>
        <div class="stat-lbl">Erros registrados</div>
        <div class="stat-sub">{{ kpi.erros_hoje }} hoje</div>
      </div>
    </div>
  </div>

  <!-- Gráficos -->
  <div class="row g-3 mb-4">
    <div class="col-md-6">
      <div class="elb-card">
        <div class="elb-card-header">Sessões por dia</div>
        <div style="padding:1rem">
          <canvas id="chartSessoes"></canvas>
        </div>
      </div>
    </div>
    <div class="col-md-6">
      <div class="elb-card">
        <div class="elb-card-header">Formatos de arquivo</div>
        <div style="padding:1rem">
          <canvas id="chartFormatos"></canvas>
        </div>
      </div>
    </div>
  </div>

  <!-- Últimas exportações -->
  <div class="mb-4">
    <div class="section-title">Últimas exportações</div>
    <div class="elb-card">
      {% if exports %}
      <table class="tbl">
        <thead>
          <tr>
            <th>Data/hora</th>
            <th>Eventos</th>
            <th>Casos</th>
            <th>Atividades</th>
            <th>Fontes</th>
            <th>Formato</th>
            <th>Criptografia</th>
          </tr>
        </thead>
        <tbody>
          {% for r in exports %}
          <tr>
            <td>{{ r.timestamp }}</td>
            <td>{{ r.total_eventos }}</td>
            <td>{{ r.unique_cases }}</td>
            <td>{{ r.unique_activities }}</td>
            <td>{{ r.num_fontes }}</td>
            <td><span class="badge-orange">{{ r.formato_export }}</span></td>
            <td>
              {% if r.criptografou == "sim" %}
                <span class="badge-green">✓ {{ r.colunas_cripto }}</span>
              {% else %}
                <span style="color:var(--gray-500);font-size:.8rem">—</span>
              {% endif %}
            </td>
          </tr>
          {% endfor %}
        </tbody>
      </table>
      {% else %}
      <div style="padding:2rem;text-align:center;color:var(--gray-500);font-size:.88rem">
        Nenhuma exportação registrada ainda.
      </div>
      {% endif %}
    </div>
  </div>

  <!-- Arquivos processados -->
  <div class="mb-4">
    <div class="section-title">Arquivos processados</div>
    <div class="elb-card">
      {% if files %}
      <table class="tbl">
        <thead>
          <tr><th>Data/hora</th><th>Fonte</th><th>Arquivo</th><th>Formato</th><th>Tamanho</th><th>Linhas</th><th>Colunas</th></tr>
        </thead>
        <tbody>
          {% for r in files %}
          <tr>
            <td>{{ r.timestamp }}</td>
            <td>{{ r.source_name }}</td>
            <td style="font-family:monospace;font-size:.8rem">{{ r.file_name }}</td>
            <td><span class="badge-orange">{{ r.file_format }}</span></td>
            <td>{{ r.file_size_kb }} KB</td>
            <td>{{ r.rows }}</td>
            <td>{{ r.cols }}</td>
          </tr>
          {% endfor %}
        </tbody>
      </table>
      {% else %}
      <div style="padding:2rem;text-align:center;color:var(--gray-500);font-size:.88rem">Nenhum arquivo registrado ainda.</div>
      {% endif %}
    </div>
  </div>

  <!-- Erros -->
  <div class="mb-4">
    <div class="section-title">Erros e avisos</div>
    <div class="elb-card">
      {% if errors %}
      <table class="tbl">
        <thead>
          <tr><th>Data/hora</th><th>Etapa</th><th>Fonte</th><th>Mensagem</th></tr>
        </thead>
        <tbody>
          {% for r in errors %}
          <tr>
            <td>{{ r.timestamp }}</td>
            <td><span class="badge-red">{{ r.step }}</span></td>
            <td>{{ r.source_name }}</td>
            <td style="font-size:.8rem;color:#991b1b">{{ r.mensagem }}</td>
          </tr>
          {% endfor %}
        </tbody>
      </table>
      {% else %}
      <div style="padding:2rem;text-align:center;color:var(--gray-500);font-size:.88rem">Nenhum erro registrado. ✓</div>
      {% endif %}
    </div>
  </div>

  <!-- Sessões recentes -->
  <div class="mb-4">
    <div class="section-title">Sessões recentes</div>
    <div class="elb-card">
      {% if sessions %}
      <table class="tbl">
        <thead>
          <tr><th>Início</th><th>Fim</th><th>Duração (min)</th><th>Fontes</th><th>Atividades</th><th>Concluiu</th></tr>
        </thead>
        <tbody>
          {% for r in sessions %}
          <tr>
            <td>{{ r.inicio }}</td>
            <td>{{ r.fim }}</td>
            <td>{{ r.duracao_min }}</td>
            <td>{{ r.total_fontes }}</td>
            <td>{{ r.total_atividades }}</td>
            <td>
              {% if r.concluiu_export == "sim" %}
                <span class="badge-green">✓ sim</span>
              {% else %}
                <span style="color:var(--gray-500);font-size:.8rem">não</span>
              {% endif %}
            </td>
          </tr>
          {% endfor %}
        </tbody>
      </table>
      {% else %}
      <div style="padding:2rem;text-align:center;color:var(--gray-500);font-size:.88rem">Nenhuma sessão registrada ainda.</div>
      {% endif %}
    </div>
  </div>

</div><!-- /wrap -->

<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js"></script>
<script>
const orange = '#f97316';
const orangeLight = '#fed7aa';

// Sessões por dia
const sessDias  = {{ chart_sessoes_labels | tojson }};
const sessVals  = {{ chart_sessoes_values | tojson }};
new Chart(document.getElementById('chartSessoes'), {
  type: 'bar',
  data: {
    labels: sessDias,
    datasets: [{ label: 'Sessões', data: sessVals,
      backgroundColor: orangeLight, borderColor: orange, borderWidth: 2, borderRadius: 6 }]
  },
  options: { plugins: { legend: { display: false } }, scales: { y: { beginAtZero: true, ticks: { stepSize: 1 } } } }
});

// Formatos
const fmtLabels = {{ chart_formatos_labels | tojson }};
const fmtVals   = {{ chart_formatos_values | tojson }};
new Chart(document.getElementById('chartFormatos'), {
  type: 'doughnut',
  data: {
    labels: fmtLabels,
    datasets: [{ data: fmtVals,
      backgroundColor: ['#f97316','#fb923c','#fdba74','#fed7aa','#fff7ed'],
      borderColor: '#fff', borderWidth: 2 }]
  },
  options: { plugins: { legend: { position: 'bottom' } }, cutout: '60%' }
});
</script>
</body>
</html>
"""


@app.route("/")
def dashboard():
    df_sess    = _ler("sessions")
    df_files   = _ler("files")
    df_errors  = _ler("errors")
    df_exports = _ler("exports")

    hoje = datetime.now().strftime("%Y-%m-%d")

    # KPIs
    kpi = {
        "total_sessoes":    len(df_sess),
        "sessoes_completas": len(df_sess[df_sess.get("concluiu_export","") == "sim"]) if not df_sess.empty and "concluiu_export" in df_sess else 0,
        "total_arquivos":   len(df_files),
        "formatos":         ", ".join(df_files["file_format"].unique().tolist()) if not df_files.empty and "file_format" in df_files else "—",
        "total_eventos":    f"{sum(_num(v) for v in df_exports['total_eventos'].tolist()):,}" if not df_exports.empty and "total_eventos" in df_exports else "0",
        "total_exports":    len(df_exports),
        "total_erros":      len(df_errors),
        "erros_hoje":       len(df_errors[df_errors["timestamp"].str.startswith(hoje)]) if not df_errors.empty and "timestamp" in df_errors else 0,
    }

    # Gráfico sessões por dia
    if not df_sess.empty and "inicio" in df_sess:
        df_sess["dia"] = df_sess["inicio"].str[:10]
        dias = df_sess.groupby("dia").size().tail(14)
        chart_sessoes_labels = dias.index.tolist()
        chart_sessoes_values = dias.values.tolist()
    else:
        chart_sessoes_labels = []
        chart_sessoes_values = []

    # Gráfico formatos
    if not df_files.empty and "file_format" in df_files:
        fmts = df_files["file_format"].value_counts()
        chart_formatos_labels = fmts.index.tolist()
        chart_formatos_values = fmts.values.tolist()
    else:
        chart_formatos_labels = []
        chart_formatos_values = []

    # Tabelas — últimos 50 de cada
    def to_records(df):
        if df.empty: return []
        return df.tail(50).iloc[::-1].to_dict("records")

    return render_template_string(
        TEMPLATE,
        agora=datetime.now().strftime("%d/%m/%Y %H:%M"),
        kpi=kpi,
        sessions=to_records(df_sess),
        files=to_records(df_files),
        errors=to_records(df_errors),
        exports=to_records(df_exports),
        chart_sessoes_labels=chart_sessoes_labels,
        chart_sessoes_values=chart_sessoes_values,
        chart_formatos_labels=chart_formatos_labels,
        chart_formatos_values=chart_formatos_values,
    )


@app.route("/download/all")
def download_all():
    """Gera um Excel com uma aba por CSV de monitoração."""
    import io
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        for chave, caminho in ARQUIVOS.items():
            df = _ler(chave)
            if df.empty:
                df = pd.DataFrame(columns=[chave])
            df.to_excel(writer, sheet_name=chave, index=False)
    buf.seek(0)
    return send_file(
        buf,
        as_attachment=True,
        download_name="elb_monitoring.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )


if __name__ == "__main__":
    if not MONITOR_DIR.exists():
        print("AVISO: pasta 'monitoring/' não encontrada. Rode o app principal primeiro.")
    app.run(debug=True, port=8081)
