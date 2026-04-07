# Event Log Builder — Documentação

Ferramenta web para construção de Event Logs padronizados para Process Mining.

---

## Como Rodar

```bash
pip install -r requirements.txt
python app.py
```

Acesse: **http://localhost:5000**

---

## Estrutura de Arquivos

```
event_log_simple/
├── app.py          # Servidor Flask: todas as rotas e lógica web
├── core.py         # Motor Python: profiling, exportação, validação
├── requirements.txt
├── DOCUMENTACAO.md
├── templates/      # 7 HTMLs (base + 6 passos do wizard)
│   ├── base.html
│   ├── step1.html  # Upload do arquivo
│   ├── step2.html  # Seleção de colunas
│   ├── step3.html  # Seleção de atividades
│   ├── step4.html  # Resumo das fontes
│   ├── step5.html  # Exportação
│   └── step6.html  # Validação cruzada
├── static/
│   ├── style.css   # Tema branco/laranja
│   └── script.js
└── uploads/        # Arquivos enviados (criado automaticamente)
```

---

## O Que o Sistema Faz

O Event Log Builder transforma bases de dados brutas (CSV, Excel, etc.) em um **Event Log** padronizado para ferramentas de Process Mining como ProM, Celonis, Disco e PM4Py.

### Fluxo em 6 Passos

| Passo | Ação |
|-------|------|
| 1 | Upload do arquivo + nome da fonte |
| 2 | O sistema sugere automaticamente as colunas de Case_ID, Timestamp e Activity |
| 3 | Usuário seleciona quais atividades incluir |
| 4 | Resumo — opção de adicionar mais uma fonte |
| 5 | Exporta CSV e/ou SQL |
| 6 | Validação cruzada do Event Log gerado |

### Formato de Saída

| Coluna | Descrição |
|--------|-----------|
| `Case_ID` | Identificador único do caso/processo |
| `Activity` | Nome da atividade/etapa |
| `Timestamp_Start` | Data/hora de início |
| `Timestamp_End` | Data/hora de fim (opcional) |
| `Source` | Nome da fonte de origem |

---

## Arquitetura

### app.py — Servidor Flask

- **Sessões em memória**: estado de cada usuário em `ESTADOS` (dict Python), sem banco de dados
- **Rotas**: uma por passo (`/step1` a `/step6`) + `/api/activities` (AJAX) + `/download/csv` e `/download/sql`
- **Uploads**: salvos em `uploads/<session_id>/`

### core.py — Motor Python

Dividido em 4 seções:

**1. Utils**
- `carregar_dataframe(caminho)` — detecta extensão e carrega com pandas
- `parsear_timestamps(serie, fmt)` — converte coluna para datetime

**2. Profiler** — Inferência automática de colunas
- `profile_dataframe(df)` → candidatas ranqueadas por papel (Case_ID, Timestamp, Activity)
- Cada candidata recebe um score 0–1 baseado em cardinalidade, nulos e padrão no nome da coluna

**3. Exportação**
- `construir_event_log(sources)` → DataFrame empilhado com todas as fontes
- `exportar_csv_bytes(df)` → bytes para download
- `exportar_sql_str(sources)` → query ANSI SQL com `UNION ALL`

**4. Validação Cruzada** (`run_validation`)
- Ordem temporal entre fontes
- Compatibilidade de Case_IDs (% de overlap)
- Alinhamento de timestamps (nulos, fuso, granularidade)
- Coerência de atividades (duplicatas, eventos sem data)

---

## Dependências

| Pacote | Uso |
|--------|-----|
| `flask` | Servidor web |
| `pandas` | Manipulação de dados |
| `numpy` | Cálculos numéricos |
| `python-dateutil` | Parsing flexível de datas |
| `openpyxl` | Leitura de arquivos Excel |
| `pyarrow` | Leitura/escrita de Parquet |
| `werkzeug` | Upload seguro de arquivos |
