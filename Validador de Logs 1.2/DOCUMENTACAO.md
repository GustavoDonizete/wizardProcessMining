# Event Log Builder — Documentação

Ferramenta web para construção de Event Logs padronizados para Process Mining.

---

## Como Rodar

```bash
pip install -r requirements.txt
python app.py
```

Acesse: **http://localhost:8080**

---

## Estrutura de Arquivos

```
event_log_builder/
├── app.py          # Servidor Flask: todas as rotas e lógica web
├── core.py         # Motor Python: carregamento, exportação, criptografia
├── requirements.txt
├── DOCUMENTACAO.md
├── templates/      # 7 HTMLs (base + 6 passos do wizard)
│   ├── base.html
│   ├── step1.html  # Upload do arquivo
│   ├── step2.html  # Mapeamento de colunas
│   ├── step3.html  # Seleção de atividades
│   ├── step4.html  # Resumo das fontes
│   ├── step5.html  # Criptografia de colunas sensíveis
│   └── step6.html  # Exportação
├── static/
│   ├── style.css   # Tema branco/laranja
│   └── script.js
└── uploads/        # Arquivos enviados (criado automaticamente)
```

---

## O Que o Sistema Faz

O Event Log Builder transforma bases de dados brutas (CSV, Excel, Parquet ou JSON) em um **Event Log** padronizado para ferramentas de Process Mining como ProM, Celonis, Disco e PM4Py.

### Fluxo em 6 Passos

| Passo | Ação |
|-------|------|
| 1 | Upload do arquivo + nome da fonte |
| 2 | Mapeamento manual de colunas (o usuário define o papel de cada coluna) |
| 3 | Seleção de atividades a incluir |
| 4 | Resumo das fontes — opção de adicionar mais uma fonte |
| 5 | Criptografia — escolha de colunas sensíveis para anonimização MD5 |
| 6 | Exportação: CSV e/ou SQL |

---

## Detalhes por Passo

### Passo 2 — Mapeamento de Colunas

O sistema lê automaticamente todas as colunas do arquivo. O usuário atribui manualmente o **tipo/referência** de cada coluna via dropdown:

| Tipo | Descrição |
|------|-----------|
| `Case_ID` | Identificador único do caso (obrigatório) |
| `Atividade` | Nome da atividade/etapa (obrigatório) |
| `Timestamp_Inicio` | Data/hora de início (obrigatório) |
| `Timestamp_Fim` | Data/hora de fim (opcional) |
| `Texto` | Coluna textual complementar |
| `Número` | Coluna numérica complementar |
| `Data` | Coluna de data complementar |
| `Outro` | Qualquer outro dado |

Para os tipos complementares (Texto, Número, Data, Outro), o usuário pode marcar **"Incluir na saída"** para que a coluna apareça no Event Log final além das colunas padrão.

**Regra de validação:** os tipos `Case_ID`, `Atividade` e `Timestamp_Inicio` são obrigatórios. O sistema bloqueia o avanço se algum deles não for mapeado.

---

### Passo 5 — Criptografia MD5

O usuário escolhe quais colunas do Event Log consolidado deseja anonimizar. O sistema aplica **MD5** sobre o valor textual de cada registro.

**Propriedade importante:** o mesmo valor original sempre gera o mesmo hash MD5. Isso significa que um `Case_ID` presente em duas fontes diferentes terá o mesmo hash nas duas — preservando a rastreabilidade entre bases mesmo após a anonimização.

Junto com o Event Log exportado, o sistema gera um arquivo Excel **de-para** (`de_para_criptografia.xlsx`) com uma aba por coluna criptografada, contendo:

| valor_original | hash_md5 |
|----------------|----------|
| 12345-CPF      | a1b2c3d4e5f6... |
| 67890-CPF      | f6e5d4c3b2a1... |

Este arquivo permite ao analista reverter a anonimização quando necessário.

---

## Formato de Saída do Event Log

| Coluna | Descrição |
|--------|-----------|
| `Case_ID` | Identificador único do caso/processo |
| `Activity` | Nome da atividade/etapa |
| `Timestamp_Start` | Data/hora de início |
| `Timestamp_End` | Data/hora de fim (opcional) |
| `Source` | Nome da fonte de origem |
| *(colunas extras)* | Colunas complementares marcadas para incluir |

---

## Arquitetura

### app.py — Servidor Flask

- **Sessões em memória**: estado de cada usuário em `ESTADOS` (dict Python), sem banco de dados
- **Rotas**: uma por passo (`/step1` a `/step6`) + `/api/activities` (AJAX) + downloads
- **Uploads**: salvos em `uploads/<session_id>/`
- **Nova sessão**: rota `/nova-sessao` limpa o estado e reinicia o fluxo

### core.py — Motor Python

Dividido em 3 seções:

**1. Utils**
- `carregar_dataframe(caminho)` — detecta extensão e carrega com pandas (CSV com auto-detect de separador e encoding)
- `parsear_timestamps(serie, fmt)` — converte coluna para datetime
- `get_unique_values(df, col)` — lista valores únicos de uma coluna

**2. Exportação**
- `TIPOS_REFERENCIA` — lista de tipos disponíveis para mapeamento
- `TIPOS_EXTRAS` — tipos que podem ser incluídos como colunas extras
- `TIPOS_OBRIGATORIOS` — tipos que devem obrigatoriamente estar mapeados
- `construir_event_log(sources)` → DataFrame empilhado com todas as fontes
- `exportar_sql_str(sources)` → query ANSI SQL com `UNION ALL`

**3. Criptografia**
- `md5_valor(valor)` → hash MD5 de 32 chars do valor textual
- `criptografar_event_log(event_log, colunas)` → Event Log anonimizado + dict de-para por coluna
- `exportar_depara_bytes(de_para)` → bytes de Excel com uma aba por coluna

---

## Dependências

| Pacote | Uso |
|--------|-----|
| `flask` | Servidor web |
| `pandas` | Manipulação de dados |
| `numpy` | Cálculos numéricos |
| `python-dateutil` | Parsing flexível de datas |
| `openpyxl` | Leitura de Excel e geração do de-para |
| `pyarrow` | Leitura/escrita de Parquet |
| `werkzeug` | Upload seguro de arquivos |

---

## Notas de Segurança

- O MD5 é usado aqui para **anonimização/pseudonimização**, não para segurança criptográfica (senhas, etc.)
- O arquivo de-para deve ser tratado como dado sensível — ele contém os valores originais
- Os arquivos de upload ficam no servidor enquanto a sessão estiver ativa; reiniciar o servidor limpa tudo
