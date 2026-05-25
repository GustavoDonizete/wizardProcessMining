# Como instalar

## Opção A — Pasta nova (recomendado)
```
unzip event_log_builder.zip
cd event_log_builder
pip install -r requirements.txt
python app.py
```
Acesse: http://localhost:8080

## Opção B — Substituir na pasta existente
Se você já tem o projeto rodando em outra pasta, copie:
- app.py
- core.py
- templates/   (pasta inteira)
- static/      (pasta inteira)

para dentro da pasta do projeto existente, substituindo tudo.

## IMPORTANTE
O Flask precisa ser executado DE DENTRO da pasta `event_log_builder/`.
Não execute de fora da pasta, pois ele não encontrará os arquivos `templates/` e `static/`.

```
# ERRADO
python /caminho/para/event_log_builder/app.py

# CERTO
cd /caminho/para/event_log_builder
python app.py
```
