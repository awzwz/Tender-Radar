# demo_bidcheck (парсер приложений + генератор документов поставщиков)

Задача:
1) Спарсить 5 PDF-приложений (квалификационные требования/трудовые ресурсы и т.п.) в JSON.
2) Сгенерировать текстовые "пакеты документов" поставщиков (TXT + JSON) для демо.
LLM подключим позже.

## Быстрый старт
```bash
python -m venv .venv
# Windows:
.venv\Scripts\activate
pip install -r requirements.txt

python run_demo.py
```

## Что получится
- `outputs/appendix_7_16289545.requirements.json` — извлечённые требования (в т.ч. трудовые ресурсы)
- `outputs/suppliers/*.txt` — текстовые пакеты документов поставщиков
- `outputs/suppliers/*.json` — структурированная версия тех же данных

## Как добавить ещё PDF (когда дашь 5 ссылок)
Положи их в `inputs/` (или скачай по URL) и вызови парсер для каждого:

```bash
python -m parser.parse_appendix --pdf inputs/APPENDIX_X.pdf --out outputs/APPENDIX_X.requirements.json
python -m generator.generate_supplier_docs --req outputs/APPENDIX_X.requirements.json --outdir outputs/suppliers_APPENDIX_X
```

> Примечание: текущий парсер заточен под hackathon-формат и ищет секцию "Трудовые ресурсы/Еңбек ресурстары" + роли/кол-во.
Если в других 4 PDF формат будет чуть другой — расширим patterns.py.
