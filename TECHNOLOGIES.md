# Фактические технологии

| Компонент | Версия | Назначение и причина выбора | Официальный источник |
| --- | --- | --- | --- |
| Python | 3.11–3.12; локально 3.12.14 | Команды, строгая проверка JSON и управление файлами | https://docs.python.org/3.12/ |
| polars | 2.0.0, прямая зависимость | Ленивое чтение, выражения, группировки, перцентиль и Parquet | https://docs.pola.rs/ |
| polars-runtime-32 | 2.0.0, обязательная транзитивная зависимость, тоже закреплена | Бинарный движок Polars на Rust; отдельная установка Rust не нужна | https://pypi.org/project/polars/2.0.0/ |
| JSONL / NDJSON | UTF-8 | Один JSON-объект на строку, удобные пакетные журналы | https://docs.pola.rs/user-guide/io/json/ |
| Apache Parquet и Zstandard | Встроены в бинарный движок Polars | Типизированные столбцовые выгрузки со сжатием | https://docs.pola.rs/user-guide/io/parquet/ |
| Markdown и JSON | Текстовые форматы | Русская сводка, конспект и машинные метаданные | https://spec.commonmark.org/ |
| unittest и unittest.mock | Стандартная библиотека Python | Поведение и воспроизводимые сбои | https://docs.python.org/3.12/library/unittest.html |
| pip и venv | В составе Python | Изолированная установка двух закреплённых пакетов | https://pip.pypa.io/ |
| Git | 2.x | Собственная история проекта со служебным авторством | https://git-scm.com/docs |
| GitHub Actions | Настроены Ubuntu/Python 3.11, Ubuntu/Python 3.12, Windows/Python 3.12 | Повторяемые проверки при публикации | https://docs.github.com/ru/actions |
| actions/checkout | v4, SHA закреплён в CI | Официальное получение исходников | https://github.com/actions/checkout |
| actions/setup-python | v5, SHA закреплён в CI | Официальная установка Python | https://github.com/actions/setup-python |
| actions/upload-artifact | v4, SHA закреплён в CI | Сохранение проверенного исходного архива | https://github.com/actions/upload-artifact |

`requirements.txt` содержит полный перечень установленных runtime-пакетов:
polars==2.0.0 и polars-runtime-32==2.0.0. Дополнительные возможности Polars не включены.
Бинарный пакет рассчитан на поддерживаемую архитектуру и инструкции процессора;
совместимость со старым процессором отдельно не проверялась.

Используемые стандартные модули Python: argparse, datetime, hashlib, importlib,
json, math, os, pathlib, random, re, shutil, subprocess, sys, tempfile, unittest,
uuid и zipfile. Отдельно устанавливать их не нужно.

Проект не зависит от DuckDB, Pandas, PyArrow, сервера базы и облачного аккаунта.
Локальная обработка не выполняет сетевых запросов. Установка пакетов и CI используют сеть.
Архив распространяет исходники; библиотеки устанавливаются отдельно из PyPI.
