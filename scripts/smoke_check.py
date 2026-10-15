"""Проверка стандартного демо и повторной сборки без удвоения запросов."""

from pathlib import Path

import polars as pl


def main():
    # Запуск через модуль из корня проекта сохраняет обычный путь поиска пакета.
    import subprocess
    import sys
    import json
    result = subprocess.run([sys.executable,"-m","logs_pipeline","run","--json"],capture_output=True,text=True,encoding="utf-8",check=True)
    summary = json.loads(result.stdout)
    expected = {"input_rows":10105,"accepted_rows":10000,"duplicate_rows":100,"rejected_rows":5,
                "partitions":3,"hourly_groups":150,"server_errors":500,"client_errors":500}
    assert all(summary[key]==value for key,value in expected.items())
    hourly = pl.read_parquet(Path("workspace/output/hourly.parquet"))
    assert hourly["requests"].sum()==10000
    print("Демо и повторная сборка проверены: 10 000 запросов, без удвоения.")


if __name__=="__main__":
    main()
