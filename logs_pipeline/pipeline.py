"""Ленивые выражения Polars, проверка качества и полная замена снимка выгрузки."""

import hashlib
import json
import math
import os
import re
import shutil
import tempfile
import uuid
from pathlib import Path

import polars as pl

SCHEMA = {"event_id": pl.String, "timestamp": pl.String, "service": pl.String,
          "status_code": pl.Int64, "duration_ms": pl.Float64}
SERVICES = ("api", "worker", "gateway")
REASONS = {"event_id": "Код события", "timestamp": "Дата и время",
           "service": "Сервис", "status_code": "HTTP-статус", "duration_ms": "Время ответа"}
MARKER = ".logs-snapshot"
MAX_BYTES = 8 * 1024 * 1024
MAX_ROWS = 100_000


class PipelineError(Exception):
    """Ошибка без раскрытия исходных значений и локальных путей."""


def _reject_constant(value):
    raise PipelineError("В JSON нельзя использовать NaN или Infinity.")


def _strict_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise PipelineError("Повтор одного JSON-ключа внутри объекта запрещён.")
        result[key] = value
    return result


def sources(directory: Path, validate: bool = True) -> tuple[list[Path], list[str]]:
    if not directory.is_dir():
        raise PipelineError("Папка с журналами не существует.")
    files = sorted(directory.glob("*.jsonl"))
    if not 1 <= len(files) <= 100:
        raise PipelineError("Нужно от 1 до 100 JSONL-файлов.")
    hashes, total_bytes, rows = [], 0, 0
    for path in files:
        if path.is_symlink() or not path.is_file():
            raise PipelineError("Символические ссылки и специальные входные файлы запрещены.")
        with path.open("rb") as handle:
            data = handle.read(MAX_BYTES + 1)
        total_bytes += len(data)
        if len(data) > MAX_BYTES or total_bytes > 32 * 1024 * 1024:
            raise PipelineError("Предел: 8 МиБ на файл и 32 МиБ на весь набор.")
        hashes.append(hashlib.sha256(data).hexdigest())
        if not validate:
            continue
        try:
            for line in data.decode("utf-8").splitlines():
                if not line.strip():
                    continue
                rows += 1
                if rows > MAX_ROWS:
                    raise PipelineError("В наборе более 100 000 входных записей.")
                record = json.loads(line, parse_constant=_reject_constant, object_pairs_hook=_strict_object)
                if not isinstance(record, dict) or set(record) - set(SCHEMA):
                    raise PipelineError("Нужен JSON-объект только с полями описанной схемы.")
                for key, value in record.items():
                    if value is None:
                        continue
                    valid = (type(value) is str if key in ("event_id", "timestamp", "service")
                             else type(value) is int if key == "status_code"
                             else type(value) in (int, float) and math.isfinite(value))
                    if not valid:
                        raise PipelineError("Неверный тип JSON-поля; строки с числами и логические значения не принимаются.")
        except (UnicodeError, ValueError, OverflowError) as exc:
            raise PipelineError("Файл должен содержать корректные JSON-объекты в строках UTF-8.") from exc
    if validate and rows == 0:
        raise PipelineError("В файлах нет записей.")
    return files, sorted(hashes)


def checked_plan(files: list[Path]) -> pl.LazyFrame:
    raw = pl.scan_ndjson(files, schema=SCHEMA, row_index_name="row_index").with_columns(
        pl.when(pl.col("timestamp").str.contains(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$").fill_null(False))
        .then(pl.col("timestamp")).otherwise(pl.lit(None,dtype=pl.String)).alias("timestamp")
    )
    timestamp = pl.col("timestamp").str.to_datetime("%Y-%m-%dT%H:%M:%SZ", strict=False, time_zone="UTC")
    frame = raw.with_columns(timestamp.alias("timestamp"))
    return frame.with_columns(
        pl.when(~pl.col("event_id").str.contains(r"^LOG-[0-9]{8}$").fill_null(False)).then(pl.lit("event_id"))
        .when(pl.col("timestamp").is_null() | ~pl.col("timestamp").dt.year().is_between(1,9999)).then(pl.lit("timestamp"))
        .when(~pl.col("service").is_in(SERVICES).fill_null(False)).then(pl.lit("service"))
        .when(~pl.col("status_code").is_between(100, 599).fill_null(False)).then(pl.lit("status_code"))
        .when(~(pl.col("duration_ms").is_finite() & pl.col("duration_ms").is_between(0, 60_000)).fill_null(False)).then(pl.lit("duration_ms"))
        .otherwise(pl.lit(None, dtype=pl.String)).alias("reason")
    )


def aggregate(frame: pl.DataFrame, keys: list[str], slow_ms: float) -> pl.DataFrame:
    return (frame.lazy().with_columns(
        (pl.col("status_code") >= 500).alias("server_error"),
        pl.col("status_code").is_between(400, 499).alias("client_error"),
        (pl.col("duration_ms") >= slow_ms).alias("slow"),
    ).group_by(keys).agg(
        pl.len().alias("requests"), pl.col("server_error").sum().alias("server_errors"),
        pl.col("client_error").sum().alias("client_errors"),
        (pl.col("server_error").mean() * 100).round(2).alias("error_rate_pct"),
        pl.col("duration_ms").mean().round(2).alias("avg_duration_ms"),
        pl.col("duration_ms").quantile(0.95, interpolation="linear").round(2).alias("p95_duration_ms"),
        pl.col("slow").sum().alias("slow_requests"),
    ).sort(keys).collect(engine="streaming"))


def _check_output(directory: Path) -> None:
    if directory.is_symlink():
        raise PipelineError("Символическая ссылка на папку результата запрещена.")
    if not directory.exists():
        return
    if not directory.is_dir() or (directory / MARKER).is_symlink() or not (directory / MARKER).is_file():
        raise PipelineError("Выберите новую папку: существующая не принадлежит этому проекту.")
    if (directory / MARKER).read_text(encoding="utf-8") != "polars-service-logs\n":
        raise PipelineError("Маркер папки результатов повреждён.")
    allowed = {MARKER, "manifest.json", "hourly.parquet", "hourly.csv", "quality.json", "preview.md"}
    for path in directory.rglob("*"):
        relative = path.relative_to(directory).as_posix()
        if path.is_symlink():
            raise PipelineError("В папке результатов обнаружена символическая ссылка.")
        if path.is_file() and relative not in allowed and not re.fullmatch(r"events/date=[0-9]{4}-[0-9]{2}-[0-9]{2}/part.parquet", relative):
            raise PipelineError("В папке результатов есть посторонние файлы; выберите другую папку.")


def _publish(stage: Path, output: Path) -> None:
    backup = None
    try:
        if output.exists():
            backup = output.with_name(output.name + ".backup-" + uuid.uuid4().hex)
            os.replace(output, backup)
        os.replace(stage, output)
    except BaseException:
        if backup and backup.exists() and not output.exists():
            os.replace(backup, output)
        raise
    if backup:
        shutil.rmtree(backup)


def _preview(summary: dict, per_service: pl.DataFrame, quality: list[dict]) -> str:
    rows = []
    for row in per_service.iter_rows(named=True):
        rows.append(f"| {row['service']} | {row['requests']} | {row['server_errors']} | {row['error_rate_pct']:.2f} | {row['avg_duration_ms']:.2f} | {row['p95_duration_ms']:.2f} |")
    reasons = "\n".join(f"- {REASONS[row['reason']]}: {row['rows']}" for row in quality) or "Ошибочных строк нет."
    return f"""# Журналы сервисов — результат обработки

Входных строк: **{summary['input_rows']}**. Уникальных запросов: **{summary['accepted_rows']}**.
Пропущено дублей: **{summary['duplicate_rows']}**. Отклонено: **{summary['rejected_rows']}**.
Разделов по дате UTC: **{summary['partitions']}**. Почасовых групп: **{summary['hourly_groups']}**.

## Сервисы

| Сервис | Запросов | Ошибок 5xx | Ошибок, % | Среднее, мс | p95, мс |
| --- | --- | --- | --- | --- | --- |
{chr(10).join(rows)}

Порог медленного запроса: {summary['slow_ms']} мс, включительно.
Ошибки 4xx считаются отдельно от 5xx. p95 вычисляется линейной интерполяцией.

## Качество входа

{reasons}

Технические таблицы: hourly.parquet и hourly.csv. Очищенные события: events/date=.../part.parquet.
Это локальный результат. Команда demo создаёт синтетические данные; свои настоящие журналы не публикуйте.
"""


def run(input_dir: Path, output: Path, slow_ms: float = 500) -> dict:
    if not math.isfinite(slow_ms) or not 1 <= slow_ms <= 60_000:
        raise PipelineError("Порог времени ответа должен быть от 1 до 60 000 мс.")
    source_path, target_path = input_dir.resolve(), output.resolve()
    if source_path == target_path or source_path in target_path.parents or target_path in source_path.parents:
        raise PipelineError("Папки входа и результата не должны пересекаться.")
    _check_output(output)
    files, hashes = sources(input_dir)
    # collect возвращает таблицу в память; режим streaming не делает весь проект безразмерным.
    checked = checked_plan(files).collect(engine="streaming")
    valid = checked.filter(pl.col("reason").is_null()).select(list(SCHEMA))
    clean = valid.unique().sort("event_id")
    if clean.group_by("event_id").len().filter(pl.col("len") > 1).height:
        raise PipelineError("Один код события имеет разное содержимое; прежний результат сохранён.")
    rejected = checked.filter(pl.col("reason").is_not_null())
    quality = rejected.group_by("reason").agg(pl.len().alias("rows")).sort("reason").to_dicts()
    clean = clean.with_columns(pl.col("timestamp").dt.date().alias("date"),
                               pl.col("timestamp").dt.truncate("1h").alias("hour"))
    if clean["date"].n_unique() > 366:
        raise PipelineError("В учебном наборе допускается не более 366 дат UTC.")
    hourly = aggregate(clean, ["hour", "service"], slow_ms)
    per_service = aggregate(clean, ["service"], slow_ms)
    if int(hourly["requests"].sum()) != clean.height:
        raise PipelineError("Число событий и сумма почасовых запросов не совпали.")
    output.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=".logs-stage-", dir=output.parent))
    try:
        for partition in clean.partition_by("date", maintain_order=True):
            day = partition["date"][0]
            folder = stage / "events" / f"date={day}"
            folder.mkdir(parents=True)
            partition.drop("date", "hour").write_parquet(folder/"part.parquet", compression="zstd")
        hourly.write_parquet(stage/"hourly.parquet", compression="zstd")
        hourly.write_csv(stage/"hourly.csv")
        (stage/"quality.json").write_text(json.dumps(quality, ensure_ascii=False, indent=2)+"\n", encoding="utf-8")
        summary = {"input_rows": checked.height, "accepted_rows": clean.height,
                   "duplicate_rows": valid.height-clean.height, "rejected_rows": rejected.height,
                   "partitions": clean["date"].n_unique(), "hourly_groups": hourly.height,
                   "server_errors": int(hourly["server_errors"].sum()), "client_errors": int(hourly["client_errors"].sum()),
                   "slow_ms": slow_ms}
        (stage/"preview.md").write_text(_preview(summary, per_service, quality), encoding="utf-8")
        if sources(input_dir, validate=False)[1] != hashes:
            raise PipelineError("Входные файлы изменились во время обработки; повторите запуск.")
        manifest = {"format_version": 1, "polars_version": pl.__version__, "source_sha256": hashes,
                    "summary": summary, "files": {p.relative_to(stage).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
                                                   for p in sorted(stage.rglob("*")) if p.is_file()}}
        (stage/"manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2)+"\n", encoding="utf-8")
        (stage/MARKER).write_text("polars-service-logs\n", encoding="utf-8")
        _check_output(output)
        _publish(stage, output)
        return summary
    finally:
        if stage.exists():
            shutil.rmtree(stage)
