"""Воспроизводимые синтетические журналы без IP, URL, пользователей и контактов."""

import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .pipeline import PipelineError, SERVICES


def generate(directory: Path, rows: int = 10_000, seed: int = 42) -> dict:
    if not 10 <= rows <= 90_000:
        raise PipelineError("Выберите от 10 до 90 000 синтетических запросов.")
    if directory.exists() and any(directory.iterdir()):
        raise PipelineError("Для генерации нужна пустая папка.")
    directory.mkdir(parents=True, exist_ok=True)
    rng = random.Random(seed)
    base = datetime(2026, 10, 1, tzinfo=timezone.utc)
    events = []
    for i in range(1, rows+1):
        events.append({"event_id": f"LOG-{i:08d}",
                       "timestamp": (base+timedelta(seconds=(i-1)*18)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                       "service": SERVICES[(i-1)%3],
                       "status_code": 500 if i%20 == 0 else 404 if i%10 == 0 else 200,
                       "duration_ms": round(rng.uniform(5,1500),2)})
    bad = []
    for i,(field,value) in enumerate([("timestamp","не дата"),("status_code",999),
                                     ("duration_ms",-1),("service","unknown"),("event_id","не код")],1):
        item = {**events[0], "event_id": f"LOG-{90_000_000+i:08d}"}
        item[field] = value
        bad.append(item)
    duplicates = max(1,rows//100)
    batches = [events[:rows//2]+events[:duplicates],events[rows//2:]+bad]
    for i,batch in enumerate(batches,1):
        with (directory/f"part-{i:02d}.jsonl").open("w",encoding="utf-8",newline="\n") as handle:
            for event in batch:
                handle.write(json.dumps(event,ensure_ascii=False,allow_nan=False)+"\n")
    return {"events":rows,"duplicates":duplicates,"invalid_rows":5,"seed":seed}
