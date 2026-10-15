"""Проверяем только явно перечисленные исходники; файловую систему пользователя не обходим."""

import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def listed_files(root: Path = ROOT) -> list[Path]:
    entries = (root / "PUBLIC_FILES.txt").read_text(encoding="utf-8").splitlines()
    if not entries or entries != sorted(set(entries)):
        raise ValueError("Список публикуемых файлов должен быть непустым, отсортированным и без повторов.")
    files = []
    for entry in entries:
        relative = Path(entry)
        if (relative.is_absolute() or ".." in relative.parts
                or any(part in {".git", ".venv", "workspace", "artifacts", "sources"} for part in relative.parts)
                or relative.name.startswith(".env")
                or relative.suffix.lower() in {".csv", ".parquet", ".duckdb", ".jsonl", ".ndjson", ".pem", ".key", ".log"}):
            raise ValueError("Небезопасный путь в списке публикуемых файлов.")
        path = root / relative
        if not path.is_file() or path.stat().st_size > 1_000_000:
            raise ValueError("Публикуемый файл отсутствует или превышает допустимый размер.")
        if any(part.is_symlink() for part in (path, *path.parents) if part != root.parent):
            raise ValueError("Символические ссылки в публикуемых исходниках запрещены.")
        files.append(path)
    return files


def inspect_content(text: str) -> None:
    patterns = (
        (r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----", "закрытый ключ"),
        (r"\bgh[pousr]_[A-Za-z0-9_]{20,}\b", "токен GitHub"),
        (r"\bgithub_pat_[A-Za-z0-9_]{20,}\b", "токен GitHub"),
        (r"\bAKIA[A-Z0-9]{16}\b", "ключ облачного сервиса"),
        (r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", "адрес электронной почты"),
        (r"(?:/home/|/Users/)[A-Za-z0-9_.-]+/|[A-Z]:\\Users\\[A-Za-z0-9_.-]+", "приватный локальный путь"),
        (r"https?://[^\s/:]+:[^\s/]+@", "пароль в адресе URL"),
    )
    for pattern, kind in patterns:
        if re.search(pattern, text, re.IGNORECASE):
            raise ValueError("В исходниках обнаружен возможный секрет: " + kind + ".")


def check(root: Path = ROOT) -> list[Path]:
    files = listed_files(root)
    for path in files:
        inspect_content(path.read_text(encoding="utf-8"))
    git_root = subprocess.run(["git", "rev-parse", "--show-toplevel"], cwd=root, capture_output=True, text=True)
    if git_root.returncode == 0 and Path(git_root.stdout.strip()).resolve() == root.resolve():
        tracked = subprocess.check_output(["git", "ls-files", "-z"], cwd=root).decode().split("\0")
        allowed = {p.relative_to(root).as_posix() for p in files}
        if set(filter(None, tracked)) - allowed:
            raise ValueError("В Git есть файлы вне списка публикации.")
        history = subprocess.run(["git", "log", "--format=%ae%n%ce"], cwd=root, capture_output=True, text=True)
        if any(not address.lower().endswith("@users.noreply.github.com")
               for address in history.stdout.splitlines()):
            print("Примечание: Git содержит адреса авторства вне домена no-reply. "
                  "Эти метаданные видны в публичной истории; в исходный ZIP они не входят.")
    return files


if __name__ == "__main__":
    try:
        files = check()
    except (ValueError, OSError, UnicodeError, subprocess.SubprocessError) as exc:
        raise SystemExit("Проверка публикации не пройдена: " + str(exc))
    print(f"Проверка публикации пройдена: {len(files)} текстовых файлов; лишних данных в Git нет.")
