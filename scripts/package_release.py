"""Сборка воспроизводимого ZIP только из проверенных исходников по списку."""

import sys
import hashlib
import zipfile

from check_public import ROOT, check


def main() -> None:
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8")
    files = check()
    folder = ROOT / "artifacts"
    folder.mkdir(exist_ok=True)
    output = folder / "polars-service-logs-0.1.0.zip"
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as archive:
        for path in files:
            info = zipfile.ZipInfo("polars-service-logs/" + path.relative_to(ROOT).as_posix(), (2026, 10, 15, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o100644 << 16
            archive.writestr(info, path.read_bytes())
    with zipfile.ZipFile(output) as archive:
        if archive.testzip() is not None:
            raise RuntimeError("Проверка целостности ZIP релиза не пройдена.")
    digest = hashlib.sha256(output.read_bytes()).hexdigest()
    (folder / "SHA256SUMS").write_text(digest + "  " + output.name + "\n", encoding="utf-8")
    print("Собран релиз с исходниками: " + output.name)


if __name__ == "__main__":
    main()
