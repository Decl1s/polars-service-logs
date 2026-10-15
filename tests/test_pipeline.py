import hashlib
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import polars as pl

from logs_pipeline.pipeline import PipelineError, run, sources
from logs_pipeline.synthetic import generate


def event(i=1, **changes):
    return {"event_id": f"LOG-{i:08d}", "timestamp": "2026-10-01T10:15:00Z",
            "service": "api", "status_code": 200, "duration_ms": 100, **changes}


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.input = self.root/"input"
        self.input.mkdir()
        self.output = self.root/"output"

    def tearDown(self):
        self.temp.cleanup()

    def write(self, rows, name="batch.jsonl"):
        (self.input/name).write_text("".join(json.dumps(row, allow_nan=False)+"\n" for row in rows), encoding="utf-8")

    def test_metrics_and_linear_p95(self):
        self.write([event(), event(), event(2, duration_ms=200, status_code=500), event(3, duration_ms=300, status_code=404)])
        result = run(self.input, self.output, slow_ms=200)
        self.assertEqual((result["input_rows"],result["accepted_rows"],result["duplicate_rows"]),(4,3,1))
        row = pl.read_parquet(self.output/"hourly.parquet").row(0,named=True)
        self.assertEqual(row["requests"],3)
        self.assertEqual(row["server_errors"],1)
        self.assertEqual(row["client_errors"],1)
        self.assertEqual(row["slow_requests"],2)
        self.assertEqual(row["error_rate_pct"],33.33)
        self.assertEqual(row["avg_duration_ms"],200)
        self.assertEqual(row["p95_duration_ms"],290)

    def test_utc_date_partitions_can_be_read_together(self):
        self.write([event(timestamp="2026-10-01T23:59:59Z"),event(2,timestamp="2026-10-02T00:00:00Z")])
        result = run(self.input,self.output)
        self.assertEqual(result["partitions"],2)
        self.assertEqual(len(list((self.output/"events").rglob("*.parquet"))),2)
        frame = pl.scan_parquet(str(self.output/"events/**/*.parquet"),hive_partitioning=True).collect()
        self.assertEqual(frame.height,2)
        self.assertEqual(frame["date"].n_unique(),2)

    def test_invalid_values_and_missing_field_enter_quality_summary(self):
        missing=event(7)
        del missing["duration_ms"]
        self.write([event(),event(2,event_id="bad"),event(3,timestamp="wrong"),event(4,service="unknown"),
                    event(5,status_code=999),event(6,duration_ms=-1),missing])
        result=run(self.input,self.output)
        self.assertEqual(result["accepted_rows"],1)
        self.assertEqual(result["rejected_rows"],6)
        quality=json.loads((self.output/"quality.json").read_text())
        self.assertEqual(sum(row["rows"] for row in quality),6)
        self.assertEqual(next(row["rows"] for row in quality if row["reason"]=="duration_ms"),2)

    def test_conflicting_id_preserves_previous_snapshot(self):
        self.write([event()])
        run(self.input,self.output)
        original=(self.output/"manifest.json").read_bytes()
        self.write([event(),event(duration_ms=400)])
        with self.assertRaisesRegex(PipelineError,"разное содержимое"):
            run(self.input,self.output)
        self.assertEqual(original,(self.output/"manifest.json").read_bytes())

    def test_invalid_json_and_types_fail_without_replacing_snapshot(self):
        self.write([event()])
        run(self.input,self.output)
        original=(self.output/"manifest.json").read_bytes()
        bad=["not-json\n",json.dumps(event(status_code="200"))+"\n",json.dumps(event(duration_ms=True))+"\n",
             json.dumps({**event(),"extra":"unwanted"})+"\n",json.dumps(event())[:-1]+',"duration_ms":NaN}\n',
             json.dumps(event())[:-1]+',"duration_ms":200}\n']
        for raw in bad:
            with self.subTest(raw=raw[:12]):
                (self.input/"batch.jsonl").write_text(raw,encoding="utf-8")
                with self.assertRaises(PipelineError):run(self.input,self.output)
                self.assertEqual(original,(self.output/"manifest.json").read_bytes())

    def test_repeat_rebuilds_same_business_rows(self):
        self.write([event(),event(),event(2)])
        first=run(self.input,self.output)
        table=pl.read_parquet(self.output/"hourly.parquet")
        second=run(self.input,self.output)
        self.assertEqual(first,second)
        self.assertTrue(table.equals(pl.read_parquet(self.output/"hourly.parquet")))

    def test_removed_source_does_not_leave_old_date_partition(self):
        self.write([event()],"one.jsonl")
        self.write([event(2,timestamp="2026-10-02T00:00:00Z")],"two.jsonl")
        run(self.input,self.output)
        (self.input/"two.jsonl").unlink()
        result=run(self.input,self.output)
        self.assertEqual(result["partitions"],1)
        self.assertFalse((self.output/"events/date=2026-10-02").exists())

    def test_all_invalid_rows_create_empty_mart(self):
        self.write([event(duration_ms=-1)])
        result=run(self.input,self.output)
        self.assertEqual(result["accepted_rows"],0)
        self.assertEqual(result["partitions"],0)
        self.assertEqual(pl.read_parquet(self.output/"hourly.parquet").height,0)
        self.assertIn("Время ответа",(self.output/"preview.md").read_text(encoding="utf-8"))

    def test_export_failure_keeps_previous_output_and_cleans_stage(self):
        self.write([event()])
        run(self.input,self.output)
        original=(self.output/"manifest.json").read_bytes()
        with patch("polars.DataFrame.write_parquet",side_effect=OSError("синтетический сбой")):
            with self.assertRaises(OSError):run(self.input,self.output)
        self.assertEqual(original,(self.output/"manifest.json").read_bytes())
        self.assertEqual(list(self.root.glob(".logs-stage-*")),[])

    def test_rename_failure_restores_previous_output(self):
        self.write([event()])
        run(self.input,self.output)
        original=(self.output/"manifest.json").read_bytes()
        replace=os.replace
        calls=0
        def fail_second(source,target):
            nonlocal calls
            calls+=1
            if calls==2:raise OSError("синтетический сбой")
            return replace(source,target)
        with patch("logs_pipeline.pipeline.os.replace",side_effect=fail_second):
            with self.assertRaises(OSError):run(self.input,self.output)
        self.assertEqual(original,(self.output/"manifest.json").read_bytes())

    def test_changed_source_during_processing_is_not_published(self):
        self.write([event()])
        run(self.input,self.output)
        original=(self.output/"manifest.json").read_bytes()
        initial=sources(self.input)
        with patch("logs_pipeline.pipeline.sources",side_effect=[initial,(initial[0],["different"])]):
            with self.assertRaisesRegex(PipelineError,"изменились"):
                run(self.input,self.output)
        self.assertEqual(original,(self.output/"manifest.json").read_bytes())

    def test_unrelated_files_and_overlapping_paths_are_protected(self):
        self.write([event()])
        run(self.input,self.output)
        note=self.output/"keep.txt"
        note.write_text("оставить",encoding="utf-8")
        with self.assertRaisesRegex(PipelineError,"посторонние"):
            run(self.input,self.output)
        self.assertEqual(note.read_text(encoding="utf-8"),"оставить")
        with self.assertRaisesRegex(PipelineError,"пересекаться"):
            run(self.input,self.input)
        unrelated=self.root/"unrelated"
        unrelated.mkdir()
        with self.assertRaises(PipelineError):run(self.input,unrelated)

    def test_input_symlinks_are_rejected(self):
        self.write([event()])
        try:(self.input/"copy.jsonl").symlink_to(self.input/"batch.jsonl")
        except OSError:self.skipTest("Нужны права ОС для символической ссылки")
        with self.assertRaisesRegex(PipelineError,"Символические"):
            run(self.input,self.output)

    def test_manifest_checksums_and_seeded_demo(self):
        first=self.root/"demo-a"
        second=self.root/"demo-b"
        generate(first,rows=100)
        generate(second,rows=100)
        self.assertEqual((first/"part-01.jsonl").read_bytes(),(second/"part-01.jsonl").read_bytes())
        result=run(first,self.output)
        self.assertEqual((result["accepted_rows"],result["duplicate_rows"],result["rejected_rows"]),(100,1,5))
        manifest=json.loads((self.output/"manifest.json").read_text())
        for name,digest in manifest["files"].items():
            self.assertEqual(hashlib.sha256((self.output/name).read_bytes()).hexdigest(),digest)
        with self.assertRaises(PipelineError):generate(first,rows=100)

    def test_russian_cli_and_json_mode(self):
        cli=[sys.executable,"-m","logs_pipeline"]
        help_result=subprocess.run(cli+["--help"],capture_output=True,text=True,encoding="utf-8")
        self.assertIn("Использование:",help_result.stdout)
        demo=subprocess.run(cli+["demo","--rows","10","--input",str(self.root/"cli"),"--json"],capture_output=True,text=True,encoding="utf-8")
        self.assertEqual(demo.returncode,0,demo.stderr)
        self.assertEqual(json.loads(demo.stdout)["events"],10)
        execute=subprocess.run(cli+["run","--input",str(self.root/"cli"),"--output",str(self.output),"--json"],capture_output=True,text=True,encoding="utf-8")
        self.assertEqual(execute.returncode,0,execute.stderr)
        self.assertEqual(json.loads(execute.stdout)["accepted_rows"],10)


if __name__=="__main__":
    unittest.main()
