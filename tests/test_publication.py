import importlib.util
import tempfile
import unittest
from pathlib import Path

spec = importlib.util.spec_from_file_location("check_public", Path(__file__).resolve().parents[1] / "scripts/check_public.py")
checker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checker)


class PublicationTests(unittest.TestCase):
    def test_secret_and_contact_patterns_are_rejected(self):
        candidates = ["ghp_" + "a" * 30, "test" + "@" + "example" + ".test",
                      "-----BEGIN " + "PRIVATE KEY-----", "https://user:" + "pass@" + "example.test"]
        for candidate in candidates:
            with self.subTest(kind=candidate[:4]):
                with self.assertRaises(ValueError):
                    checker.inspect_content(candidate)

    def test_manifest_cannot_include_private_workspace_or_path_escape(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / "workspace").mkdir()
            (root / "workspace/input.csv").write_text("synthetic", encoding="utf-8")
            for value in ["../other.py", "workspace/input.csv", ".env"]:
                (root / "PUBLIC_FILES.txt").write_text(value + "\n", encoding="utf-8")
                with self.assertRaises(ValueError):
                    checker.listed_files(root)


if __name__ == "__main__":
    unittest.main()
