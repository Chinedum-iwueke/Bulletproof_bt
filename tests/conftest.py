from __future__ import annotations

import sys
from pathlib import Path

# Ensure 'src' is on sys.path so `import bt` works when running pytest
# without installing the package (common in CI/Codex containers).
ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
