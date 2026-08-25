from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "services" / "shared"))
sys.path.insert(0, str(PROJECT_ROOT / "services" / "processing-service"))

# The library default (AUTH_MODE unset) is "strict" — this test suite
# exercises the platform the way its own local `.env.example` configures it
# (AUTH_MODE=dev_compat), matching this project's actual local/demo
# environment rather than a stricter posture nothing in this repo's default
# local setup actually runs with. `setdefault` so any test that explicitly
# sets AUTH_MODE itself (e.g. tests/test_auth_hardening.py exercising
# strict mode directly) is never overridden by this.
os.environ.setdefault("AUTH_MODE", "dev_compat")

