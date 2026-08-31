import os
import sys

# Ensure the repo root is importable (namespace packages: src.common, src.ml, ...)
# and that Pipeline/src is importable too: several config modules use absolute
# "common.*" imports, which is how they resolve inside the Spark container.
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
for entry in (ROOT, os.path.join(ROOT, "src")):
    if entry not in sys.path:
        sys.path.insert(0, entry)