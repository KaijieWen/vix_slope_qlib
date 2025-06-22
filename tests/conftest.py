"""
Pytest configuration: make project packages importable from any cwd and
guarantee that `import util` works inside tests.
"""
import sys, types, pathlib, importlib.util, importlib.machinery

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.extend([str(ROOT), str(ROOT / "vix_slope_system")])

# try real util first -------------------------------------------------
try:
    import util                       # noqa: F401  (already importable)
except ModuleNotFoundError:
    # fall back to a stub so feature_engineering can import safely
    stub = types.ModuleType("util")
    stub.CFG = {"paths": {}}
    stub.ensure_dirs = lambda *a, **k: None
    stub.log = lambda *a, **k: None
    sys.modules["util"] = stub
