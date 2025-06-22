import os, yaml, pathlib

def load_config(path: str | None = None):
    """Return parsed YAML; fall back to sane defaults."""
    path = path or pathlib.Path(__file__).with_name("config.yml")
    if not path.exists():
        return {"paths": {}, "symbols": []}
    with open(path, "r") as fh:
        cfg = yaml.safe_load(fh) or {}
    cfg.setdefault("paths", {})
    cfg.setdefault("symbols", [])
    return cfg

CFG = load_config()

def ensure_dirs():
    for p in CFG["paths"].values():
        os.makedirs(p, exist_ok=True)

def log(msg, lvl=20):
    print(f"{lvl}| {msg}")
