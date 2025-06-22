import pathlib, yaml, logging

def load_config(path: str = "config.yml"):
    with open(path, "r") as fh:
        return yaml.safe_load(fh)

CFG = load_config()

def ensure_dirs() -> None:
    """Create folders declared in config if they don’t yet exist."""
    for p in CFG["paths"].values():
        pathlib.Path(p).mkdir(parents=True, exist_ok=True)

def log(msg: str, level=logging.INFO) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
    logging.log(level, msg)
