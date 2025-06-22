"""
Parse vix_slope_system/feature_engineering.py *statically* (no import)
and dump docs/factor_map.xlsx with every function name it defines.
"""
import ast, os, pandas as pd, openpyxl

SRC = "vix_slope_system/feature_engineering.py"
with open(SRC, "r") as fh:
    tree = ast.parse(fh.read(), filename=SRC)

rows = []
for node in tree.body:
    if isinstance(node, ast.FunctionDef):
        doc = ast.get_docstring(node) or ""
        rows.append((node.name, doc.split("\n")[0].strip()))

os.makedirs("docs", exist_ok=True)
pd.DataFrame(rows, columns=["Factor", "Legacy_Formula"]).to_excel(
    "docs/factor_map.xlsx", index=False
)
print("✅  docs/factor_map.xlsx written with", len(rows), "rows")
