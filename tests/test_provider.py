import qlib, os
from vix_provider.vix_provider import VixProvider

def test_load():
    qlib.init(config="configs/qlib/vix_local.yaml", verbose=False)
    prov = VixProvider("qlib_data")
    sym = prov.instruments("daily")[0]
    cal = prov.calendar("daily")
    df = prov.load(["close"], [sym], "daily", str(cal[0]), str(cal[-1]))
    assert not df.empty and "close" in df.columns
