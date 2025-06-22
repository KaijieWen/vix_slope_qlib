### Tracebacks on 1-month sample run
Traceback (most recent call last):
  File "/Users/wan/Desktop/vix_slope_qlib/vix_slope_system/train_backtest.py", line 15, in <module>
    from util import CFG, log
  File "/Users/wan/Desktop/vix_slope_qlib/vix_slope_system/util.py", line 7, in <module>
    CFG = load_config()
          ^^^^^^^^^^^^^
  File "/Users/wan/Desktop/vix_slope_qlib/vix_slope_system/util.py", line 4, in load_config
    with open(path, "r") as fh:
         ^^^^^^^^^^^^^^^
FileNotFoundError: [Errno 2] No such file or directory: 'config.yml'
