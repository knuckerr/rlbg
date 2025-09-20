import threading
import datetime
import sys


class Logger:
    _lock = threading.Lock()
    # ANSI colors
    DATE_COLOR = "\033[94m"  # bright blue
    RESET = "\033[0m"
    LEVEL_COLORS = {
        "DEBUG": "\033[90m",  # grey
        "INFO": "\033[92m",  # green
        "WARNING": "\033[93m",  # yellow
        "ERROR": "\033[91m",  # red
        "CRITICAL": "\033[95m",  # magenta
    }

    @staticmethod
    def log(level, msg, **kwargs):
        with Logger._lock:
            now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            colored_time = f"{Logger.DATE_COLOR}{now}{Logger.RESET}"
            color = Logger.LEVEL_COLORS.get(level.upper(), "")
            extra = " ".join(f"{k}={v}" for k, v in kwargs.items())
            print(
                f"{color}[{level.upper()}]{Logger.RESET} {colored_time} {msg} {extra}",
                file=sys.stdout,
            )
