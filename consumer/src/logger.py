import datetime
import sys
import asyncio
import traceback


class AsyncLogger:
    DATE_COLOR = "\033[94m"  # bright blue
    RESET = "\033[0m"
    LEVEL_COLORS = {
        "DEBUG": "\033[90m",  # grey
        "INFO": "\033[92m",  # green
        "WARNING": "\033[93m",  # yellow
        "ERROR": "\033[91m",  # red
        "CRITICAL": "\033[95m",  # magenta
    }

    def __init__(self):
        self.queue = asyncio.Queue()
        self.task = None
        self._running = False

    async def _worker(self):
        while self._running:
            try:
                level, msg, kwargs, exc_info = await self.queue.get()
                now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                colored_time = f"{self.DATE_COLOR}{now}{self.RESET}"
                color = self.LEVEL_COLORS.get(level.upper(), "")
                extra = " ".join(f"{k}={v}" for k, v in kwargs.items())
                if exc_info:
                    # Format traceback
                    exc_text = "".join(traceback.format_exception(*exc_info))
                    msg = f"{msg}\n{exc_text}"

                print(
                    f"{color}[{level.upper()}]{self.RESET} {colored_time} {msg} {extra}",
                    file=sys.stdout,
                )
                self.queue.task_done()
            except Exception as e:
                print(f"[LOGGER ERROR] {e}", file=sys.stderr)

    def start(self):
        if not self._running:
            self._running = True
            self.task = asyncio.create_task(self._worker())

    async def stop(self):
        self._running = False
        if self.task:
            await self.queue.join()
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass

    async def log(self, level: str, msg: str, exc_info=False, **kwargs):
        info = None
        if exc_info:
            info = sys.exc_info()
        await self.queue.put((level, msg, kwargs, info))


logger = AsyncLogger()
