import colorama
import logging
import sys

# Initialize colorama for cross-platform ANSI support
colorama.init(autoreset=True)


class ColoredFormatter(logging.Formatter):
    """Custom formatter to inject ANSI colors into specific log levels."""
    COLORS = {
        'INFO': colorama.Fore.GREEN + '[INFO]' + colorama.Style.RESET_ALL,
        'WARNING': colorama.Fore.YELLOW + '[WARN]' + colorama.Style.RESET_ALL,
        'ERROR': colorama.Fore.RED + '[ERROR]' + colorama.Style.RESET_ALL,
        'CRITICAL': colorama.Back.RED + colorama.Fore.WHITE + '[CRITICAL]' + colorama.Style.RESET_ALL
    }

    def format(self, record):
        levelname = record.levelname
        status_tag = self.COLORS.get(levelname, f"[{levelname}]")
        log_fmt = f"%(asctime)s {status_tag} [%(node)s] %(message)s"
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


class PktGenLogger:
    def __init__(self, level=logging.INFO):
        self.logger = logging.getLogger("PktGenLogger")
        self.logger.setLevel(level)
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(ColoredFormatter())
        self.logger.addHandler(handler)

    def info(text):
        self.logger.info(f"\n[INFO] {text}")

    def critical(text):
        self.logger.critical(f"\n[CRITICAL] {text}")
    
    def error(text):
        self.logger.errpr(f"\n[ERROR] {text}")

    def warn(text):
        self.logger.warning(f"\n[WARNING] {text}")
