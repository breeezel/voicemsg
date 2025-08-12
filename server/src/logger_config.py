import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path


def setup_logging(
    app_name: str = "voip_server",
    log_dir: Path | None = None,
    include_console: bool = True,
    level: int = logging.INFO,
) -> logging.Logger:
    """
    Configure and return a logger with a rotating file handler and optional console output.

    Windows-friendly. Uses pathlib for path manipulation.
    """
    logger = logging.getLogger(app_name)
    if logger.handlers:
        return logger

    logger.setLevel(level)

    # Resolve log directory
    if log_dir is None:
        project_root = Path(__file__).resolve().parents[2]
        log_dir = project_root / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    log_file = log_dir / f"{app_name}.log"

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # File handler with rotation
    file_handler = RotatingFileHandler(
        filename=str(log_file), maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(level)

    logger.addHandler(file_handler)

    if include_console:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        console_handler.setLevel(level)
        logger.addHandler(console_handler)

    logger.info("Logging initialized. Log file: %s", log_file)
    return logger


