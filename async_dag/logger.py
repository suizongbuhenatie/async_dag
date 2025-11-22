import logging

try:
    import colorlog
except Exception:
    colorlog = None


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    if colorlog is not None:
        logger = colorlog.getLogger(name)
        if logger.hasHandlers():
            return logger
        handler = colorlog.StreamHandler()
        handler.setFormatter(
            colorlog.ColoredFormatter(
                "%(log_color)s%(levelname).1s %(asctime)s %(name)s: %(message)s",
                datefmt="%m-%d %H:%M:%S",
                log_colors={
                    "DEBUG": "cyan",
                    "INFO": "green",
                    "WARNING": "yellow",
                    "ERROR": "red",
                    "CRITICAL": "bold_red",
                },
            )
        )
        logger.addHandler(handler)
        logger.setLevel(level)
        return logger
    logger = logging.getLogger(name)
    if logger.hasHandlers():
        return logger
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(levelname).1s %(asctime)s %(name)s: %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(level)
    return logger


logger = get_logger("async_dag")
