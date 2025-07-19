# logging_config.py
import logging


def setup_logger():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[logging.StreamHandler()],
    )

    app_logger = logging.getLogger("app")
    app_logger.setLevel(logging.DEBUG)

    return app_logger
