import sys
import logging
import aws_utils
from amazon_kclpy import kcl
from record_processor import RecordProcessor

aws_utils.configure_logging()
logger = logging.getLogger(__name__)


def main():
    try:
        kcl_process = kcl.KCLProcess(RecordProcessor())
        kcl_process.run()
    except Exception as e:
        logger.error(f"KCL Process failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
