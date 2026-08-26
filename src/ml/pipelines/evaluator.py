import json
from ml.pipelines.settings import EVALUATION_REPORT_PATH
from ml.pipelines.logger import get_logger

logger = get_logger("evaluator")


def load_evaluation_report() -> dict:
    try:
        with open(EVALUATION_REPORT_PATH) as f:
            return json.load(f)
    except FileNotFoundError:
        logger.warning("No evaluation report found")
        return {"status": "no_model"}
