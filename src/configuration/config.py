from pathlib import Path

ROOT_DIR = Path(__file__).parent.parent.parent

DATA_DIR = ROOT_DIR / "data"
LOG_DIR = ROOT_DIR / "logs"
CHECKPOINT_DIR = ROOT_DIR / "checkpoints"
WEB_STATIC_DIR = ROOT_DIR / "src" / "web" / "static"

MODEL_NAME = "google-bert/bert-base-chinese"

LABELS = ['B', 'I', 'O']


