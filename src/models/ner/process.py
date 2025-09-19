from dotenv import load_dotenv

load_dotenv()
from datasets import load_dataset

from transformers import AutoTokenizer

from configuration import config

def process():
    dataset = load_dataset('json', data_files=str(config.DATA_DIR / 'ner' / 'raw' / 'data.json'))['train']
    dataset = dataset.remove_columns(['id', 'annotator', 'annotation_id', 'created_at', 'updated_at', 'lead_time'])
    print(dataset)








