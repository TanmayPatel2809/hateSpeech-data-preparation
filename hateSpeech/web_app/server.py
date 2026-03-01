from hateSpeech.utils.config_utils import load_pickle_config
from hydra.utils import instantiate
from fastapi import FastAPI

config = load_pickle_config(config_path="./hateSpeech/configs/automatically_generated", config_name="data_processing_config")

dataset_cleaner_manager = instantiate(config.dataset_cleaner_manager)

app = FastAPI()

@app.post("/process_data")
def process_data_(text: str)-> dict[str,str]:
    return {"cleaned_text": dataset_cleaner_manager(text)}