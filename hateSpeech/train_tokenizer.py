from omegaconf import OmegaConf
from hateSpeech.utils.config_utils import custom_instantiate, get_pickle_config
from hateSpeech.utils.io_utils import write_yaml_file
from hateSpeech.utils.utils import get_logger
from pathlib import Path
from hateSpeech.config_schemas.tokenizer_training_config_schema import TokenizerTrainingConfig
import pandas as pd
from hydra.utils import instantiate
import os

@get_pickle_config(config_path="hateSpeech/configs/automatically_generated", config_name="tokenizer_training_config")
def train_tokenizer(config: TokenizerTrainingConfig) -> None:
    # print(OmegaConf.to_yaml(config, resolve=True))
    # return

    logger = get_logger(Path(__file__).name)

    data_parquet_path = config.data_parquet_path
    text_column_name = config.text_column_name
    tokenizer = instantiate(config.tokenizer, _convert_ = "all")

    logger.info("Reading dataset...")
    df = pd.read_parquet(data_parquet_path)

    logger.info("Training tokenizer...")
    tokenizer.train(df[text_column_name].values)

    logger.info("Saving tokenizer...")
    tokenizer_save_dir = os.path.join(os.path.dirname(data_parquet_path), "trained_tokenizer")

    tokenizer.save(tokenizer_save_dir)

    docker_info = {"docker_image": config.docker_image_name, "docker_tag": config.docker_image_tag}
    docker_info_save_path = os.path.join(tokenizer_save_dir, "tokenizer_training_docker_info.yaml")
    write_yaml_file(docker_info_save_path, docker_info)

    logger.info("Done!")



if __name__ == "__main__":
    train_tokenizer()
