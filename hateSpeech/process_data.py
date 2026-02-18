from hateSpeech.config_schemas.data_processing_config_schema import DataProcessingConfig
from hateSpeech.data_processing.data_cleaners import DatasetCleanerManager
from hateSpeech.utils.config_utils import custom_instantiate, get_pickle_config
from hateSpeech.utils.data_utils import filter_based_on_minimum_number_of_words, get_raw_data_with_version
from hateSpeech.utils.gcp_utils import access_secret_version
from hydra.utils import instantiate
from omegaconf import OmegaConf
from dask.distributed import Client
from pathlib import Path
import dask.dataframe as dd
import os

from hateSpeech.utils.io_utils import write_yaml_file
from hateSpeech.utils.utils import get_logger


def process_raw_data(df_partition: dd.core.DataFrame, dataset_cleaner_manager: DatasetCleanerManager)-> dd.core.Series:
    return df_partition["text"].apply(dataset_cleaner_manager)
    

@get_pickle_config(config_path="hateSpeech/configs/automatically_generated", config_name="data_processing_config")
def process_data(config: DataProcessingConfig) -> None:
    # print(OmegaConf.to_yaml(config, resolve=True))
    # print(config)
    # return

    logger = get_logger(Path(__file__).name)
    logger.info("Processing raw data...")

    processed_data_save_dir = config.processed_data_save_dir

    cluster = custom_instantiate(config.dask_cluster)
    client = Client(cluster)
    
    try:
        # github_access_token = access_secret_version(config.infrastructure.project_id, config.github_access_token_secret_id)
        # get_raw_data_with_version(
        #     version=config.version,
        #     data_local_save_dir=config.data_local_save_dir,
        #     dvc_remote_repo=config.dvc_remote_repo,
        #     dvc_data_folder=config.dvc_data_folder,
        #     github_user_name=config.github_user_name,
        #     github_access_token=github_access_token
        # )
        

        dataset_reader_manager = instantiate(config.dataset_reader_manager)
        dataset_cleaner_manager = instantiate(config.dataset_cleaner_manager)

        df = dataset_reader_manager.read_data(config.dask_cluster.n_workers)

        # print(df.compute().head())
        # exit(0)

        # print(60*"-")
        # print(df.npartitions)
        # print(60*"-")

        logger.info("Cleaning data...")
        df = df.assign(cleaned_text= df.map_partitions(process_raw_data, dataset_cleaner_manager=dataset_cleaner_manager, meta=("text", "object")))
        df = df.compute()



        train_parquet_path = os.path.join(processed_data_save_dir, "train.parquet")
        dev_parquet_path = os.path.join(processed_data_save_dir, "dev.parquet")
        test_parquet_path = os.path.join(processed_data_save_dir, "test.parquet")


        train_df = df[df["split"] == "train"]
        test_df = df[df["split"] == "test"]
        dev_df = df[df["split"] == "dev"]

        train_df = filter_based_on_minimum_number_of_words(train_df, min_nrof_words=config.min_nrof_words)
        dev_df = filter_based_on_minimum_number_of_words(dev_df, min_nrof_words=config.min_nrof_words)
        test_df = filter_based_on_minimum_number_of_words(test_df, min_nrof_words=config.min_nrof_words)

        train_df.to_parquet(train_parquet_path)
        dev_df.to_parquet(dev_parquet_path)
        test_df.to_parquet(test_parquet_path)

        docker_info = {"docker_image": config.docker_image_name, "docker_tag": config.docker_image_tag}
        docker_info_save_path = os.path.join(processed_data_save_dir, "docker_info.yaml")

        write_yaml_file(docker_info_save_path, docker_info)


        logger.info("Data processing completed successfully.")


    finally:
        logger.info("Closing Dask client and cluster...")

        client.close()
        cluster.close()

if __name__ == "__main__":
    process_data()
