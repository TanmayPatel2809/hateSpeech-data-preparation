from abc import ABC, abstractmethod
import os

from typing import Optional
from hateSpeech.utils.data_utils import repartition_dataframe, get_repo_address_with_access_token
from hateSpeech.utils.utils import get_logger
import dask.dataframe as dd
from dask_ml.model_selection import train_test_split
from dvc.api import get_url

class DatasetReader(ABC):
    required_columns = {"text", "label", "split", "dataset_name"}
    split_names = {"train", "dev", "test"}


    def __init__(self, dataset_dir: str, dataset_name: str, gcp_project_id: str, gcp_github_access_token_secret_id: str,
                 dvc_remote_repo: str, github_user_name: str, version: str) -> None:
        self.logger = get_logger(self.__class__.__name__)
        self.dataset_dir = dataset_dir
        self.dataset_name = dataset_name
        self.dvc_remote_repo = get_repo_address_with_access_token(gcp_project_id, gcp_github_access_token_secret_id, dvc_remote_repo, github_user_name)
        self.version = version

    def read_data(self) -> dd.core.DataFrame:
        self.logger.info(f"Reading {self.__class__.__name__}")
        train_df , dev_df , test_df = self._read_data()
        df = self.assign_split_names_to_data_frames_and_merge(train_df, dev_df, test_df)
        df["dataset_name"] = self.dataset_name


        if any(required_column not in df.columns.values for required_column in self.required_columns):
            raise ValueError(f"Dataframe is missing required columns. Required columns are: {self.required_columns}. Dataframe columns are: {df.columns.values}")
        unique_split_names_in_df = set(df["split"].unique())
        if not unique_split_names_in_df.issubset(self.split_names):
            raise ValueError(f"Dataframe contains invalid split names. Valid split names are: {self.split_names}. Split names in dataframe are: {unique_split_names_in_df}")
        return df[list(self.required_columns)]

    @abstractmethod
    def _read_data(self) -> tuple[dd.core.DataFrame, dd.core.DataFrame, dd.core.DataFrame]:
        """
        Reads the data from the source and returns the train, dev and test dataframes.
        """

    def assign_split_names_to_data_frames_and_merge(self, train_df: dd.core.DataFrame, dev_df: dd.core.DataFrame, test_df: dd.core.DataFrame) -> dd.core.DataFrame:
        """
        Assigns the split names to the dataframes and merges them into a single dataframe.
        """
        train_df["split"] = "train"
        dev_df["split"] = "dev"
        test_df["split"] = "test"

        return dd.concat([train_df, dev_df, test_df])
    
    def split_dataset(self, df: dd.core.DataFrame, test_size: float, stratify_columns: Optional[str] = None) -> tuple[dd.core.DataFrame, dd.core.DataFrame]:
        if stratify_columns is None:
            return train_test_split(df, test_size=test_size, random_state=1234, shuffle=True)
        unique_columns_values = df[stratify_columns].unique()
        first_dfs = []
        second_dfs = []
        for unique_column_value in unique_columns_values:
            sub_df = df[df[stratify_columns] == unique_column_value]
            sub_first_df, sub_second_df = train_test_split(sub_df, test_size=test_size, random_state=1234, shuffle=True)
            first_dfs.append(sub_first_df)
            second_dfs.append(sub_second_df)

        first_df = dd.concat(first_dfs)
        second_df = dd.concat(second_dfs)
        return first_df, second_df

    def get_remote_data_url(self, dataset_path: str) -> str:
        return get_url(path=dataset_path, repo=self.dvc_remote_repo, rev=self.version )
    
class GHCDatasetReader(DatasetReader):
    def __init__(self, dataset_dir: str, dataset_name: str, dev_split_ratio: float,
                 gcp_project_id: str, gcp_github_access_token_secret_id: str, dvc_remote_repo: str, github_user_name: str, version: str)->None:
        super().__init__(dataset_dir, dataset_name, gcp_project_id, gcp_github_access_token_secret_id, dvc_remote_repo, github_user_name, version)
        self.dev_split_ratio = dev_split_ratio

    def _read_data(self) -> tuple[dd.core.DataFrame, dd.core.DataFrame, dd.core.DataFrame]:
        train_tsv_path = os.path.join(self.dataset_dir, "ghc_train.tsv")
        train_tsv_url = self.get_remote_data_url(train_tsv_path)
        test_tsv_path = os.path.join(self.dataset_dir, "ghc_test.tsv")
        test_tsv_url = self.get_remote_data_url(test_tsv_path)

        train_df = dd.read_csv(train_tsv_url, sep="\t", header=0)
        test_df = dd.read_csv(test_tsv_url, sep="\t", header=0)

        train_df["label"] = (train_df["hd"] + train_df["cv"]+ train_df["vo"] > 0).astype(int)
        test_df["label"] = (test_df["hd"] + test_df["cv"]+ test_df["vo"] > 0).astype(int)

        train_df, dev_df = self.split_dataset(train_df, test_size=self.dev_split_ratio, stratify_columns="label")

        return train_df, dev_df, test_df
    

class JigsawToxicCommentsDatasetReader(DatasetReader):
    def __init__(self, dataset_dir: str, dataset_name: str, dev_split_ratio: float,
                 gcp_project_id: str, gcp_github_access_token_secret_id: str, dvc_remote_repo: str, github_user_name: str, version: str)->None:
        super().__init__(dataset_dir, dataset_name, gcp_project_id, gcp_github_access_token_secret_id, dvc_remote_repo, github_user_name, version)
        self.dev_split_ratio = dev_split_ratio
        self.columns_for_label = ["toxic", "severe_toxic", "obscene", "threat", "insult", "identity_hate"]  

    def _read_data(self) -> tuple[dd.core.DataFrame, dd.core.DataFrame, dd.core.DataFrame]:

        test_csv_path = os.path.join(self.dataset_dir, "test.csv")
        test_csv_url = self.get_remote_data_url(test_csv_path)
        test_df = dd.read_csv(test_csv_url)

        test_labels_csv_path = os.path.join(self.dataset_dir, "test_labels.csv")
        test_labels_csv_url = self.get_remote_data_url(test_labels_csv_path)
        test_labels_df = dd.read_csv(test_labels_csv_url)

        test_df = test_df.merge(test_labels_df, on=["id"])
        test_df = test_df[test_df["toxic"] != -1]

        test_df = self.get_text_and_label_columns(test_df)
        to_train_df, test_df = self.split_dataset(test_df,0.1, stratify_columns="label")

        train_csv_path = os.path.join(self.dataset_dir, "train.csv")
        train_csv_url = self.get_remote_data_url(train_csv_path)
        train_df = dd.read_csv(train_csv_url)
        train_df = self.get_text_and_label_columns(train_df)
        train_df = dd.concat([train_df, to_train_df])

        train_df, dev_df = self.split_dataset(train_df, test_size=self.dev_split_ratio, stratify_columns="label")
        return train_df, dev_df, test_df

    def get_text_and_label_columns(self, df: dd.core.DataFrame) -> dd.core.DataFrame:
        df = df.rename(columns={"comment_text": "text"})
        df["label"] = (df[self.columns_for_label].sum(axis=1) > 0).astype(int)
        return df

class TwitterDatasetReader(DatasetReader):
    def __init__(self,dataset_dir: str, dataset_name: str, dev_split_ratio: float, test_split_ratio: float,
                 gcp_project_id: str, gcp_github_access_token_secret_id: str, dvc_remote_repo: str, github_user_name: str, version: str)->None:
        super().__init__(dataset_dir, dataset_name, gcp_project_id, gcp_github_access_token_secret_id, dvc_remote_repo, github_user_name, version)
        self.dev_split_ratio = dev_split_ratio
        self.test_split_ratio = test_split_ratio
        
    def _read_data(self) -> tuple[dd.core.DataFrame, dd.core.DataFrame, dd.core.DataFrame]:

        train_csv_path = os.path.join(self.dataset_dir, "cyberbullying_tweets.csv")
        train_csv_url = self.get_remote_data_url(train_csv_path)
        df = dd.read_csv(train_csv_url)
        df = df.rename(columns={"tweet_text": "text", "cyberbullying_type": "label"})
        df["label"] = (df["label"] != "not_cyberbullying").astype(int)

        train_df, test_df = self.split_dataset(df, test_size=self.test_split_ratio, stratify_columns="label")
        train_df, dev_df = self.split_dataset(train_df, test_size=self.dev_split_ratio, stratify_columns="label")

        return train_df, dev_df, test_df


class DatasetReaderManager:
    def __init__(self,dataset_readers: dict[str, DatasetReader] , repartition: bool = True, available_memory: Optional[float] = None) -> None:
        self.dataset_readers = dataset_readers
        self.repartition = repartition
        self.available_memory = available_memory

    def read_data(self, nrof_workers: int) -> dd.core.DataFrame:
        dfs = [dataset_reader.read_data() for dataset_reader in self.dataset_readers.values()]
        df = dd.concat(dfs)

        if self.repartition:
            df = repartition_dataframe(df, nrof_workers=nrof_workers, available_memory=self.available_memory)
        return df