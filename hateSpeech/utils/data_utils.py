from shutil import rmtree

from hateSpeech.utils.utils import run_shell_command


def get_cmd_to_get_raw_data(
        version: str,
        data_local_save_dir: str,
        dvc_remote_repo: str,
        dvc_data_folder:str,
        github_user_name: str,
        github_access_token: str
)-> str:
    """
    Get the command to get the raw data from the dvc remote repository.

    Args:
        version (str): The version of the data to get. This should be a git tag or branch name in the dvc remote repository.
        data_local_save_dir (str): The local directory where the data should be saved.
        dvc_remote_repo (str): The URL of the dvc remote repository where the data is stored.
        dvc_data_folder (str): The folder in the dvc remote repository where the data is stored.
        github_user_name (str): The GitHub username to use for authentication when accessing the dvc remote repository.
        github_access_token (str): The GitHub access token to use for authentication when accessing the dvc remote repository.

    Returns:
        str: The command to get the raw data from the dvc remote repository.
    """
    without_https = dvc_remote_repo.replace("https://", "")
    dvc_remote_repo = f"https://{github_user_name}:{github_access_token}@{without_https}"
    command = f"dvc get {dvc_remote_repo} {dvc_data_folder} -o {data_local_save_dir} --rev {version}"
    return command


def get_raw_data_with_version(
        version: str,
        data_local_save_dir: str,
        dvc_remote_repo: str,
        dvc_data_folder:str,
        github_user_name: str,
        github_access_token: str
)-> None:
    """
    Get the raw data from the dvc remote repository.

    Args:
        version (str): The version of the data to get. This should be a git tag or branch name in the dvc remote repository.
        data_local_save_dir (str): The local directory where the data should be saved.
        dvc_remote_repo (str): The URL of the dvc remote repository where the data is stored.
        dvc_data_folder (str): The folder in the dvc remote repository where the data is stored.
        github_user_name (str): The GitHub username to use for authentication when accessing the dvc remote repository.
        github_access_token (str): The GitHub access token to use for authentication when accessing the dvc remote repository.

    Returns:
        None
    """
    rmtree(data_local_save_dir, ignore_errors=True)
    command = get_cmd_to_get_raw_data(
        version=version,
        data_local_save_dir=data_local_save_dir,
        dvc_remote_repo=dvc_remote_repo,
        dvc_data_folder=dvc_data_folder,
        github_user_name=github_user_name,
        github_access_token=github_access_token
    )
    run_shell_command(command)

    
    