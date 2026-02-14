import logging
import socket
import subprocess

import symspellpy

from symspellpy import SymSpell


logger = logging.getLogger(__name__)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(f"[{socket.gethostname()}] {name}")


def run_shell_command(cmd: str) -> str:
    """
    Run a shell command and return its output.

    Args:
        cmd (str): The shell command to run.

    Returns:
        str: The stdout output of the command.

    Raises:
        subprocess.CalledProcessError: If the command fails.
    """
    try:
        result = subprocess.run(cmd, text=True, shell=True, check=True, capture_output=True)
        return result.stdout
    except subprocess.CalledProcessError as e:
        raise Exception(f"Command failed: {e.stderr}") from e