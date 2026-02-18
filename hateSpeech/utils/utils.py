import logging
import socket
import subprocess

import pkg_resources

from symspellpy import SymSpell
import symspellpy

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
    

class SpellCorrectionModel:
    def __init__(
            self,
            max_dictionary_edit_distance: int = 2,
            prefix_length: int = 7,
            count_threshold: int = 1,

            )-> None:
        
        self.max_dictionary_edit_distance = max_dictionary_edit_distance

        self.model = self._initialize_model(prefix_length, count_threshold)

    def _initialize_model(self, prefix_length: int, count_threshold: int) -> symspellpy.symspellpy.SymSpell:
        model = SymSpell(max_dictionary_edit_distance=self.max_dictionary_edit_distance,
                         prefix_length=prefix_length, 
                         count_threshold=count_threshold)
        dictionary_path = pkg_resources.resource_filename("symspellpy", "frequency_dictionary_en_82_765.txt")
        bigram_dictionary_path = pkg_resources.resource_filename("symspellpy", "frequency_bigramdictionary_en_243_342.txt")
        
        model.load_dictionary(dictionary_path, term_index=0, count_index=1)
        model.load_bigram_dictionary(bigram_dictionary_path, term_index=0, count_index=2)
        return model
    
    def __call__(self, text: str) -> str:
        return self.model.lookup_compound(text, max_edit_distance=self.max_dictionary_edit_distance)[0].term