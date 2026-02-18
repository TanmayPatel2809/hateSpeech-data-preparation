from pydantic.dataclasses import dataclass
from hydra.core.config_store import ConfigStore


@dataclass
class GCPConfig:
    project_id: str = "gen-lang-client-0958213549"
    zone: str = "europe-west4-b"
    network: str = "default"

def setup_config() -> None:
    cs = ConfigStore.instance()
    cs.store(name="gcp_config_schema", node=GCPConfig)
    