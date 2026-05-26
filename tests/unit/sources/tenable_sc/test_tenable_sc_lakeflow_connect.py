from databricks.labs.community_connector.sources.tenable_sc.tenable_sc import (
    TenableScLakeflowConnect,
)
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestTenableScConnector(LakeflowConnectTests):
    connector_class = TenableScLakeflowConnect
    simulator_source = "tenable_sc"
    replay_config = {
        "access_key": "simulator-fake-access-key",
        "secret_key": "simulator-fake-secret-key",
        "base_url": "https://tenable-sc.example.local",
    }
