from databricks.labs.community_connector.sources.tenable_vm.tenable_vm import (
    TenableVMLakeflowConnect,
)
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestTenableVMConnector(LakeflowConnectTests):
    connector_class = TenableVMLakeflowConnect
    simulator_source = "tenable_vm"
    replay_config = {
        "access_key": "simulator-fake-access-key",
        "secret_key": "simulator-fake-secret-key",
        "base_url": "https://cloud.tenable.com",
        "poll_interval_seconds": "0",
    }
