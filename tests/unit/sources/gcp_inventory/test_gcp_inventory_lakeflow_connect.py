from databricks.labs.community_connector.sources.gcp_inventory.gcp_inventory import (
    GcpInventoryLakeflowConnect,
)
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestGcpInventoryConnector(LakeflowConnectTests):
    connector_class = GcpInventoryLakeflowConnect
    simulator_source = "gcp_inventory"
    replay_config = {
        "service_account_email": "simulator-sa@example.invalid",
        "service_account_key": "simulator-fake-private-key",
        "project_id": "sim-sec-prod-001",
    }
