from databricks.labs.community_connector.sources.sentinel_bounded.sentinel_bounded import (
    SentinelBoundedLakeflowConnect,
)
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestSentinelBoundedConnector(LakeflowConnectTests):
    connector_class = SentinelBoundedLakeflowConnect
    simulator_source = "sentinel_bounded"
    replay_config = {
        "access_token": "simulator-fake-azure-token",
        "subscription_id": "sub-123",
        "resource_group": "rg-sec",
        "workspace_name": "ws-sentinel",
        "base_url": "https://management.azure.com",
    }
