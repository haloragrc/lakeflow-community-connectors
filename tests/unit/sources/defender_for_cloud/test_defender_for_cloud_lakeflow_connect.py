from databricks.labs.community_connector.sources.defender_for_cloud.defender_for_cloud import (
    DefenderForCloudLakeflowConnect,
)
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestDefenderForCloudConnector(LakeflowConnectTests):
    connector_class = DefenderForCloudLakeflowConnect
    simulator_source = "defender_for_cloud"
    replay_config = {
        "access_token": "simulator-fake-azure-token",
        "subscription_id": "sub-123",
        "base_url": "https://management.azure.com",
    }
