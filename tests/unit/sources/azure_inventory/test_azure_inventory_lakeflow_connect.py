from databricks.labs.community_connector.sources.azure_inventory.azure_inventory import (
    AzureInventoryLakeflowConnect,
)
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestAzureInventoryConnector(LakeflowConnectTests):
    connector_class = AzureInventoryLakeflowConnect
    simulator_source = "azure_inventory"
    replay_config = {
        "tenant_id": "simulator-tenant-id",
        "client_id": "simulator-client-id",
        "client_secret": "simulator-client-secret",
        "subscription_id": "11111111-2222-3333-4444-555555555555",
    }
