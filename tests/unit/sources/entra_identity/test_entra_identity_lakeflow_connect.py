from databricks.labs.community_connector.sources.entra_identity.entra_identity import (
    EntraIdentityLakeflowConnect,
)
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestEntraIdentityConnector(LakeflowConnectTests):
    connector_class = EntraIdentityLakeflowConnect
    simulator_source = "entra_identity"
    replay_config = {
        "access_token": "simulator-fake-graph-token",
        "base_url": "https://graph.microsoft.com",
    }
