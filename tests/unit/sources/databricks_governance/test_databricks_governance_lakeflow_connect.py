from databricks.labs.community_connector.sources.databricks_governance.databricks_governance import (
    DatabricksGovernanceLakeflowConnect,
)
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestDatabricksGovernanceConnector(LakeflowConnectTests):
    connector_class = DatabricksGovernanceLakeflowConnect
    simulator_source = "databricks_governance"
    replay_config = {
        "account_id": "sim-account-id",
        "client_id": "sim-client-id",
        "client_secret": "sim-client-secret",
        "workspace_id": "9001",
    }
