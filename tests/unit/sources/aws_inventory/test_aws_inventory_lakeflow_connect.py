from databricks.labs.community_connector.sources.aws_inventory.aws_inventory import (
    AwsInventoryLakeflowConnect,
)
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestAwsInventoryConnector(LakeflowConnectTests):
    connector_class = AwsInventoryLakeflowConnect
    simulator_source = "aws_inventory"
    replay_config = {
        "access_key_id": "simulator-fake-access-key",
        "secret_access_key": "simulator-fake-secret-key",
        "session_token": "simulator-fake-session-token",
        "account_id": "111122223333",
    }
