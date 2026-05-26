from databricks.labs.community_connector.sources.knowbe4.knowbe4 import Knowbe4LakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestKnowbe4Connector(LakeflowConnectTests):
    connector_class = Knowbe4LakeflowConnect
    simulator_source = "knowbe4"
    replay_config = {
        "api_key": "simulator-fake-knowbe4-key",
        "base_url": "https://us.api.knowbe4.com/v1",
    }
