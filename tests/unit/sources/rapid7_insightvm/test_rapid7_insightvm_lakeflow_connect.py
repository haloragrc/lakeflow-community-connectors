from databricks.labs.community_connector.sources.rapid7_insightvm.rapid7_insightvm import (
    Rapid7InsightVmLakeflowConnect,
)
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestRapid7InsightVmConnector(LakeflowConnectTests):
    connector_class = Rapid7InsightVmLakeflowConnect
    simulator_source = "rapid7_insightvm"
    replay_config = {
        "api_key": "simulator-fake-rapid7-api-key",
        "base_url": "https://example.insightvm.local",
    }
