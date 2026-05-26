from databricks.labs.community_connector.sources.semgrep.semgrep import SemgrepLakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestSemgrepConnector(LakeflowConnectTests):
    connector_class = SemgrepLakeflowConnect
    simulator_source = "semgrep"
    replay_config = {
        "api_token": "simulator-fake-semgrep-token",
        "deployment_slug": "simulator-deployment",
        "base_url": "https://semgrep.dev",
    }
