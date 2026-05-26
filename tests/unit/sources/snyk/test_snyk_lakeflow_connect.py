from databricks.labs.community_connector.sources.snyk.snyk import SnykLakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestSnykConnector(LakeflowConnectTests):
    connector_class = SnykLakeflowConnect
    simulator_source = "snyk"
    replay_config = {
        "api_token": "simulator-fake-snyk-token",
        "org_id": "00000000-0000-0000-0000-000000000111",
        "base_url": "https://api.snyk.io/rest",
        "api_version": "2025-11-05",
    }
