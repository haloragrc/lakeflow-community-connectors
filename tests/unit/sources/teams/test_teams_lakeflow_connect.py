from databricks.labs.community_connector.sources.teams.teams import TeamsLakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestTeamsConnector(LakeflowConnectTests):
    connector_class = TeamsLakeflowConnect
    simulator_source = "teams"
    replay_config = {
        "access_token": "simulator-fake-graph-token",
        "base_url": "https://graph.microsoft.com",
    }
