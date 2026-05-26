from databricks.labs.community_connector.sources.slack.slack import SlackLakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestSlackConnector(LakeflowConnectTests):
    connector_class = SlackLakeflowConnect
    simulator_source = "slack"
    replay_config = {
        "api_token": "simulator-fake-slack-token",
        "base_url": "https://slack.com/api",
    }
