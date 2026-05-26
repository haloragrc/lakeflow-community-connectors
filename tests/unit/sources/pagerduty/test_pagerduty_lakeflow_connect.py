from databricks.labs.community_connector.sources.pagerduty.pagerduty import PagerdutyLakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestPagerdutyConnector(LakeflowConnectTests):
    connector_class = PagerdutyLakeflowConnect
    simulator_source = "pagerduty"
    replay_config = {
        "api_token": "simulator-fake-pagerduty-token",
        "base_url": "https://api.pagerduty.com",
    }
