from databricks.labs.community_connector.sources.crowdstrike.crowdstrike import (
    CrowdStrikeLakeflowConnect,
)
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestCrowdStrikeConnector(LakeflowConnectTests):
    connector_class = CrowdStrikeLakeflowConnect
    simulator_source = "crowdstrike"
    replay_config = {
        "client_id": "simulator-fake-crowdstrike-client-id",
        "client_secret": "simulator-fake-crowdstrike-client-secret",
        "base_url": "https://api.crowdstrike.com",
        "token_url": "https://api.crowdstrike.com/oauth2/token",
    }
