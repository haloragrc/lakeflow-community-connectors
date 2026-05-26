from databricks.labs.community_connector.sources.burp_enterprise.burp_enterprise import (
    BurpEnterpriseLakeflowConnect,
)
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestBurpEnterpriseConnector(LakeflowConnectTests):
    connector_class = BurpEnterpriseLakeflowConnect
    simulator_source = "burp_enterprise"
    replay_config = {
        "api_token": "simulator-fake-burp-api-token",
        "base_url": "https://burp-dast.example.local",
    }
