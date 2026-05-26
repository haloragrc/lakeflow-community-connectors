from databricks.labs.community_connector.sources.okta_identity.okta_identity import (
    OktaIdentityLakeflowConnect,
)
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestOktaIdentityConnector(LakeflowConnectTests):
    connector_class = OktaIdentityLakeflowConnect
    simulator_source = "okta_identity"
    replay_config = {
        "api_token": "simulator-fake-okta-token",
        "base_url": "https://example.okta.com",
    }
