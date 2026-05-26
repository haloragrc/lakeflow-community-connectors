from databricks.labs.community_connector.sources.wiz.wiz import WizLakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestWizConnector(LakeflowConnectTests):
    connector_class = WizLakeflowConnect
    simulator_source = "wiz"
    replay_config = {
        "client_id": "simulator-fake-wiz-client-id",
        "client_secret": "simulator-fake-wiz-client-secret",
        "base_url": "https://api.wiz.io",
        "token_url": "https://auth.app.wiz.io/oauth/token",
        "audience": "wiz-api",
    }
