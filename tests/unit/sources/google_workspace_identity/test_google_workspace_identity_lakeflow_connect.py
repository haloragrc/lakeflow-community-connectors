from databricks.labs.community_connector.sources.google_workspace_identity.google_workspace_identity import (
    GoogleWorkspaceIdentityLakeflowConnect,
)
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestGoogleWorkspaceIdentityConnector(LakeflowConnectTests):
    connector_class = GoogleWorkspaceIdentityLakeflowConnect
    simulator_source = "google_workspace_identity"
    replay_config = {
        "access_token": "simulator-fake-google-admin-token",
        "customer": "my_customer",
        "base_url": "https://admin.googleapis.com",
    }
