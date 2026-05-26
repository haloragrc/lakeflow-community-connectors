from databricks.labs.community_connector.sources.google_workspace_drive.google_workspace_drive import (
    GoogleWorkspaceDriveLakeflowConnect,
)
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestGoogleWorkspaceDriveConnector(LakeflowConnectTests):
    connector_class = GoogleWorkspaceDriveLakeflowConnect
    simulator_source = "google_workspace_drive"
    replay_config = {
        "access_token": "simulator-fake-google-drive-token",
        "base_url": "https://www.googleapis.com",
    }
