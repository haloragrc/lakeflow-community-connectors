from databricks.labs.community_connector.sources.gitlab.gitlab import GitlabLakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestGitlabConnector(LakeflowConnectTests):
    connector_class = GitlabLakeflowConnect
    simulator_source = "gitlab"
    replay_config = {
        "private_token": "simulator-fake-gitlab-token",
        "base_url": "https://gitlab.example.com",
    }
