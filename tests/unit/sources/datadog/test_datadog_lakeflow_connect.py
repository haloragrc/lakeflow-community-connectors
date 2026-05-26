from databricks.labs.community_connector.sources.datadog.datadog import DatadogLakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestDatadogConnector(LakeflowConnectTests):
    connector_class = DatadogLakeflowConnect
    simulator_source = "datadog"
    replay_config = {
        "api_key": "simulator-fake-datadog-api-key",
        "app_key": "simulator-fake-datadog-app-key",
        "base_url": "https://api.datadoghq.com",
    }
