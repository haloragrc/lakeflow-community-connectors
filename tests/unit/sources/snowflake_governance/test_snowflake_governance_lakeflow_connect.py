from databricks.labs.community_connector.sources.snowflake_governance.snowflake_governance import (
    SnowflakeGovernanceLakeflowConnect,
)
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestSnowflakeGovernanceConnector(LakeflowConnectTests):
    connector_class = SnowflakeGovernanceLakeflowConnect
    simulator_source = "snowflake_governance"
    replay_config = {
        "account_identifier": "sim-acct",
        "user": "sim-user",
        "password": "sim-password",
    }
