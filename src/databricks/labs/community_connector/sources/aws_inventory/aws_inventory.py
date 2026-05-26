"""AWS inventory and governance API connector."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Callable, Iterator

from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.aws_inventory.aws_inventory_schemas import (
    SUPPORTED_TABLES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
)
from databricks.labs.community_connector.sources.high_value_common import (
    HighValueApiClient,
    as_iso8601,
    first_present,
)


class AwsInventoryLakeflowConnect(LakeflowConnect):
    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        access_key_id = options.get("access_key_id")
        secret_access_key = options.get("secret_access_key")
        if not access_key_id or not secret_access_key:
            raise ValueError("aws_inventory requires access_key_id and secret_access_key")

        self.account_id = options.get("account_id")
        self.page_size = int(options.get("page_size", "200"))
        self.client = HighValueApiClient(
            base_url=options.get("base_url", "https://api.aws.example.com"),
            timeout_seconds=int(options.get("timeout_seconds", "30")),
            headers={
                "Accept": "application/json",
                "X-Aws-Access-Key-Id": access_key_id,
                "X-Aws-Secret-Access-Key": secret_access_key,
                "X-Aws-Session-Token": options.get("session_token", ""),
            },
        )
        self._init_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    def list_tables(self) -> list[str]:
        return SUPPORTED_TABLES.copy()

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        del table_options
        if table_name not in TABLE_SCHEMAS:
            raise ValueError(f"Unsupported table: {table_name!r}")
        return TABLE_SCHEMAS[table_name]

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        del table_options
        if table_name not in TABLE_METADATA:
            raise ValueError(f"Unsupported table: {table_name!r}")
        return TABLE_METADATA[table_name]

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        del table_options
        start_offset = start_offset or {}

        if table_name == "accounts":
            records = self._read_collection("/identity/accounts", "accounts", self._map_account)
            return self._finalize_cdc(records, start_offset, "updated_at")

        if table_name == "iam_users":
            records = self._read_collection("/iam/users", "users", self._map_iam_user)
            return self._finalize_cdc(records, start_offset, "updated_at")

        if table_name == "iam_roles":
            records = self._read_collection("/iam/roles", "roles", self._map_iam_role)
            return self._finalize_cdc(records, start_offset, "updated_at")

        if table_name == "iam_policies":
            records = self._read_collection("/iam/policies", "policies", self._map_iam_policy)
            return self._finalize_cdc(records, start_offset, "updated_at")

        if table_name == "ec2_instances":
            records = self._read_collection("/compute/ec2/instances", "instances", self._map_ec2_instance)
            return self._finalize_cdc(records, start_offset, "updated_at")

        if table_name == "vpcs":
            records = self._read_collection("/network/vpcs", "vpcs", self._map_vpc)
            return self._finalize_cdc(records, start_offset, "updated_at")

        if table_name == "security_groups":
            records = self._read_collection("/network/security-groups", "security_groups", self._map_security_group)
            return self._finalize_cdc(records, start_offset, "updated_at")

        if table_name == "s3_buckets":
            records = self._read_collection("/storage/s3/buckets", "buckets", self._map_s3_bucket)
            return self._finalize_cdc(records, start_offset, "updated_at")

        if table_name == "kms_keys":
            records = self._read_collection("/security/kms/keys", "keys", self._map_kms_key)
            return self._finalize_cdc(records, start_offset, "updated_at")

        if table_name == "cloudtrail_trails":
            records = self._read_collection("/audit/cloudtrail/trails", "trails", self._map_cloudtrail_trail)
            return self._finalize_cdc(records, start_offset, "updated_at")

        if table_name == "config_recorders":
            records = self._read_collection("/posture/config/recorders", "recorders", self._map_config_recorder)
            return self._finalize_cdc(records, start_offset, "updated_at")

        if table_name == "guardduty_findings":
            records = self._read_collection("/security/guardduty/findings", "findings", self._map_guardduty_finding)
            return self._finalize_cdc(records, start_offset, "updated_at")

        raise ValueError(f"Unsupported table: {table_name!r}")

    def _read_collection(
        self,
        path: str,
        records_key: str,
        mapper: Callable[[dict[str, Any]], dict[str, Any]],
    ) -> list[dict[str, Any]]:
        next_token = None
        all_records: list[dict[str, Any]] = []

        for _ in range(100):
            params: dict[str, Any] = {"page_size": self.page_size}
            if next_token:
                params["next_token"] = next_token
            body = self.client.request_json("GET", path, params=params)
            rows = self._extract_rows(body, records_key)
            all_records.extend(mapper(r) for r in rows if isinstance(r, dict))

            if not isinstance(body, dict):
                break
            maybe_next = body.get("next_token") or body.get("nextToken")
            if not maybe_next or maybe_next == next_token:
                break
            next_token = str(maybe_next)

        return all_records

    def _extract_rows(self, body: Any, records_key: str) -> list[dict[str, Any]]:
        if isinstance(body, list):
            return [r for r in body if isinstance(r, dict)]
        if not isinstance(body, dict):
            return []

        rows = body.get(records_key)
        if isinstance(rows, list):
            return [r for r in rows if isinstance(r, dict)]

        data = body.get("data")
        if isinstance(data, list):
            return [r for r in data if isinstance(r, dict)]

        return []

    def _with_account(self, row: dict[str, Any]) -> str | None:
        return first_present(row.get("account_id"), row.get("accountId"), self.account_id)

    def _map_account(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "account_id": str(first_present(row.get("id"), row.get("account_id"), row.get("accountId"), "")),
            "account_name": first_present(row.get("name"), row.get("account_name")),
            "email": row.get("email"),
            "status": row.get("status"),
            "joined_method": first_present(row.get("joined_method"), row.get("joinedMethod")),
            "joined_timestamp": as_iso8601(first_present(row.get("joined_timestamp"), row.get("joinedTimestamp"))),
            "updated_at": as_iso8601(first_present(row.get("updated_at"), row.get("joined_timestamp"), row.get("joinedTimestamp"), row.get("id"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_iam_user(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "user_id": str(first_present(row.get("user_id"), row.get("userId"), row.get("id"), "")),
            "account_id": self._with_account(row),
            "user_name": first_present(row.get("user_name"), row.get("userName")),
            "arn": row.get("arn"),
            "path": row.get("path"),
            "create_date": as_iso8601(first_present(row.get("create_date"), row.get("createDate"))),
            "password_last_used": as_iso8601(first_present(row.get("password_last_used"), row.get("passwordLastUsed"))),
            "updated_at": as_iso8601(first_present(row.get("updated_at"), row.get("password_last_used"), row.get("passwordLastUsed"), row.get("create_date"), row.get("createDate"), row.get("id"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_iam_role(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "role_id": str(first_present(row.get("role_id"), row.get("roleId"), row.get("id"), "")),
            "account_id": self._with_account(row),
            "role_name": first_present(row.get("role_name"), row.get("roleName")),
            "arn": row.get("arn"),
            "path": row.get("path"),
            "description": row.get("description"),
            "create_date": as_iso8601(first_present(row.get("create_date"), row.get("createDate"))),
            "updated_at": as_iso8601(first_present(row.get("updated_at"), row.get("create_date"), row.get("createDate"), row.get("id"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_iam_policy(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "policy_arn": str(first_present(row.get("arn"), row.get("policy_arn"), row.get("policyArn"), "")),
            "account_id": self._with_account(row),
            "policy_name": first_present(row.get("policy_name"), row.get("policyName")),
            "path": row.get("path"),
            "default_version_id": first_present(row.get("default_version_id"), row.get("defaultVersionId")),
            "attachment_count": str(first_present(row.get("attachment_count"), row.get("attachmentCount"), "")),
            "create_date": as_iso8601(first_present(row.get("create_date"), row.get("createDate"))),
            "update_date": as_iso8601(first_present(row.get("update_date"), row.get("updateDate"))),
            "updated_at": as_iso8601(first_present(row.get("updated_at"), row.get("update_date"), row.get("updateDate"), row.get("create_date"), row.get("createDate"), row.get("arn"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_ec2_instance(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "instance_id": str(first_present(row.get("instance_id"), row.get("instanceId"), row.get("id"), "")),
            "account_id": self._with_account(row),
            "region": row.get("region"),
            "instance_type": first_present(row.get("instance_type"), row.get("instanceType")),
            "state": row.get("state"),
            "vpc_id": first_present(row.get("vpc_id"), row.get("vpcId")),
            "subnet_id": first_present(row.get("subnet_id"), row.get("subnetId")),
            "private_ip": first_present(row.get("private_ip"), row.get("privateIp")),
            "launch_time": as_iso8601(first_present(row.get("launch_time"), row.get("launchTime"))),
            "updated_at": as_iso8601(first_present(row.get("updated_at"), row.get("launch_time"), row.get("launchTime"), row.get("id"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_vpc(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "vpc_id": str(first_present(row.get("vpc_id"), row.get("vpcId"), row.get("id"), "")),
            "account_id": self._with_account(row),
            "region": row.get("region"),
            "cidr_block": first_present(row.get("cidr_block"), row.get("cidrBlock")),
            "is_default": str(first_present(row.get("is_default"), row.get("isDefault"), "")),
            "state": row.get("state"),
            "owner_id": first_present(row.get("owner_id"), row.get("ownerId")),
            "updated_at": as_iso8601(first_present(row.get("updated_at"), row.get("id"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_security_group(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "security_group_id": str(first_present(row.get("group_id"), row.get("groupId"), row.get("security_group_id"), row.get("id"), "")),
            "account_id": self._with_account(row),
            "region": row.get("region"),
            "group_name": first_present(row.get("group_name"), row.get("groupName")),
            "description": row.get("description"),
            "vpc_id": first_present(row.get("vpc_id"), row.get("vpcId")),
            "owner_id": first_present(row.get("owner_id"), row.get("ownerId")),
            "updated_at": as_iso8601(first_present(row.get("updated_at"), row.get("id"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_s3_bucket(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "bucket_name": str(first_present(row.get("name"), row.get("bucket_name"), row.get("bucketName"), "")),
            "account_id": self._with_account(row),
            "arn": row.get("arn"),
            "region": row.get("region"),
            "creation_date": as_iso8601(first_present(row.get("creation_date"), row.get("creationDate"))),
            "public_access_blocked": str(first_present(row.get("public_access_blocked"), row.get("publicAccessBlocked"), "")),
            "versioning_status": first_present(row.get("versioning_status"), row.get("versioningStatus")),
            "updated_at": as_iso8601(first_present(row.get("updated_at"), row.get("creation_date"), row.get("creationDate"), row.get("name"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_kms_key(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "key_id": str(first_present(row.get("key_id"), row.get("keyId"), row.get("id"), "")),
            "account_id": self._with_account(row),
            "arn": row.get("arn"),
            "region": row.get("region"),
            "key_manager": first_present(row.get("key_manager"), row.get("keyManager")),
            "key_state": first_present(row.get("key_state"), row.get("keyState")),
            "creation_date": as_iso8601(first_present(row.get("creation_date"), row.get("creationDate"))),
            "enabled": str(first_present(row.get("enabled"), "")),
            "updated_at": as_iso8601(first_present(row.get("updated_at"), row.get("creation_date"), row.get("creationDate"), row.get("id"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_cloudtrail_trail(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "trail_arn": str(first_present(row.get("trail_arn"), row.get("trailARN"), row.get("arn"), "")),
            "account_id": self._with_account(row),
            "name": row.get("name"),
            "home_region": first_present(row.get("home_region"), row.get("homeRegion")),
            "s3_bucket_name": first_present(row.get("s3_bucket_name"), row.get("s3BucketName")),
            "is_multi_region_trail": str(first_present(row.get("is_multi_region_trail"), row.get("isMultiRegionTrail"), "")),
            "log_file_validation_enabled": str(first_present(row.get("log_file_validation_enabled"), row.get("logFileValidationEnabled"), "")),
            "kms_key_id": first_present(row.get("kms_key_id"), row.get("kmsKeyId")),
            "updated_at": as_iso8601(first_present(row.get("updated_at"), row.get("id"), row.get("arn"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_config_recorder(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "recorder_name": str(first_present(row.get("name"), row.get("recorder_name"), row.get("recorderName"), "")),
            "account_id": self._with_account(row),
            "region": row.get("region"),
            "role_arn": first_present(row.get("role_arn"), row.get("roleARN")),
            "recording_group": json.dumps(first_present(row.get("recording_group"), row.get("recordingGroup"), {}), separators=(",", ":"), default=str),
            "recording": str(first_present(row.get("recording"), "")),
            "last_status": first_present(row.get("last_status"), row.get("lastStatus")),
            "last_status_change_time": as_iso8601(first_present(row.get("last_status_change_time"), row.get("lastStatusChangeTime"))),
            "updated_at": as_iso8601(first_present(row.get("updated_at"), row.get("last_status_change_time"), row.get("lastStatusChangeTime"), row.get("name"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _map_guardduty_finding(self, row: dict[str, Any]) -> dict[str, Any]:
        service = row.get("service") if isinstance(row.get("service"), dict) else {}
        resource = row.get("resource") if isinstance(row.get("resource"), dict) else {}
        action = service.get("action") if isinstance(service.get("action"), dict) else {}
        return {
            "finding_id": str(first_present(row.get("id"), row.get("finding_id"), row.get("findingId"), "")),
            "account_id": self._with_account(row),
            "region": row.get("region"),
            "detector_id": first_present(row.get("detector_id"), row.get("detectorId")),
            "type": row.get("type"),
            "severity": str(row.get("severity")) if row.get("severity") is not None else None,
            "title": row.get("title"),
            "resource_type": resource.get("resourceType"),
            "service_action": json.dumps(action, separators=(",", ":"), default=str) if action else None,
            "created_at": as_iso8601(first_present(row.get("created_at"), row.get("createdAt"))),
            "updated_at": as_iso8601(first_present(row.get("updated_at"), row.get("updatedAt"), row.get("created_at"), row.get("createdAt"), row.get("id"))),
            "raw_payload": json.dumps(row, separators=(",", ":"), default=str),
        }

    def _finalize_cdc(
        self, records: list[dict[str, Any]], start_offset: dict, cursor_field: str
    ) -> tuple[Iterator[dict], dict]:
        if not records:
            return iter([]), start_offset or {}
        current = start_offset.get("cursor") if start_offset else None
        max_cursor = current
        for record in records:
            value = record.get(cursor_field)
            if value and (max_cursor is None or str(value) > str(max_cursor)):
                max_cursor = value
        if max_cursor is None:
            max_cursor = self._init_time
        if current is not None and str(max_cursor) <= str(current):
            return iter([]), start_offset
        return iter(records), {"cursor": max_cursor}
