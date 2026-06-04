import json
import boto3
import os
from urllib.parse import urlparse


def load_rules_from_s3(s3_path_key: str, rules_file: str) -> dict:
    try:
        fallback_path = "s3://cdp-test-uat-infra/rules"
        base_path = os.getenv(s3_path_key, fallback_path)

        if not base_path:
            raise ValueError(
                f"Environment variable '{s3_path_key}' is not set."
            )

        full_path = f"{base_path.rstrip('/')}/{rules_file}"

        parsed = urlparse(full_path)

        bucket = parsed.netloc
        key = parsed.path.lstrip("/")

        s3 = boto3.client("s3")

        obj = s3.get_object(
            Bucket=bucket,
            Key=key
        )

        rules = json.loads(
            obj["Body"].read().decode("utf-8")
        )

        if "rules" not in rules:
            raise ValueError(
                f"Missing 'rules' key in {rules_file}"
            )

        return rules

    except Exception as e:
        raise RuntimeError(
            f"[RULE_ENGINE ERROR] Failed loading "
            f"rule file={rules_file} "
            f"from path={s3_path_key}"
        ) from e