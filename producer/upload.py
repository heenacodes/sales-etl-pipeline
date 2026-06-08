"""
Producer: simulates an external system dropping sales.csv into S3.

Today:     uploads file to s3://<bucket>/raw/sales.csv
Later:     that upload triggers Lambda → Step Function → Glue → Redshift
"""

import os
import sys
from pathlib import Path

import boto3
from botocore.exceptions import ClientError


def load_config() -> dict:
    """Read settings from config.env (local) or environment variables."""
    project_root = Path(__file__).resolve().parents[1]
    config_file = project_root / "config.env"

    if config_file.exists():
        for line in config_file.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, value = line.partition("=")
                os.environ.setdefault(key.strip(), value.strip())

    bucket = os.environ.get("S3_BUCKET")
    region = os.environ.get("AWS_REGION", "eu-west-2")
    sales_file = os.environ.get("SALES_FILE", "data/sales.csv")

    if not bucket or "YOUR_ACCOUNT_ID" in bucket:
        print("Error: set S3_BUCKET in config.env (copy from config.env.example)")
        sys.exit(1)

    return {
        "bucket": bucket,
        "region": region,
        "sales_path": project_root / sales_file,
        "s3_key": "raw/sales.csv",
    }


def main() -> None:
    config = load_config()

    if not config["sales_path"].exists():
        print(f"Error: file not found: {config['sales_path']}")
        sys.exit(1)

    print(f"Bucket:  {config['bucket']}")
    print(f"Region:  {config['region']}")
    print(f"Upload:  {config['sales_path'].name} → s3://{config['bucket']}/{config['s3_key']}")

    s3 = boto3.client("s3", region_name=config["region"])

    try:
        s3.upload_file(str(config["sales_path"]), config["bucket"], config["s3_key"])
    except ClientError as exc:
        print(f"Upload failed: {exc}")
        sys.exit(1)

    print("Done. File is in S3 raw/ folder.")
    print("(Pipeline will auto-run once Lambda + Step Function exist on Day 4.)")


if __name__ == "__main__":
    main()
