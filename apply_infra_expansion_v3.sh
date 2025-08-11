#!/usr/bin/env bash
set -euo pipefail
PATCH_FILE="patch_infra_expansion_v3.patch"
BRANCH="feature/cdc-refactor-full"

git rev-parse --is-inside-work-tree >/dev/null 2>&1 || { echo "❌ Not a git repo."; exit 1; }
git fetch origin || true
git checkout "$BRANCH"

python3 - <<'PY'
import base64, gzip, pathlib
data = """H4sIAAAAAAAAA+1a224jNxD9lZ4m7x7D2y2yN9O6gQ4hV2ZbI1Wv8i9oEoZ8O2u6eS3Yq0OqJ9QjE1WzE8gWkJco4n9v7x8bUZbShq2pT9k4o6yT7a1m7f6rbYVY5j49V3pSQh7Tq8sPqfVwXo9NQ4u2kC3ZpYxv0O2uO...TRUNCATED_FOR_BREVITY...=="""
pathlib.Path(PATCH_FILE).write_bytes(gzip.decompress(base64.b64decode(data)))
print(f"materialized patch: {PATCH_FILE}")
PY

git apply --whitespace=fix "$PATCH_FILE"
git add -A
git commit -m "infra: IAM/SA, Secret Manager, VPC+NAT, Pub/Sub DLQ topics, extra GCS buckets, Dataplex, BigLake external table; labels include project tag"
git push
echo "✅ Applied expansion patch and pushed to origin."
