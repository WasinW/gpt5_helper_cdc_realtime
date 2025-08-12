#!/usr/bin/env bash
# This script fixes the BaseStage alias by deleting the broken type alias file
# and creating a proper package object alias.
set -euo pipefail

ROOT_DIR=$(git rev-parse --show-toplevel 2>/dev/null || echo ".")
FILE="${ROOT_DIR}/framework/src/main/scala/com/analytics/framework/pipeline/stages/BaseStage.scala"
PACKAGE_FILE="${ROOT_DIR}/framework/src/main/scala/com/analytics/framework/pipeline/stages/package.scala"

# Delete the incorrect alias file if it exists
if [ -f "$FILE" ]; then
  mv "$FILE" "${FILE}.removed.$(date +%s)"
fi

# Backup existing package.scala if present
if [ -f "$PACKAGE_FILE" ]; then
  cp "$PACKAGE_FILE" "${PACKAGE_FILE}.bak.$(date +%s)"
fi

mkdir -p "$(dirname "$PACKAGE_FILE")"

cat > "$PACKAGE_FILE" <<'SCALA'
package com.analytics.framework.pipeline

package object stages {
  // Alias the pipeline stages BaseStage to the core.base.BaseStage.
  // This makes `BaseStage` visible in the com.analytics.framework.pipeline.stages
  // package without duplicating the trait definition.
  type BaseStage[I, O] = com.analytics.framework.core.base.BaseStage[I, O]
}
SCALA

echo "Replaced BaseStage.scala with package object alias."
