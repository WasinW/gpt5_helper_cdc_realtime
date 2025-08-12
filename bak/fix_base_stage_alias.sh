#!/usr/bin/env bash
# This script unifies the BaseStage definitions by making the version under
# pipeline/stages a type alias of the core/base version, and adds a default
# implementation of run() to the core version.  This resolves compile
# conflicts where stages use apply() and do not implement run().

set -euo pipefail

ROOT_DIR=$(git rev-parse --show-toplevel 2>/dev/null || echo ".")
CORE_FILE="$ROOT_DIR/framework/src/main/scala/com/analytics/framework/core/base/BaseStage.scala"
PIPELINE_FILE="$ROOT_DIR/framework/src/main/scala/com/analytics/framework/pipeline/stages/BaseStage.scala"

# Ensure core file exists
if [ ! -f "$CORE_FILE" ]; then
  echo "Error: $CORE_FILE not found." >&2
  exit 1
fi

# Backup core file
cp "$CORE_FILE" "${CORE_FILE}.bak.$(date +%s)"

# Write updated core BaseStage with default run method
cat > "$CORE_FILE" <<'SCALA'
package com.analytics.framework.core.base

import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.values.PCollection

/**
 * Base class for all Beam pipeline stages.  Stages implementing Beam logic
 * should override the `apply` method to accept a Pipeline and PCollection.
 *
 * A default `run` method is provided to satisfy generic usage when executing
 * stages over local collections; it simply returns the input unchanged.
 */
abstract class BaseStage[I, O] {
  def name: String
  def apply(p: Pipeline, in: PCollection[I])(implicit ctx: PipelineCtx): PCollection[O]

  /**
   * Execute this stage over a sequence of inputs.  Default implementation
   * returns the input unchanged (after casting to the output type).  Stages
   * operating on Beam collections can ignore this method.
   */
  def run(ctx: PipelineCtx, in: Seq[I]): Seq[O] = in.asInstanceOf[Seq[O]]
}
SCALA

# Backup and replace pipeline/stages/BaseStage.scala with type alias
if [ -f "$PIPELINE_FILE" ]; then
  cp "$PIPELINE_FILE" "${PIPELINE_FILE}.bak.$(date +%s)"
fi
mkdir -p "$(dirname "$PIPELINE_FILE")"
cat > "$PIPELINE_FILE" <<'SCALA'
package com.analytics.framework.pipeline.stages

type BaseStage[I, O] = com.analytics.framework.core.base.BaseStage[I, O]
SCALA

echo "Unified BaseStage in core and added alias in pipeline/stages. Backups created."
