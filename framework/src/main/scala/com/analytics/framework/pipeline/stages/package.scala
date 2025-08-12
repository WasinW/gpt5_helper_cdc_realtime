package com.analytics.framework.pipeline

/**
 * Type aliases for pipeline stages.  The `BaseStage` trait is defined in the
 * `com.analytics.framework.core.base` package with default implementations.
 * This package object re‑exports the base trait so that code can continue to
 * reference `com.analytics.framework.pipeline.stages.BaseStage`.
 */
package object stages {
  type BaseStage[I, O] = com.analytics.framework.core.base.BaseStage[I, O]
}
