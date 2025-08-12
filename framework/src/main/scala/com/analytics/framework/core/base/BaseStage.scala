package com.analytics.framework.core.base

import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.values.PCollection

/**
 * Base trait for all pipeline stages.
 *
 * A stage consumes an input of type `I` and produces an output of type `O`. Concrete
 * stages may operate either on Apache Beam collections (via the [[apply]] method)
 * or on in‑memory Scala data types (via the [[process]] method).  To support
 * both environments, this trait defines a default implementation for each
 * method which simply casts and returns the input unchanged.  Stages that do not
 * override one of the methods will therefore compile without having to
 * implement both.
 */
abstract class BaseStage[I, O] {

  /** Human readable name for the stage. */
  def name: String

  /**
   * Apply this stage to an Apache Beam [[org.apache.beam.sdk.values.PCollection]].
   *
   * Beam stages should override this method to transform the incoming
   * PCollection.  The default implementation returns the input unchanged (after
   * casting to the expected output type) so that stages operating only on local
   * data can ignore this method.
   *
   * @param p the current Beam pipeline
   * @param in the input PCollection
   * @param ctx implicit pipeline context providing runtime configuration
   * @return a transformed PCollection
   */
  def apply(p: Pipeline, in: PCollection[I])(implicit ctx: PipelineCtx): PCollection[O] =
    in.asInstanceOf[PCollection[O]]

  /**
   * Process the given input and produce an output using in‑memory collections.
   *
   * Local stages should override this method to perform their logic on the
   * provided input.  The default implementation returns the input unchanged
   * (after casting) so that Beam stages can ignore it.
   *
   * @param input the input value of type `I`
   * @param ctx the pipeline context
   * @return a transformed value of type `O`
   */
  def process(input: I)(implicit ctx: PipelineCtx): O = input.asInstanceOf[O]

  /**
   * Process a batch of records using the process method.  This method
   * provides backwards‑compatibility for older stage implementations that
   * expect to work with Scala collections via a run method.  The default
   * implementation simply maps over the input sequence, applying the
   * process method to each element.  Concrete stages may override this
   * method to implement more efficient batch processing if desired.
   *
   * @param ctx pipeline context
   * @param in  a sequence of input records
   * @return a sequence of transformed records
   */
  def run(ctx: PipelineCtx, in: Seq[I]): Seq[O] = {
    in.map { elem =>
      implicit val ictx: PipelineCtx = ctx
      process(elem)
    }
  }
}
