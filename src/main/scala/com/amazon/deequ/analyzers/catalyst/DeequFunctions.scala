/**
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not
 * use this file except in compliance with the License. A copy of the License
 * is located at
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package org.apache.spark.sql


import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateFunction, StatefulApproxQuantile, StatefulHyperloglogPlus}
import org.apache.spark.sql.catalyst.expressions.Literal

/* Custom aggregation functions used internally by deequ */
object DeequFunctions {

  /**
   * Extract Expression from Column, compatible with both Spark 3.5 and 4.1
   */
  private[this] def columnExpr(column: Column): Expression = {
    val columnClass = column.getClass
    try {
      // Spark 3.5 API: Column has public expr field
      val exprMethod = columnClass.getMethod("expr")
      exprMethod.invoke(column).asInstanceOf[Expression]
    } catch {
      case _: NoSuchMethodException =>
        // Spark 4.1 API: Column uses ColumnNode
        val nodeMethod = columnClass.getMethod("node")
        val node = nodeMethod.invoke(column)

        // Try ExpressionColumnNode first
        try {
          val exprNodeClass = Class.forName("org.apache.spark.sql.classic.ExpressionColumnNode")
          if (exprNodeClass.isInstance(node)) {
            val exprMethod = exprNodeClass.getMethod("expression")
            return exprMethod.invoke(node).asInstanceOf[Expression]
          }
        } catch {
          case _: ClassNotFoundException =>
        }

        // Try ColumnNodeToExpressionConverter as fallback
        try {
          val converterClass = Class.forName("org.apache.spark.sql.classic.ColumnNodeToExpressionConverter$")
          val converter = converterClass.getField("MODULE$").get(null)
          val applyMethod = converterClass.getMethod("apply",
            Class.forName("org.apache.spark.sql.internal.ColumnNode"))
          return applyMethod.invoke(converter, node).asInstanceOf[Expression]
        } catch {
          case _: ClassNotFoundException =>
        }

        throw new RuntimeException(
          s"Unable to extract Expression from Column. Node type: ${node.getClass.getName}. " +
          s"This code supports Spark 3.5 and Spark 4.1 only."
        )
    }
  }

  /**
   * Create Column from Expression, compatible with both Spark 3.5 and 4.1
   */
  private[this] def columnFromExpression(expr: Expression): Column = {
    val columnClass = classOf[Column]

    // Spark 3.5 API: Column has constructor taking Expression directly
    val exprCtor = columnClass.getDeclaredConstructors.find { ctor =>
      val params = ctor.getParameterTypes
      params.length == 1 &&
      params.head.getName == "org.apache.spark.sql.catalyst.expressions.Expression"
    }

    exprCtor match {
      case Some(ctor) =>
        ctor.setAccessible(true)
        ctor.newInstance(expr).asInstanceOf[Column]
      case None =>
        // Spark 4.1 API: Column uses ColumnNode
        val nodeClass = Class.forName("org.apache.spark.sql.classic.ExpressionColumnNode")
        val applyMethod = nodeClass.getMethod("apply", classOf[Expression])
        val node = applyMethod.invoke(null, expr).asInstanceOf[AnyRef]

        val columnNodeClass = Class.forName("org.apache.spark.sql.internal.ColumnNode")
        val columnCtor = columnClass.getDeclaredConstructors
          .find { ctor =>
            val params = ctor.getParameterTypes
            params.length == 1 && params.head.getName == columnNodeClass.getName
          }
          .getOrElse {
            throw new RuntimeException(
              s"Unable to find Column constructor taking ColumnNode. " +
              s"This code supports Spark 3.5 and Spark 4.1 only."
            )
          }

        columnCtor.setAccessible(true)
        columnCtor.newInstance(node).asInstanceOf[Column]
    }
  }

  private[this] def withAggregateFunction(
      func: AggregateFunction,
      isDistinct: Boolean = false): Column = {

    columnFromExpression(func.toAggregateExpression(isDistinct))
  }

  /** Pearson correlation with state */
  def stateful_corr(columnA: String, columnB: String): Column = {
    stateful_corr(Column(columnA), Column(columnB))
  }

  /** Pearson correlation with state */
  def stateful_corr(columnA: Column, columnB: Column): Column = withAggregateFunction {
    new StatefulCorrelation(columnExpr(columnA), columnExpr(columnB))
  }

  /** Standard deviation with state */
  def stateful_stddev_pop(column: String): Column = {
    stateful_stddev_pop(Column(column))
  }

  /** Standard deviation with state */
  def stateful_stddev_pop(column: Column): Column = withAggregateFunction {
    StatefulStdDevPop(columnExpr(column))
  }

  /** Approximate number of distinct values with state via HLL's */
  def stateful_approx_count_distinct(column: String): Column = {
    stateful_approx_count_distinct(Column(column))
  }

  /** Approximate number of distinct values with state via HLL's */
  def stateful_approx_count_distinct(column: Column): Column = withAggregateFunction {
    StatefulHyperloglogPlus(columnExpr(column))
  }

  def stateful_approx_quantile(
      column: Column,
      relativeError: Double)
    : Column = withAggregateFunction {

    StatefulApproxQuantile(
      columnExpr(column),
      // val relativeError = 1.0D / accuracy inside StatefulApproxQuantile
      Literal(1.0 / relativeError),
      mutableAggBufferOffset = 0,
      inputAggBufferOffset = 0
    )
  }

  /** Data type detection with state */
  def stateful_datatype(column: Column): Column = {
    val statefulDataType = new StatefulDataType()
    statefulDataType(column)
  }

  def stateful_kll(
      column: Column,
      sketchSize: Int,
      shrinkingFactor: Double): Column = {
    val statefulKLL = new StatefulKLLSketch(sketchSize, shrinkingFactor)
    statefulKLL(column)
  }
}


