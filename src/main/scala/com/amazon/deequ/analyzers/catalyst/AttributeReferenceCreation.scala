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

package com.amazon.deequ.analyzers.catalyst

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.Metadata

/**
 * Creates AttributeReference instances compatible with both Spark 3.5 and 4.1.
 * Uses reflection to handle API differences between Spark versions.
 */
object AttributeReferenceCreation {

  /**
   * Creates an AttributeReference with the given name and LongType data type.
   * Uses reflection to handle different Spark versions (2.x, 3.x, 4.x).
   */
  def createSafe(name: String): AttributeReference = {
    try {
      // Try Spark 4.x API first - uses factory method
      createSpark4x(name)
    } catch {
      case _: Exception =>
        try {
          // Fall back to Spark 3.x / 2.4 API
          createSpark3x(name)
        } catch {
          case _: Exception =>
            // Fall back to Spark 2.2/2.3 API
            createSpark2x(name)
        }
    }
  }

  private def createSpark4x(name: String): AttributeReference = {
    // In Spark 4.x, AttributeReference companion object has apply method
    val companionClass = Class.forName("org.apache.spark.sql.catalyst.expressions.AttributeReference$")
    val companion = companionClass.getField("MODULE$").get(null)

    // Get NamedExpression$.newExprId()
    val namedExprClass = Class.forName("org.apache.spark.sql.catalyst.expressions.NamedExpression$")
    val namedExprCompanion = namedExprClass.getField("MODULE$").get(null)
    val newExprIdMethod = namedExprClass.getMethod("newExprId")
    val exprId = newExprIdMethod.invoke(namedExprCompanion)

    // Find apply method - parameters may vary by version
    val applyMethods = companionClass.getMethods.filter(_.getName == "apply")
    val applyMethod = applyMethods.head // Take first apply method

    val dataType = LongType
    val nullable = java.lang.Boolean.TRUE
    val metadata = Metadata.empty

    applyMethod.getParameterCount match {
      case 6 =>
        // Spark 3.x/4.x: apply(name, dataType, nullable, metadata, exprId, qualifier)
        val emptySeq = Seq.empty[String]
        applyMethod.invoke(companion, name, dataType, nullable, metadata, exprId, emptySeq)
          .asInstanceOf[AttributeReference]
      case _ =>
        throw new IllegalStateException(s"Unexpected parameter count: ${applyMethod.getParameterCount}")
    }
  }

  private def createSpark3x(name: String): AttributeReference = {
    // Spark 3.x: AttributeReference(name, dataType, nullable, metadata)(exprId, qualifier)
    // This is a case class with multiple parameter lists

    val attrRefClass = classOf[AttributeReference]

    // Get NamedExpression$.newExprId()
    val namedExprClass = Class.forName("org.apache.spark.sql.catalyst.expressions.NamedExpression$")
    val namedExprCompanion = namedExprClass.getField("MODULE$").get(null)
    val newExprIdMethod = namedExprClass.getMethod("newExprId")
    val exprId = newExprIdMethod.invoke(namedExprCompanion)

    val dataType = LongType
    val metadata = Metadata.empty
    val emptySeq = Seq.empty[String]

    // Find constructor
    val constructors = attrRefClass.getConstructors
    val constructor = constructors.find(_.getParameterCount == 6).getOrElse(
      throw new IllegalStateException("Cannot find AttributeReference constructor with 6 parameters")
    )

    constructor.newInstance(
      name,
      dataType,
      java.lang.Boolean.TRUE,
      metadata,
      exprId,
      emptySeq
    ).asInstanceOf[AttributeReference]
  }

  private def createSpark2x(name: String): AttributeReference = {
    // Spark 2.x: may have Option[String] qualifier or isGenerated parameter

    val attrRefClass = classOf[AttributeReference]

    // Get NamedExpression$.newExprId()
    val namedExprClass = Class.forName("org.apache.spark.sql.catalyst.expressions.NamedExpression$")
    val namedExprCompanion = namedExprClass.getField("MODULE$").get(null)
    val newExprIdMethod = namedExprClass.getMethod("newExprId")
    val exprId = newExprIdMethod.invoke(namedExprCompanion)

    val dataType = LongType
    val metadata = Metadata.empty
    val none: Option[String] = None

    val constructors = attrRefClass.getConstructors

    // Try 7-parameter constructor (Spark 2.2)
    constructors.find(_.getParameterCount == 7) match {
      case Some(constructor) =>
        constructor.newInstance(
          name,
          dataType,
          java.lang.Boolean.TRUE,
          metadata,
          exprId,
          none,
          java.lang.Boolean.FALSE
        ).asInstanceOf[AttributeReference]
      case None =>
        // Try 6-parameter constructor with Option (Spark 2.3)
        val constructor = constructors.find(_.getParameterCount == 6).getOrElse(
          throw new IllegalStateException("Cannot find AttributeReference constructor")
        )
        constructor.newInstance(
          name,
          dataType,
          java.lang.Boolean.TRUE,
          metadata,
          exprId,
          none
        ).asInstanceOf[AttributeReference]
    }
  }
}
