/*
 * Copyright 2018 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.minio.spark.select

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

/**
 * Helper methods for pushing filters into Select queries.
 */
private[spark] object FilterPushdown {
  /**
   * Build a SQL WHERE clause for the given filters. If a filter cannot be pushed down then no
   * condition will be added to the WHERE clause. If none of the filters can be pushed down then
   * an empty string will be returned.
   *
   * @param schema the schema of the table being queried
   * @param filters an array of filters, the conjunction of which is the filter condition for the
   *                scan.
   */
  def buildWhereClause(schema: StructType, filters: Seq[Filter]): String = {
    val filterExpressions = filters.flatMap(f => buildFilterExpression(schema, f)).mkString(" AND ")
    if (filterExpressions.isEmpty) "" else "WHERE " + filterExpressions
  }

  /**
   * Attempt to convert the given filter into a Select expression. Returns None if the expression
   * could not be converted.
   */
  def buildFilterExpression(schema: StructType, filter: Filter): Option[String] = {
    def buildComparison(attr: String, value: Any, comparisonOp: String): Option[String] = {
      getTypeForAttribute(schema, attr).map { dataType =>
        val sqlEscapedValue: String = dataType match {
          case StringType => s"""'${value.toString.replace("'", "\\'\\'")}'"""
          case DateType => s""""${value.asInstanceOf[Date]}""""
          case TimestampType => s""""${value.asInstanceOf[Timestamp]}""""
          case _ => value.toString
        }
        s"s."+s""""$attr""""+s" $comparisonOp $sqlEscapedValue"  
      }
    }

    filter match {
      case EqualTo(attr, value) => buildComparison(attr, value, "=")
      case LessThan(attr, value) => buildComparison(attr, value, "<")
      case GreaterThan(attr, value) => buildComparison(attr, value, ">")
      case LessThanOrEqual(attr, value) => buildComparison(attr, value, "<=")
      case GreaterThanOrEqual(attr, value) => buildComparison(attr, value, ">=")
      case _ => None
    }
  }

  /**
   * Use the given schema to look up the attribute's data type. Returns None if the attribute could
   * not be resolved.
   */
  private def getTypeForAttribute(schema: StructType, attribute: String): Option[DataType] = {
    if (schema.fieldNames.contains(attribute)) {
      Some(schema(attribute).dataType)
    } else {
      None
    }
  }

  def queryFromSchema(schema: StructType, filters: Array[Filter]): String = {
    var columnList = schema.fields.map(x => s"s."+s""""${x.name}"""").mkString(",")
    if (columnList.length == 0) {
      columnList = "*"
    }
    val whereClause = buildWhereClause(schema, filters)
    if (whereClause.length == 0) {
      s"select $columnList from S3Object s"
    } else {
      s"select $columnList from S3Object s $whereClause"
    }
  }

}
