package com.esri.battuta.fgb

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType

/**
  */
class DefaultSource extends RelationProvider with SchemaRelationProvider {
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    createRelation(sqlContext, parameters, null)
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    val pathOpt = parameters.get("path")
    if (pathOpt.isEmpty)
      throw new Exception("Parameter 'path' must be defined.")
    val fieldsOpt = parameters.get("fields")
    val fieldNamesOpt = if (fieldsOpt.isDefined) {
      Some(fieldsOpt.get.split(','))
    }
    else {
      None
    }
    val cqlOpt = parameters.get("cql")
    FileGDBRelation(pathOpt.get, fieldNamesOpt, cqlOpt, schema)(sqlContext)
  }
}
