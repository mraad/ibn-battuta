package com.esri.battuta.dbf

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType

/**
 * Provides access to DBF data from pure SQL statements (i.e. for users of the JDBC server).
 */
class DefaultSource extends RelationProvider with SchemaRelationProvider {
  /**
   * Creates a new relation for data store in DBF given parameters.
   * Parameters have to include 'path'.
   */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    createRelation(sqlContext, parameters, null)
  }

  /**
   * Creates a new relation for data store in DBF given parameters and user supported schema.
   * Parameters have to include 'path'.
   */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    val pathOpt = parameters.get("path")
    if (pathOpt.isEmpty)
      throw new Exception("Parameter 'path' must be defined.")
    DBFRelation(pathOpt.get, schema)(sqlContext)
  }
}
