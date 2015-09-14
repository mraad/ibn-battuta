package com.esri.battuta

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  */
package object dbf {

  /**
   * Adds a method, `dbf`, to SQLContext that allows reading DBF data.
   */
  implicit class SQLContextImplicits(sqlContext: SQLContext) extends Serializable {

    implicit def dbf(path: String) = {
      sqlContext.baseRelationToDataFrame(DBFRelation(path)(sqlContext))
    }
  }

  /**
   * Adds a method, `dbf`, to SparkContext that allows reading DBF data.
   */
  implicit class SparkContextImplicits(sc: SparkContext) {

    implicit def dbf(location: String) = {
      DBFRDD(sc, location, null)
    }
  }

}
