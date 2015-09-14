package com.esri.battuta

import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  */
package object fgb {

  /**
   * Adds a method, `fileGDB`, to SQLContext that allows reading FileGDB data.
   */
  implicit class SQLContextImplicits(sqlContext: SQLContext) extends Serializable {
    implicit def fileGDB(path: String, fieldNames: Option[Array[String]], cql: Option[String]) = {
      sqlContext.baseRelationToDataFrame(FileGDBRelation(path)(sqlContext))
    }
  }

  /**
   * Adds a method, `fileGDB`, to SparkContext that allows reading FileGDB data.
   */
  implicit class SparkContextImplicits(sc: SparkContext) {

    implicit def fileGDB(path: String) = {
      val index = path.lastIndexOf(File.separatorChar)
      val dataSource = path.substring(0, index)
      val featureSource = path.substring(index + 1)
      new SinglePartRDD(sc, dataSource, featureSource, None, None)
    }

    implicit def fileGDB(path: String,
                         fieldNames: Array[String],
                         cql: String
                          ) = {
      val index = path.lastIndexOf(File.separatorChar)
      val dataSource = path.substring(0, index)
      val featureSource = path.substring(index + 1)
      new SinglePartRDD(sc, dataSource, featureSource, Some(fieldNames), Some(cql))
    }

    implicit def fileGDB(dataSource: String,
                         featureSource: String,
                         fieldNames: Array[String],
                         cql: String
                          ) = {
      new SinglePartRDD(sc, dataSource, featureSource, Some(fieldNames), Some(cql))
    }

    implicit def fileGDB(dataSource: String,
                         featureSource: String,
                         fieldNamesOpt: Option[Array[String]] = None,
                         cqlOpt: Option[String] = None
                          ) = {
      new SinglePartRDD(sc, dataSource, featureSource, fieldNamesOpt, cqlOpt)
    }
  }

}
