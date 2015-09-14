package com.esri.battuta.dbf

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

import scala.collection.JavaConversions._

/**
  */
case class DBFRelation(location: String, userSchema: StructType = null
                        )(@transient val sqlContext: SQLContext) extends BaseRelation with TableScan {

  override val schema = inferSchema()

  private def inferSchema(): StructType = {
    if (userSchema != null) {
      userSchema
    } else {
      val sc = sqlContext.sparkContext
      val path = new Path(location)
      val conf = if (sc == null) new Configuration() else sc.hadoopConfiguration
      val fs = FileSystem.get(path.toUri, conf)
      val dataInputStream = fs.open(path)
      try {
        val dbfReader = new DBFReader(dataInputStream)
        val structFields = dbfReader.getFields.map(field => {
          val dataType = field.dataType match {
            case 'C' => StringType
            case 'D' => LongType
            case 'F' => FloatType
            case 'L' => BooleanType
            case 'N' => {
              if (field.decimalCount == 0) {
                if (field.fieldLength < 5) {
                  ShortType
                }
                else if (field.fieldLength < 8) {
                  IntegerType
                }
                else {
                  LongType
                }
              }
              else {
                DoubleType
              }
            }
          }
          StructField(field.fieldName, dataType, true)
        })
        StructType(structFields)
      } finally {
        dataInputStream.close()
      }

    }
  }

  override def buildScan(): RDD[Row] = {
    DBFRDD(sqlContext.sparkContext, location, schema)
  }
}
