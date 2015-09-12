package com.esri.battuta.fgb

import java.io.File
import java.sql.Timestamp

import com.vividsolutions.jts.geom.Point
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.geotools.data.ogr.OGRDataStoreFactory
import org.geotools.data.ogr.bridj.BridjOGRDataStoreFactory

import scala.collection.JavaConverters._

/**
  */
case class FileGDBRelation(path: String,
                           fieldNamesOpt: Option[Array[String]] = None,
                           cqlOpt: Option[String] = None,
                           userSchema: StructType = null
                            )(@transient val sqlContext: SQLContext) extends BaseRelation with TableScan {

  override val schema = inferSchema()

  private def inferSchema(): StructType = {
    if (userSchema != null) {
      userSchema
    } else {
      val factory = new BridjOGRDataStoreFactory()

      val index = path.lastIndexOf(File.separatorChar)
      val dataSource = path.substring(0, index)
      val featureSource = path.substring(index + 1)

      val params = Map(
        OGRDataStoreFactory.OGR_DRIVER_NAME.key -> "OpenFileGDB",
        OGRDataStoreFactory.OGR_NAME.key -> dataSource)

      val dataStore = factory.createDataStore(params.asJava)
      try {
        val source = dataStore.getFeatureSource(featureSource)
        val featureType = source.getSchema
        val structFields = featureType.getAttributeDescriptors.asScala.
          filter(attrDesc => {
            if (fieldNamesOpt.isEmpty) true
            else fieldNamesOpt.get.contains(attrDesc.getLocalName)
          }).
          map(attrDesc => {
            val dataType = attrDesc.getType.getBinding match {
              case b if b == classOf[java.lang.Byte] => ByteType
              case b if b == classOf[java.lang.Double] => DoubleType
              case b if b == classOf[Timestamp] => TimestampType
              case b if b == classOf[String] => StringType
              // TODO - Implement PointUDT
              case b if b == classOf[Point] => StructType(Array(
                StructField("x", DoubleType, false),
                StructField("y", DoubleType, false)
              ))
              // TODO - more types !!
              case _ => BinaryType
            }
            StructField(attrDesc.getLocalName, dataType, attrDesc.isNillable)
          })
        StructType(structFields)
      }
      finally {
        dataStore.dispose()
      }
    }
  }

  override def buildScan(): RDD[Row] = {
    val index = path.lastIndexOf(File.separatorChar)
    val dataSource = path.substring(0, index)
    val featureSource = path.substring(index + 1)
    SinglePartRDD(sqlContext.sparkContext, dataSource, featureSource, Some(schema.fieldNames), cqlOpt)
  }
}
