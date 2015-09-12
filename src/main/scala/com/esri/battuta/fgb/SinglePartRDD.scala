package com.esri.battuta.fgb

import com.vividsolutions.jts.geom.Point
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.{Logging, Partition, SparkContext, TaskContext}
import org.geotools.data.Query
import org.geotools.data.ogr.OGRDataStoreFactory
import org.geotools.data.ogr.bridj.BridjOGRDataStoreFactory
import org.geotools.filter.text.cql2.CQL
import org.opengis.filter.Filter

import scala.collection.JavaConverters._

/**
  */
case class SinglePartRDD(@transient sc: SparkContext,
                         dataSource: String,
                         featureSource: String,
                         fieldNamesOpt: Option[Array[String]] = None,
                         cqlOpt: Option[String] = None)
  extends RDD[Row](sc, Nil) with Logging {

  override def compute(thePart: Partition, context: TaskContext): Iterator[Row] = {
    new Iterator[Row] {

      val part = thePart.asInstanceOf[SinglePartition]
      context.addTaskCompletionListener(context => close())

      val factory = new BridjOGRDataStoreFactory()

      val params = Map(
        OGRDataStoreFactory.OGR_DRIVER_NAME.key -> "OpenFileGDB",
        OGRDataStoreFactory.OGR_NAME.key -> dataSource)

      val dataStore = factory.createDataStore(params.asJava)
      val source = dataStore.getFeatureSource(featureSource)
      val filter = if (cqlOpt.isEmpty) {
        Filter.INCLUDE
      } else {
        CQL.toFilter(cqlOpt.get)
      }
      val query = if (fieldNamesOpt.isDefined) {
        new Query(source.getSchema.getTypeName, filter, fieldNamesOpt.get)
      } else {
        // Query.ALL
        new Query(source.getSchema.getTypeName, filter)
      }
      val features = source.getFeatures(query)

      val fieldNames = features.getSchema.getAttributeDescriptors.asScala.map(_.getLocalName).toArray

      val featureIterator = features.features()

      override def hasNext: Boolean = {
        featureIterator.hasNext
      }

      override def next(): Row = {
        val simpleFeature = featureIterator.next()
        // val geom = simpleFeature.getDefaultGeometry.asInstanceOf[Geometry]
        Row.fromSeq(fieldNames.map(fieldName => {
          simpleFeature.getAttribute(fieldName) match {
            case p: Point => Row(p.getX, p.getY)
            case a => a
          }
        }))
      }

      def close() = {
        if (featureIterator != null) {
          featureIterator.close()
        }
        if (dataStore != null) {
          dataStore.dispose()
        }
      }
    }
  }

  override protected def getPartitions: Array[Partition] = {
    Array(SinglePartition(0))
  }
}

private[this] case class SinglePartition(idx: Int) extends Partition {
  override def index: Int = idx
}
