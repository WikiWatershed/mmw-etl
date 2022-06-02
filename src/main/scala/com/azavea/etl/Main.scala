package com.azavea.etl

import cats.implicits._
import com.monovore.decline.{CommandApp, Opts}
import geotrellis.spark.store.kryo.KryoRegistrator
import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession

import java.net.URI

object IngestToLayout extends CommandApp(
  name = "civic-apps-etl",
  header = "Ingest image to ",
  main = {
    val sourceImageUriOpt = Opts.option[URI](
      "source-image",
      short="i",
      help = "Input image file"
    )

    val referenceLayerUriOpt = Opts.option[URI](
      "reference-layer",
      short="c",
      help = "URI of catalog to derive layer"
    )

    (sourceImageUriOpt, referenceLayerUriOpt).mapN {
      (sourceImageUri, referenceLayerUri) =>
        {
          import IngestToLayoutUtils._

          val appName = "Ingest to reference layout"

          val conf = new SparkConf()
            .setIfMissing("spark.master", "local[*]")
            .setAppName(s"${appName}")
            .set("spark.sql.orc.impl", "native")
            .set("spark.sql.orc.filterPushdown", "true")
            .set("spark.sql.parquet.mergeSchema", "false")
            .set("spark.sql.parquet.filterPushdown", "true")
            .set("spark.sql.hive.metastorePartitionPruning", "true")
            .set("spark.ui.showConsoleProgress", "true")
            .set("spark.serializer", classOf[KryoSerializer].getName)
            .set("spark.kryo.registrator", classOf[KryoRegistrator].getName)

          val spark = SparkSession.builder
            .config(conf)
            .enableHiveSupport
            .getOrCreate

          import spark.implicits._

          logWarn("Starting ingest")

          spark.stop
        }
    }
  }
)

object IngestToLayoutUtils {

  val logger = org.apache.log4j.Logger.getLogger(getClass())

  def logDebug(msg: => String) = logger.debug(msg)
  def logWarn(msg: => String) = logger.warn(msg)
  def logError(msg: => String) = logger.error(msg)

}
