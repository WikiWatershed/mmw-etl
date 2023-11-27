package com.azavea.etl

import cats.implicits._
import com.monovore.decline.{CommandApp, Opts}
import geotrellis.layer.{KeyBounds, SpatialKey, TileLayerMetadata}
import geotrellis.spark.ContextRDD
import geotrellis.spark.store.file.FileLayerWriter
import geotrellis.spark.store.kryo.KryoRegistrator
import geotrellis.spark.store.s3.{S3LayerReader, S3LayerWriter}
import geotrellis.store.LayerId
import geotrellis.store.index.ZCurveKeyIndexMethod
import geotrellis.store.s3.S3ClientProducer
import geotrellis.raster._
import geotrellis.raster.io.geotiff.OverviewStrategy
import geotrellis.vector._
import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession

import java.net.URI

object Main extends CommandApp(
  name = "mmw-etl",
  header = "Ingest image to ",
  main = {
    val sourceImageUriOpt = Opts.option[URI](
      "source-image",
      short="i",
      help = "Input image file"
    )

    val referenceCatalogOpt = Opts.option[URI](
      "reference-catalog",
      short="c",
      help = "URI of catalog from which to derive layer"
    )

    val referenceLayerOpt = Opts.option[String](
      "reference-layer",
      short="l",
      help = "Name of layer in catalog"
    )

    val outputCatalogUriOpt = Opts.option[URI](
      "output-catalog",
      help = "URI of the catalog to write out to"
    )

    val outputLayerOpt = Opts.option[String] (
      "output-layer",
      help = "Name of the layer to write"
    )

    val nPartitionsOpt = Opts.option[Int](
      "partitions",
      short="p",
      help = "Number of partitions to use for Spark process (default: 200)"
    ).withDefault(200)

    (
      sourceImageUriOpt,
      referenceCatalogOpt,
      referenceLayerOpt,
      outputCatalogUriOpt,
      outputLayerOpt,
      nPartitionsOpt
    ).mapN {
      (
        sourceImageUri,
        referenceCatalogUri,
        referenceLayer,
        outputCatalogUri,
        outputLayer,
        nPartitions
      ) => {
        import IngestToLayoutUtils._

        val appName = "Ingest to reference layout"

        assert(referenceCatalogUri.getScheme == "s3", s"This utility currently supports only S3 catalogs; got ${referenceCatalogUri}")

        logWarn("Creating SparkSession")

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
          .getOrCreate

        implicit val sc = spark.sparkContext

        import spark.implicits._

        logWarn("Collecting layer metadata & loading source image")

        val bucket = referenceCatalogUri.getHost
        val path = referenceCatalogUri.getPath
        val reader = S3LayerReader(bucket, path, S3ClientProducer.get())

        val metadata = reader
          .attributeStore
          .readMetadata[TileLayerMetadata[SpatialKey]](
            LayerId(referenceLayer, 0)
          )
        val layout = metadata.layout
        val maptrans = layout.mapTransform

        val source = RasterSource(sourceImageUri.toString)

        logWarn(s"Source image has dimension ${source.cols}×${source.rows} and ${source.extent} (${source.cellSize}, ${source.crs})")

        val imgExtent = source.extent.reproject(source.crs, metadata.crs)
        val imgBounds = maptrans.extentToBounds(imgExtent)

        logWarn("Generating layer RDD")

        val keys = sc.parallelize(imgBounds.coordsIter.toSeq, nPartitions)
        val keyedTiles = keys.flatMap(readTile(source, metadata)(_))
        val newMD = metadata.copy(
          cellType=source.cellType,
          extent=imgExtent,
          bounds=KeyBounds(imgBounds)
        )

        val layer = ContextRDD(keyedTiles, newMD)

        val writer = outputCatalogUri.getScheme match {
          case "s3" => S3LayerWriter(
            outputCatalogUri.getHost,
            outputCatalogUri.getPath,
            S3ClientProducer.get()
          )
          case "file" => FileLayerWriter(outputCatalogUri.getPath)
          case null => FileLayerWriter(outputCatalogUri.getPath)
        }

        writer.write(
          LayerId(outputLayer, 0),
          layer,
          ZCurveKeyIndexMethod
        )

        logWarn("Shutting down SparkContext")

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

  def readTile(
    source: RasterSource,
    metadata: TileLayerMetadata[SpatialKey]
  )(
    position: (Int, Int)
  ): Option[(SpatialKey, Tile)] = {
    val key = SpatialKey(position._1, position._2)
    val maptrans = metadata.layout.mapTransform
    val extent = maptrans.keyToExtent(key)
    val tl = metadata.tileLayout
    val re = RasterExtent(extent, tl.tileCols, tl.tileRows)

    val tileOpt = source
      .reprojectToRegion(
        metadata.crs,
        re,
        ResampleMethods.NearestNeighbor,
        OverviewStrategy.DEFAULT
      )
      .read

    tileOpt.map{ mbtile => (key, mbtile.tile.band(0)) }
  }
}
