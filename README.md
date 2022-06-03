# ETL process for Model My Watershed

This utility exists to assist in the ingesting of a new Geotrellis layer that matches the layout of an existing layer.

## Usage

There are two alternatives for launching this utility: (1) direct usage of `spark-submit --class com.azavea.etl.Main <assembly-jar> <args> ...`, targeting a local or remote cluster; or (2) using SBT Lighter.  The latter option will be described here, since users of the 1st option will likely know what they are doing.

In the `build.sbt` definition, one may adjust the EMR cluster parameters before issuing `sparkCreateCluster` in an SBT session (`reload` may be required).  Following this, issue `sparkSubmit` and provide the arguments to the command, the same as would be provided to `run` (or to a regular `spark-submit`).  The arguments are documented below.  When finished, issue `sparkTerminateCluster`.

## Command documentation

```
Usage: mmw-etl --source-image <uri> --reference-catalog <uri> --reference-layer <string> --output-catalog <uri> --output-layer <string> [--partitions <integer>]

Ingest image to

Options and flags:
    --help
        Display this help text.
    --source-image <uri>, -i <uri>
        Input image file
    --reference-catalog <uri>, -c <uri>
        URI of catalog from which to derive layer
    --reference-layer <string>, -l <string>
        Name of layer in catalog
    --output-catalog <uri>
        URI of the catalog to write out to
    --output-layer <string>
        Name of the layer to write
    --partitions <integer>, -p <integer>
        Number of partitions to use for Spark process (default: 200)
```

Note that the reference catalog/layer must exist already, and should be given in URI form.  The source image should be readable through the `RasterSource` interface, which if GDAL is properly installed should cover a wide array of image inputs.
