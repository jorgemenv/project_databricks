from pyspark import pipelines as dp
from pyspark.sql import functions as F

from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="proyecto_final.bronze.clientes_raw",
    comment="Entidad maestra con la información de los clientes de la tienda.",
    table_properties={"quality": "bronze"},
)
def bronze_clientes():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("cloudFiles.schemaLocation",
                    "/Volumes/proyecto_final/landing/raw_data/ventas_retail_jorgemendoza/_schemas/bronze_clientes")
            .load("/Volumes/proyecto_final/landing/raw_data/ventas_retail_jorgemendoza/clientes/")
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.col("_metadata.file_path"))
    )

@dp.table(
    name="proyecto_final.bronze.productos_raw",
    comment="Catálogo de productos vendidos.",
    table_properties={"quality": "bronze"},
)
def bronze_productos():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("cloudFiles.schemaLocation",
                    "/Volumes/proyecto_final/landing/raw_data/ventas_retail_jorgemendoza/_schemas/bronze_productos")
            .load("/Volumes/proyecto_final/landing/raw_data/ventas_retail_jorgemendoza/productos/")
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.col("_metadata.file_path"))
    )

@dp.table(
    name="proyecto_final.bronze.pedidos_raw",
    comment="Cabecera de cada pedido realizado.",
    table_properties={"quality": "bronze"},
)
def bronze_pedidos():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("multiLine", "true")
            .option("cloudFiles.schemaLocation",
                    "/Volumes/proyecto_final/landing/raw_data/ventas_retail_jorgemendoza/_schemas/bronze_pedidos")
            .load("/Volumes/proyecto_final/landing/raw_data/ventas_retail_jorgemendoza/pedidos/")
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.col("_metadata.file_path"))
    )

@dp.table(
    name="proyecto_final.bronze.detalle_pedidos_raw",
    comment="Detalle línea a línea de cada pedido (grano de la futura tabla de hechos).",
    table_properties={"quality": "bronze"},
)
def bronze_detalle_pedidos():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("multiLine", "true")
            .option("cloudFiles.schemaLocation",
                    "/Volumes/proyecto_final/landing/raw_data/ventas_retail_jorgemendoza/_schemas/bronze_detalle_pedidos")
            .load("/Volumes/proyecto_final/landing/raw_data/ventas_retail_jorgemendoza/detalle_pedidos/")
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.col("_metadata.file_path"))
    )