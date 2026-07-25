from pyspark import pipelines as dp
from pyspark.sql import functions as F

#CLIENTES
@dp.table(
    name="proyecto_final.silver.clientes",
    comment="Contiene los datos de los clientes",
    table_properties={"quality": "silver"}
)
@dp.expect_or_fail("id_nulo", "customer_id IS NOT NULL")
@dp.expect("email_valido", "email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'")
@dp.expect_or_drop("segmento_permitido", "UPPER(segmento) IN ('RETAIL', 'PREMIUM')")  # warn-only
def silver_clientes():
    return (
        spark.readStream.table("proyecto_final.bronze.clientes_raw")
            .select(
                F.col("customer_id").cast("int"),
                F.col("nombre"),
                F.col("apellido"),
                F.col("email"),
                F.col("ciudad"),
                F.col("pais"),
                F.col("fecha_registro").cast("date"),
                F.col("segmento"),
                F.col("_ingested_at")
            )
            .dropDuplicates(["customer_id"])
    )

#PRODUCTOS
@dp.table(
    name="proyecto_final.silver.productos ",
    comment="Catálogo de productos vendidos.",
    table_properties={"quality": "silver"}
)
@dp.expect_or_fail("id_nulo", "product_id IS NOT NULL")
@dp.expect_or_drop("precio_positivo", "CAST(precio_unitario AS DOUBLE) > 0")
@dp.expect_or_drop("stock_positivo", "CAST(stock_actual AS INT) > 0")
def silver_productos():
    return (
        spark.readStream.table("proyecto_final.bronze.productos_raw")
            .select(
                F.col("product_id").cast("int"),
                F.col("nombre_producto"),
                F.col("categoria"),
                F.col("subcategoria"),
                F.col("precio_unitario").cast("decimal(12,2)").alias("precio_unitario"),
                F.col("proveedor"),
                F.col("stock_actual").cast("int"),
                F.col("_ingested_at")
            )
            .dropDuplicates(["product_id"])
    )

#PEDIDOS
@dp.table(
    name="proyecto_final.silver.pedidos ",
    comment="Cabecera de cada pedido realizado.",
    table_properties={"quality": "silver"}
)
@dp.expect_or_fail("id_nulo", "order_id IS NOT NULL")
@dp.expect_or_drop("estado_pedido", "UPPER(estado_pedido) IN ('COMPLETADO', 'EN_PROCESO','CANCELADO')")
@dp.expect_or_drop("total_positivo", "CAST(total_pedido AS INT) > 0")
def silver_pedidos():
    return (
        spark.readStream.table("proyecto_final.bronze.pedidos_raw")
            .select(
                F.col("order_id").cast("int"),
                F.col("customer_id").cast("int"),
                F.col("fecha_pedido").cast("date"),
                F.col("canal_venta"),
                F.col("estado_pedido"),
                F.col("total_pedido").cast("decimal(12,2)").alias("total_pedido"),
                F.col("_ingested_at")
            )
            .dropDuplicates(["order_id"])
    )

#DETALLE PEDIDOS
@dp.table(
    name="proyecto_final.silver.detalle_pedidos",
    comment="Detalle línea a línea de cada pedido (grano de la futura tabla de hechos).",
    table_properties={"quality": "silver"}
)
@dp.expect_or_fail("id_nulo", "order_item_id IS NOT NULL")
@dp.expect_or_drop("cantidad_positiva", "CAST(cantidad AS INT) > 0")
@dp.expect_or_fail("order_id_nulo", "order_id IS NOT NULL")
@dp.expect_or_fail("product_id_nulo", "product_id IS NOT NULL")
def silver_detalle_pedidos():
    return (
        spark.readStream.table("proyecto_final.bronze.detalle_pedidos_raw")
            .select(
                F.col("order_item_id").cast("int"),
                F.col("order_id").cast("int"),
                F.col("product_id").cast("int"),
                F.col("cantidad").cast("int"),
                F.col("precio_unitario").cast("decimal(12,2)").alias("precio_unitario"),
                F.col("descuento").cast("decimal(3,2)").alias("descuento"),
                F.col("_ingested_at")
            )
            .dropDuplicates(["order_item_id"])
    )