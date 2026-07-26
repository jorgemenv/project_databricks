--DIM_CLIENTE
CREATE OR REFRESH MATERIALIZED VIEW proyecto_final.gold.dim_cliente
AS
WITH cliente_ranked AS (
  SELECT
    customer_id,
    nombre,
    apellido,
    email,
    ciudad,
    pais,
    CAST(fecha_registro AS DATE) AS fecha_registro,
    segmento,
    ROW_NUMBER() OVER (
      PARTITION BY nombre, apellido
      ORDER BY CAST(fecha_registro AS DATE) DESC, customer_id DESC
    ) AS rn
  FROM proyecto_final.silver.clientes
)
SELECT
  customer_id AS customer_key,
  concat(nombre,' ',apellido) AS full_name,
  email,
  ciudad,
  pais,
  segmento,
  fecha_registro
FROM cliente_ranked
WHERE rn = 1;


--DIM_PRODUCTO
CREATE OR REFRESH MATERIALIZED VIEW proyecto_final.gold.dim_producto
AS
WITH producto_ranked AS (
  SELECT
    product_id,
    nombre_producto,
    categoria,
    subcategoria,
    CAST(precio_unitario AS DECIMAL(12,2)) AS precio_unitario,
    proveedor,
    CAST(stock_actual AS INT) AS stock_actual,
    ROW_NUMBER() OVER (
      PARTITION BY nombre_producto, proveedor
      ORDER BY product_id DESC
    ) AS rn
  FROM proyecto_final.silver.productos
)
SELECT
  product_id AS product_key,
  nombre_producto,
  categoria,
  subcategoria,
  proveedor,
  precio_unitario,
  stock_actual
FROM producto_ranked
WHERE rn = 1;

--DIM_FECHA
CREATE OR REFRESH MATERIALIZED VIEW proyecto_final.gold.dim_fecha
AS
SELECT DISTINCT
  CAST(date_format(CAST(fecha_pedido AS DATE), 'yyyyMMdd') AS INT) AS date_key,
  date_format(CAST(fecha_pedido AS DATE),'yyyy-MM-dd') AS fecha,
  year(CAST(fecha_pedido AS DATE)) AS anio,
  month(CAST(fecha_pedido AS DATE)) AS mes,
  day(CAST(fecha_pedido AS DATE)) AS dia,
  quarter(CAST(fecha_pedido AS DATE)) AS trimestre,
  date_format(CAST(fecha_pedido AS DATE), 'MMMM') AS nombre_mes,
  date_format(CAST(fecha_pedido AS DATE), 'EEEE') AS dia_semana
FROM proyecto_final.silver.pedidos;

--FACT_VENTAS
CREATE OR REFRESH MATERIALIZED VIEW proyecto_final.gold.fact_ventas
AS
SELECT
  dp.order_item_id AS venta_id,
  pe.customer_id AS customer_key,
  dp.product_id AS product_key,
  CAST(date_format(CAST(pe.fecha_pedido AS DATE), 'yyyyMMdd') AS INT) AS date_key,
  dp.order_id,
  dp.cantidad,
  CAST(dp.precio_unitario AS DECIMAL(12,2)) AS precio_unitario,
  CAST(dp.descuento AS DECIMAL(5,4)) AS descuento,
  ROUND(dp.cantidad * dp.precio_unitario * (1 - dp.descuento), 2) AS monto_total
FROM proyecto_final.silver.detalle_pedidos dp
INNER JOIN proyecto_final.silver.pedidos pe
  ON dp.order_id = pe.order_id;