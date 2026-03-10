import os
import urllib.parse
import pandas as pd
from sqlalchemy import create_engine, text

engine = create_engine(
    "postgresql://{}:{}@{}:{}/{}?sslmode=require".format(
        os.environ['DB_USER'],
        urllib.parse.quote_plus(os.environ['DB_PASS']),
        os.environ['DB_HOST'],
        os.environ['DB_PORT'],
        os.environ['DB_NAME']
    ),
    pool_size=1, max_overflow=0, pool_pre_ping=True
)

df = pd.read_csv(os.environ['CSV_URL'], dtype=str)
print(f"📥 {len(df):,} registros extraídos desde GitHub")

with engine.begin() as conn:
    df.to_sql(
        'ventas_raw_temp', conn, schema='bronze',
        if_exists='replace', index=False,
        method='multi', chunksize=200
    )

    insertados = conn.execute(text("""
        INSERT INTO bronze.ventas_raw (
            id_txn, fecha, cliente_nombre, sucursal, categoria,
            cantidad, precio_unitario, costo_unitario,
            metodo_pago, estado_transaccion, monto_total_raw
        )
        SELECT
            id_txn::INTEGER, fecha, cliente_nombre, sucursal, categoria,
            cantidad, precio_unitario, costo_unitario,
            metodo_pago, estado_transaccion, monto_total_raw
        FROM bronze.ventas_raw_temp
        ON CONFLICT (id_txn) DO NOTHING
    """)).rowcount

    conn.execute(text("DROP TABLE bronze.ventas_raw_temp"))

    # Actualizar dim_sucursales con datos nuevos
    conn.execute(text("""
        INSERT INTO silver.dim_sucursales (
            sucursal, total_transacciones, transacciones_completadas,
            transacciones_canceladas, transacciones_pendientes, pct_completado,
            clientes_distintos, unidades_vendidas, ingreso_total, costo_total,
            utilidad_total, margen_promedio, ticket_promedio,
            pct_transacciones_rentables, categoria_top, metodo_pago_top, updated_at
        )
        SELECT
            sucursal, COUNT(*),
            SUM(CASE WHEN estado_transaccion='Completado' THEN 1 ELSE 0 END),
            SUM(CASE WHEN estado_transaccion='Cancelado'  THEN 1 ELSE 0 END),
            SUM(CASE WHEN estado_transaccion='Pendiente'  THEN 1 ELSE 0 END),
            ROUND(100.0 * SUM(CASE WHEN estado_transaccion='Completado' THEN 1 ELSE 0 END) / COUNT(*), 2),
            COUNT(DISTINCT cliente_nombre),
            SUM(cantidad),
            ROUND(SUM(ingreso_bruto), 2),
            ROUND(SUM(costo_total), 2),
            ROUND(SUM(utilidad_neta), 2),
            ROUND(AVG(margen_utilidad), 4),
            ROUND(AVG(ingreso_bruto), 2),
            ROUND(100.0 * SUM(CASE WHEN es_rentable THEN 1 ELSE 0 END) / COUNT(*), 2),
            (SELECT categoria FROM silver.ventas_clean v2
             WHERE v2.sucursal = v1.sucursal
             GROUP BY categoria ORDER BY SUM(ingreso_bruto) DESC LIMIT 1),
            (SELECT metodo_pago FROM silver.ventas_clean v3
             WHERE v3.sucursal = v1.sucursal
             GROUP BY metodo_pago ORDER BY COUNT(*) DESC LIMIT 1),
            NOW()
        FROM silver.ventas_clean v1
        GROUP BY sucursal
        ON CONFLICT (sucursal) DO UPDATE SET
            total_transacciones         = EXCLUDED.total_transacciones,
            transacciones_completadas   = EXCLUDED.transacciones_completadas,
            transacciones_canceladas    = EXCLUDED.transacciones_canceladas,
            transacciones_pendientes    = EXCLUDED.transacciones_pendientes,
            pct_completado              = EXCLUDED.pct_completado,
            clientes_distintos          = EXCLUDED.clientes_distintos,
            unidades_vendidas           = EXCLUDED.unidades_vendidas,
            ingreso_total               = EXCLUDED.ingreso_total,
            costo_total                 = EXCLUDED.costo_total,
            utilidad_total              = EXCLUDED.utilidad_total,
            margen_promedio             = EXCLUDED.margen_promedio,
            ticket_promedio             = EXCLUDED.ticket_promedio,
            pct_transacciones_rentables = EXCLUDED.pct_transacciones_rentables,
            categoria_top               = EXCLUDED.categoria_top,
            metodo_pago_top             = EXCLUDED.metodo_pago_top,
            updated_at                  = NOW();
    """))

    # Actualizar fact_rentabilidad_detalle con datos nuevos
    conn.execute(text("""
        INSERT INTO silver.fact_rentabilidad_detalle (
            categoria, sucursal, metodo_pago, estado_transaccion,
            total_transacciones, transacciones_rentables, pct_rentable,
            ingreso_total, costo_total, utilidad_total, margen_promedio, updated_at
        )
        SELECT
            categoria, sucursal, metodo_pago, estado_transaccion,
            COUNT(*),
            SUM(CASE WHEN es_rentable THEN 1 ELSE 0 END),
            ROUND(100.0 * SUM(CASE WHEN es_rentable THEN 1 ELSE 0 END) / COUNT(*), 2),
            ROUND(SUM(ingreso_bruto), 2),
            ROUND(SUM(costo_total), 2),
            ROUND(SUM(utilidad_neta), 2),
            ROUND(AVG(margen_utilidad), 4),
            NOW()
        FROM silver.ventas_clean
        GROUP BY categoria, sucursal, metodo_pago, estado_transaccion
        ON CONFLICT (categoria, sucursal, metodo_pago, estado_transaccion) DO UPDATE SET
            total_transacciones     = EXCLUDED.total_transacciones,
            transacciones_rentables = EXCLUDED.transacciones_rentables,
            pct_rentable            = EXCLUDED.pct_rentable,
            ingreso_total           = EXCLUDED.ingreso_total,
            costo_total             = EXCLUDED.costo_total,
            utilidad_total          = EXCLUDED.utilidad_total,
            margen_promedio         = EXCLUDED.margen_promedio,
            updated_at              = NOW();
    """))

print(f"✅ Nuevos registros en Bronze : {insertados:,}")
print(f"⏭️  Duplicados omitidos       : {len(df) - insertados:,}")

# Verificación completa del pipeline
with engine.connect() as conn:
    rows = conn.execute(text("""
        SELECT 'bronze.ventas_raw'                  , COUNT(*) FROM bronze.ventas_raw
        UNION ALL
        SELECT 'silver.ventas_clean'                 , COUNT(*) FROM silver.ventas_clean
        UNION ALL
        SELECT 'silver.dim_clientes'                 , COUNT(*) FROM silver.dim_clientes
        UNION ALL
        SELECT 'silver.dim_categorias'               , COUNT(*) FROM silver.dim_categorias
        UNION ALL
        SELECT 'silver.dim_sucursales'               , COUNT(*) FROM silver.dim_sucursales
        UNION ALL
        SELECT 'silver.fact_ventas_mensual'          , COUNT(*) FROM silver.fact_ventas_mensual
        UNION ALL
        SELECT 'silver.fact_rentabilidad_detalle'    , COUNT(*) FROM silver.fact_rentabilidad_detalle
    """)).fetchall()

    print(f"\n{'CAPA':<40} {'REGISTROS':>10}")
    print("─" * 52)
    for tabla, total in rows:
        print(f"{'✅' if total > 0 else '❌'} {tabla:<38} {total:>10,}")

engine.dispose()
print("\n🏁 Pipeline finalizado.")
