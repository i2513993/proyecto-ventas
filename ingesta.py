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

print(f"✅ Nuevos registros en Bronze : {insertados:,}")
print(f"⏭️  Duplicados omitidos       : {len(df) - insertados:,}")

with engine.connect() as conn:
    rows = conn.execute(text("""
        SELECT 'bronze.ventas_raw'         , COUNT(*) FROM bronze.ventas_raw
        UNION ALL
        SELECT 'silver.ventas_clean'        , COUNT(*) FROM silver.ventas_clean
        UNION ALL
        SELECT 'silver.dim_clientes'        , COUNT(*) FROM silver.dim_clientes
        UNION ALL
        SELECT 'silver.dim_categorias'      , COUNT(*) FROM silver.dim_categorias
        UNION ALL
        SELECT 'silver.fact_ventas_mensual' , COUNT(*) FROM silver.fact_ventas_mensual
    """)).fetchall()

    print(f"\n{'CAPA':<35} {'REGISTROS':>10}")
    print("─" * 47)
    for tabla, total in rows:
        print(f"{'✅' if total > 0 else '❌'} {tabla:<33} {total:>10,}")

engine.dispose()
print("\n🏁 Pipeline finalizado.")
