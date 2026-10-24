import duckdb
from os import getenv

DB_PATH: str = getenv("DB_PATH", "./data/out/retaild_warehouse.duckdb")


class SingletonDbConnection:
    _instance: duckdb.DuckDBPyConnection | None = None

    @classmethod
    def get_connection(cls) -> duckdb.DuckDBPyConnection:
        if cls._instance is None:
            cls._instance = duckdb.connect(DB_PATH)
        return cls._instance


def _create_dim():
    conn = SingletonDbConnection.get_connection()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS dim_cliente (
            sk_cliente INTEGER PRIMARY KEY,
            cliente_id VARCHAR NOT NULL UNIQUE,
            edad INTEGER CHECK (edad >= 0),
            genero VARCHAR CHECK (genero IN ('M', 'F', 'ND')),
            ciudad VARCHAR,
            segmento VARCHAR,
            fecha_registro DATE,
            canal_adquisicion VARCHAR
        );
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS dim_producto (
            sk_producto INTEGER PRIMARY KEY,
            producto_id VARCHAR NOT NULL UNIQUE,
            nombre VARCHAR,
            categoria VARCHAR,
            subcategoria VARCHAR,
            costo DECIMAL(10, 2) CHECK (costo >= 0.00),
            precio_lista DECIMAL(10, 2) CHECK (precio_lista >= 0.00),
            margen_bruto_pct DECIMAL(5, 2) CHECK (margen_bruto_pct >= 0.0)
        );
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS dim_tiempo (
            sk_tiempo INTEGER PRIMARY KEY,
            fecha DATE NOT NULL UNIQUE,
            dia INTEGER,
            mes INTEGER,
            nombre_mes VARCHAR,
            trimestre INTEGER,
            anio INTEGER,
            dia_semana VARCHAR
        );
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS fact_transacciones (
            sk_transaccion BIGINT PRIMARY KEY,
            sk_cliente INTEGER NOT NULL,
            sk_producto INTEGER NOT NULL,
            sk_tiempo INTEGER NOT NULL,
            transaccion_id VARCHAR NOT NULL,
            canal VARCHAR,
            metodo_pago VARCHAR,
            cantidad INTEGER CHECK (cantidad > 0),
            precio_unitario DECIMAL(10, 2) CHECK (precio_unitario >= 0.00),
            descuento DECIMAL(5, 3) CHECK (descuento >= 0.000 AND descuento <= 1.000),
            monto_neto DECIMAL(10, 2) CHECK (monto_neto >= 0.00),
            FOREIGN KEY (sk_cliente) REFERENCES dim_cliente(sk_cliente),
            FOREIGN KEY (sk_producto) REFERENCES dim_producto(sk_producto),
            FOREIGN KEY (sk_tiempo) REFERENCES dim_tiempo(sk_tiempo)
        );
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS fact_devoluciones (
            sk_devolucion BIGINT PRIMARY KEY,
            sk_cliente INTEGER NOT NULL,
            sk_producto INTEGER NOT NULL,
            sk_tiempo INTEGER NOT NULL,
            devolucion_id VARCHAR NOT NULL,
            transaccion_id VARCHAR NOT NULL,
            motivo VARCHAR,
            monto_devuelto DECIMAL(10, 2) CHECK (monto_devuelto >= 0.00),
            FOREIGN KEY (sk_cliente) REFERENCES dim_cliente(sk_cliente),
            FOREIGN KEY (sk_producto) REFERENCES dim_producto(sk_producto),
            FOREIGN KEY (sk_tiempo) REFERENCES dim_tiempo(sk_tiempo)
        );
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS fact_campanas_marketing (
            sk_campana_evento BIGINT PRIMARY KEY,
            sk_cliente INTEGER NOT NULL,
            sk_tiempo INTEGER NOT NULL,
            campana_id VARCHAR NOT NULL,
            tipo_campana VARCHAR,
            canal_campana VARCHAR,
            abierto BOOLEAN,
            convertido BOOLEAN,
            descuento_ofrecido DECIMAL(10, 2) CHECK (descuento_ofrecido >= 0.00),
            FOREIGN KEY (sk_cliente) REFERENCES dim_cliente(sk_cliente),
            FOREIGN KEY (sk_tiempo) REFERENCES dim_tiempo(sk_tiempo)
        );
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS fact_interacciones_crm (
            sk_interaccion BIGINT PRIMARY KEY,
            sk_cliente INTEGER NOT NULL,
            sk_tiempo INTEGER NOT NULL,
            interaccion_id VARCHAR NOT NULL,
            tipo_interaccion VARCHAR,
            canal VARCHAR,
            resolucion VARCHAR,
            tiempo_respuesta_hrs DECIMAL(10, 2) CHECK (tiempo_respuesta_hrs >= 0.0),
            FOREIGN KEY (sk_cliente) REFERENCES dim_cliente(sk_cliente),
            FOREIGN KEY (sk_tiempo) REFERENCES dim_tiempo(sk_tiempo)
        );
    """)


def _create_stg():
    conn = SingletonDbConnection.get_connection()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stg_clientes (
            cliente_id VARCHAR PRIMARY KEY,
            edad INTEGER,
            genero VARCHAR,
            ciudad VARCHAR,
            segmento VARCHAR,
            fecha_registro DATE,
            canal_adquisicion VARCHAR
        );
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS stg_productos (
            producto_id VARCHAR PRIMARY KEY,
            nombre VARCHAR,
            categoria VARCHAR,
            subcategoria VARCHAR,
            costo DECIMAL(10, 2),
            precio_lista DECIMAL(10, 2),
            margen_bruto_pct DECIMAL(5, 2)
        );
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS stg_transacciones (
            transaccion_id VARCHAR PRIMARY KEY,
            cliente_id VARCHAR NOT NULL,
            fecha DATE,
            producto_id VARCHAR NOT NULL,
            cantidad INTEGER,
            precio_unitario DECIMAL(10, 2),
            descuento DECIMAL(5, 3),
            monto_neto DECIMAL(10, 2),
            canal VARCHAR,
            metodo_pago VARCHAR
        );
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS stg_campanas_marketing (
            campana_id VARCHAR PRIMARY KEY,
            cliente_id VARCHAR NOT NULL,
            tipo_campana VARCHAR,
            fecha_envio DATE,
            canal_campana VARCHAR,
            abierto BOOLEAN,
            convertido BOOLEAN,
            descuento_ofrecido DECIMAL(10, 2)
        );
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS stg_interacciones_crm (
            interaccion_id VARCHAR PRIMARY KEY,
            cliente_id VARCHAR NOT NULL,
            tipo VARCHAR,
            fecha DATE,
            canal VARCHAR,
            resolucion VARCHAR,
            tiempo_respuesta_hrs DECIMAL(10, 2)
        );
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS stg_devoluciones (
            devolucion_id VARCHAR PRIMARY KEY,
            transaccion_id VARCHAR NOT NULL,
            cliente_id VARCHAR NOT NULL,
            motivo VARCHAR,
            monto_devuelto DECIMAL(10, 2),
            fecha_devolucion DATE
        );
    """)


def _create_clean():
    conn = SingletonDbConnection.get_connection()

    conn.execute("""
        CREATE TABLE IF NOT EXISTS clean_clientes (
            cliente_id VARCHAR PRIMARY KEY,
            edad INTEGER,
            genero VARCHAR CHECK (genero IN ('M', 'F', 'ND')),
            ciudad VARCHAR,
            segmento VARCHAR,
            fecha_registro DATE,
            canal_adquisicion VARCHAR
        );
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS clean_productos (
            producto_id VARCHAR PRIMARY KEY,
            nombre VARCHAR,
            categoria VARCHAR,
            subcategoria VARCHAR,
            costo DECIMAL(10, 2) CHECK (costo >= 0.00),
            precio_lista DECIMAL(10, 2) CHECK (precio_lista >= 0.00),
            margen_bruto_pct DECIMAL(5, 2) CHECK (margen_bruto_pct >= 0.0)
        );
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS clean_campanas_marketing (
            campana_id VARCHAR PRIMARY KEY,
            cliente_id VARCHAR NOT NULL,
            tipo_campana VARCHAR,
            fecha_envio DATE,
            canal_campana VARCHAR,
            abierto BOOLEAN,
            convertido BOOLEAN,
            descuento_ofrecido DECIMAL(10, 2) CHECK (descuento_ofrecido >= 0.00 AND descuento_ofrecido <= 1.00),
            FOREIGN KEY (cliente_id) REFERENCES clean_clientes(cliente_id)
        );
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS clean_interacciones_crm (
            interaccion_id VARCHAR PRIMARY KEY,
            cliente_id VARCHAR NOT NULL,
            tipo VARCHAR,
            fecha DATE,
            canal VARCHAR,
            resolucion VARCHAR,
            tiempo_respuesta_hrs DECIMAL(10, 2) CHECK (tiempo_respuesta_hrs >= 0.0),
            FOREIGN KEY (cliente_id) REFERENCES clean_clientes(cliente_id)
        );
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS clean_transacciones (
            transaccion_id VARCHAR PRIMARY KEY,
            cliente_id VARCHAR NOT NULL,
            fecha DATE,
            producto_id VARCHAR NOT NULL,
            cantidad INTEGER CHECK (cantidad > 0),
            precio_unitario DECIMAL(10, 2) CHECK (precio_unitario >= 0.00),
            descuento DECIMAL(5, 3) CHECK (descuento >= 0.000 AND descuento <= 1.000),
            monto_neto DECIMAL(10, 2) CHECK (monto_neto >= 0.00),
            canal VARCHAR,
            metodo_pago VARCHAR,
            FOREIGN KEY (cliente_id) REFERENCES clean_clientes(cliente_id),
            FOREIGN KEY (producto_id) REFERENCES clean_productos(producto_id)
        );
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS clean_devoluciones (
            devolucion_id VARCHAR PRIMARY KEY,
            transaccion_id VARCHAR NOT NULL,
            cliente_id VARCHAR NOT NULL,
            motivo VARCHAR,
            monto_devuelto DECIMAL(10, 2) CHECK (monto_devuelto >= 0.00),
            fecha_devolucion DATE,
            FOREIGN KEY (transaccion_id) REFERENCES clean_transacciones(transaccion_id),
            FOREIGN KEY (cliente_id) REFERENCES clean_clientes(cliente_id)
        );
    """)


def create_etl_tables():
    _create_stg()
    _create_clean()
    _create_dim()
