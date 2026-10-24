"""Métricas de calidad de datos (Actividad A1).

Todo se calcula a partir de los CSV crudos, antes de cualquier filtro del ETL,
para que las métricas describan los datos y no el comportamiento del código.

Supuestos que el grupo debe poder defender en la presentación:
  * Ventana de análisis: 2023-01-01 a 2024-12-31 (24 meses del enunciado).
  * Una conversión es "creíble" si el cliente compra dentro de los
    VENTANA_CONVERSION_DIAS días posteriores al envío de la campaña.
  * Un tiempo de respuesta de CRM mayor a UMBRAL_RESPUESTA_HRS (una semana)
    se marca como atípico.
"""

from typing import Final

import pandas as pd

VENTANA_INICIO: Final = pd.Timestamp("2023-01-01")
VENTANA_FIN: Final = pd.Timestamp("2024-12-31")
VENTANA_CONVERSION_DIAS: Final = 30
UMBRAL_RESPUESTA_HRS: Final = 168

DATASETS: Final = (
    "clientes",
    "productos",
    "transacciones",
    "devoluciones",
    "interacciones_crm",
    "campanas_marketing",
)

COLUMNA_FECHA: Final = {
    "clientes": "fecha_registro",
    "transacciones": "fecha",
    "devoluciones": "fecha_devolucion",
    "interacciones_crm": "fecha",
    "campanas_marketing": "fecha_envio",
}


# --------------------------------------------------------------------------
# 1. Perfil del CSV crudo: nulos, duplicados y outliers por columna
# --------------------------------------------------------------------------
def perfil_crudo(df: pd.DataFrame) -> dict:
    """Cuenta celdas nulas, filas duplicadas y outliers (IQR) por columna."""
    outliers: dict[str, tuple[int, float, float]] = {}

    for col in df.select_dtypes(include="number").columns:
        s = df[col].dropna()
        if s.nunique() <= 2:  # columnas 0/1: el IQR no tiene sentido
            continue
        q1, q3 = s.quantile(0.25), s.quantile(0.75)
        iqr = q3 - q1
        if iqr == 0:
            continue
        inferior, superior = q1 - 1.5 * iqr, q3 + 1.5 * iqr
        n = int(((s < inferior) | (s > superior)).sum())
        outliers[col] = (n, float(inferior), float(superior))

    return {
        "celdas": int(df.shape[0] * df.shape[1]),
        "celdas_nulas": int(df.isna().sum().sum()),
        "filas_duplicadas": int(df.duplicated().sum()),
        "outliers": outliers,
    }


# --------------------------------------------------------------------------
# 2. Carga de los CSV crudos
# --------------------------------------------------------------------------
def cargar_crudos(csv_dir: str) -> dict[str, pd.DataFrame]:
    datos = {n: pd.read_csv(f"{csv_dir}/{n}.csv") for n in DATASETS}
    for nombre, col in COLUMNA_FECHA.items():
        datos[nombre][col] = pd.to_datetime(datos[nombre][col], errors="coerce")
    return datos


# --------------------------------------------------------------------------
# 3. Reglas de negocio entre columnas y entre tablas
# --------------------------------------------------------------------------
def evaluar_reglas(d: dict[str, pd.DataFrame]):
    """Devuelve (tabla de reglas, consistencia por dataset).

    Cada regla produce una máscara booleana (True = la fila viola la regla).
    severidad "critica": el dato es lógicamente imposible o rompe una relación.
    severidad "advertencia": el dato es sospechoso pero puede ser real.
    """
    c, p, t = d["clientes"], d["productos"], d["transacciones"]
    dv, i, m = d["devoluciones"], d["interacciones_crm"], d["campanas_marketing"]

    reg_cliente = c.drop_duplicates("cliente_id").set_index("cliente_id")["fecha_registro"]
    trx = t.drop_duplicates("transaccion_id").set_index("transaccion_id")
    prod = p.drop_duplicates("producto_id").set_index("producto_id")

    reglas: list[tuple[str, str, str, pd.Series, str]] = []

    def add(dataset, nombre, severidad, mascara, accion):
        reglas.append((dataset, nombre, severidad, pd.Series(mascara, index=d[dataset].index).fillna(False).astype(bool), accion))

    # --- Integridad referencial (crítica) ---------------------------------
    add("transacciones", "FK: cliente_id existe en clientes", "critica",
        ~t["cliente_id"].isin(c["cliente_id"]), "Excluir de las tablas clean")
    add("transacciones", "FK: producto_id existe en productos", "critica",
        ~t["producto_id"].isin(p["producto_id"]), "Excluir de las tablas clean")
    add("devoluciones", "FK: transaccion_id existe en transacciones", "critica",
        ~dv["transaccion_id"].isin(t["transaccion_id"]), "Excluir de las tablas clean")
    add("devoluciones", "FK: cliente_id existe en clientes", "critica",
        ~dv["cliente_id"].isin(c["cliente_id"]), "Excluir de las tablas clean")
    add("interacciones_crm", "FK: cliente_id existe en clientes", "critica",
        ~i["cliente_id"].isin(c["cliente_id"]), "Excluir de las tablas clean")
    add("campanas_marketing", "FK: cliente_id existe en clientes", "critica",
        ~m["cliente_id"].isin(c["cliente_id"]), "Excluir de las tablas clean")

    # --- Coherencia de montos y precios (crítica) --------------------------
    esperado = t["cantidad"] * t["precio_unitario"] * (1 - t["descuento"])
    add("transacciones", "monto_neto = cantidad x precio x (1 - descuento)", "critica",
        (esperado - t["monto_neto"]).abs() > 0.05, "Recalcular monto_neto o excluir")
    precio_lista = t["producto_id"].map(prod["precio_lista"])
    add("transacciones", "precio_unitario = precio_lista del producto", "critica",
        (t["precio_unitario"] - precio_lista).abs() > 0.01, "Revisar origen del precio")
    margen_calc = (p["precio_lista"] - p["costo"]) / p["precio_lista"] * 100
    add("productos", "margen_bruto_pct = (precio - costo) / precio", "critica",
        (margen_calc - p["margen_bruto_pct"]).abs() > 0.2, "Recalcular el margen")

    # --- Coherencia de devoluciones (crítica) -----------------------------
    existe = dv["transaccion_id"].isin(trx.index)
    add("devoluciones", "fecha_devolucion no es anterior a la compra", "critica",
        dv["fecha_devolucion"] < dv["transaccion_id"].map(trx["fecha"]), "Excluir de las tablas clean")
    add("devoluciones", "el cliente de la devolución es el de la compra", "critica",
        existe & (dv["cliente_id"] != dv["transaccion_id"].map(trx["cliente_id"])), "Excluir de las tablas clean")
    add("devoluciones", "monto_devuelto no supera el monto de la compra", "critica",
        dv["monto_devuelto"] > dv["transaccion_id"].map(trx["monto_neto"]) + 0.01, "Limitar al monto de la compra")

    # --- Reglas lógicas sospechosas (advertencia) --------------------------
    add("transacciones", "compra no es anterior al registro del cliente", "advertencia",
        t["fecha"] < t["cliente_id"].map(reg_cliente),
        "Marcar; no usar fecha_registro para antigüedad (usar primera compra)")
    add("campanas_marketing", "campaña no es anterior al registro del cliente", "advertencia",
        m["fecha_envio"] < m["cliente_id"].map(reg_cliente), "Marcar; documentar")

    conv = m.loc[m["convertido"] == 1, ["cliente_id", "fecha_envio"]].reset_index(names="_i")
    j = conv.merge(t[["cliente_id", "fecha"]], on="cliente_id")
    en_ventana = (j["fecha"] >= j["fecha_envio"]) & (
        j["fecha"] <= j["fecha_envio"] + pd.Timedelta(days=VENTANA_CONVERSION_DIAS)
    )
    con_compra = j.loc[en_ventana, "_i"].unique()
    add("campanas_marketing",
        f"convertido=1 con compra en los {VENTANA_CONVERSION_DIAS} días siguientes", "advertencia",
        (m["convertido"] == 1) & ~m.index.isin(con_compra),
        "Marcar; en A4 medir conversión con compras reales, no con la bandera")
    add("campanas_marketing", "convertido=1 implica abierto=1", "advertencia",
        (m["convertido"] == 1) & (m["abierto"] == 0), "Marcar; documentar")
    add("clientes", "el cliente tiene al menos una transacción", "advertencia",
        ~c["cliente_id"].isin(t["cliente_id"]), "Mantener; estado 'Sin compras' en churn")
    add("interacciones_crm", f"tiempo_respuesta_hrs <= {UMBRAL_RESPUESTA_HRS}", "advertencia",
        i["tiempo_respuesta_hrs"] > UMBRAL_RESPUESTA_HRS, "Mantener; analizar como outlier de servicio")

    # --- Tabla de reglas y consistencia por dataset ------------------------
    filas = [
        {
            "dataset": ds,
            "regla": nombre,
            "severidad": sev,
            "filas_evaluadas": len(mask),
            "violaciones": int(mask.sum()),
            "pct_violaciones": round(100 * mask.mean(), 2),
            "accion_sugerida": accion,
        }
        for ds, nombre, sev, mask, accion in reglas
    ]

    todas = {ds: pd.Series(False, index=d[ds].index) for ds in DATASETS}
    criticas = {ds: pd.Series(False, index=d[ds].index) for ds in DATASETS}
    for ds, _, sev, mask, _ in reglas:
        todas[ds] = todas[ds] | mask
        if sev == "critica":
            criticas[ds] = criticas[ds] | mask

    consistencia = {
        ds: (round(100 * (1 - todas[ds].mean()), 2), round(100 * (1 - criticas[ds].mean()), 2))
        for ds in DATASETS
    }
    return pd.DataFrame(filas), consistencia


# --------------------------------------------------------------------------
# 4. Oportunidad: % de registros dentro de la ventana de análisis
# --------------------------------------------------------------------------
def oportunidad(d: dict[str, pd.DataFrame]) -> dict[str, float | None]:
    resultado: dict[str, float | None] = {}
    for ds in DATASETS:
        col = COLUMNA_FECHA.get(ds)
        if col is None:  # productos no tiene fecha
            resultado[ds] = None
            continue
        dentro = d[ds][col].between(VENTANA_INICIO, VENTANA_FIN)
        resultado[ds] = round(100 * dentro.mean(), 2)
    return resultado
