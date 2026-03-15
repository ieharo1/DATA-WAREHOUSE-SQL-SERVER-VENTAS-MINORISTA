# 🏪 DATA WAREHOUSE CON SQL SERVER - VENTAS MINORISTA

**Data Warehouse con SQL Server** es un proyecto educativo completo que implementa un almacén de datos para una tienda minorista, utilizando el modelo estrella con tablas de dimensiones, hechos, staging y procesos ETL.

> *"Los datos son el nuevo petróleo. El Data Warehouse es la refinería."*

---

## 🎯 ¿Qué es este Proyecto?

Este proyecto simula un entorno real de Business Intelligence para una cadena de tiendas minorista, incluyendo:

- **Modelo dimensional** completo (modelo estrella)
- **Tablas de dimensiones** con Slowly Changing Dimensions (SCD Tipo 2)
- **Tablas de hechos** para ventas, inventario y presupuesto
- **Área de staging** para carga de datos crudos
- **Procesos ETL** automatizados con procedimientos almacenados
- **Consultas analíticas** para reportes de negocio

---

## 📚 ¿Qué Aprenderás?

### 🏗️ Modelado Dimensional
- Diseño de esquemas estrella y copo de nieve
- Dimensiones conformadas y jerarquías
- Tablas de hechos con diferentes granularidades
- Slowly Changing Dimensions (SCD) Tipo 1 y 2

### 📊 Tablas de Dimensión
- Dimensión Tiempo (fecha calendario)
- Dimensión Producto con atributos jerárquicos
- Dimensión Cliente con segmentación
- Dimensión Tienda con ubicación geográfica
- Dimensión Empleado con estructura organizacional

### 📈 Tablas de Hechos
- Hechos de ventas (transaccional)
- Hechos de inventario (periódico)
- Hechos de presupuesto (planificación)
- Métricas y KPIs calculados

### 🔄 Procesos ETL
- Extracción desde sistemas origen (POS, ERP, CRM)
- Transformación y limpieza de datos
- Carga incremental y completa
- Manejo de SCD Tipo 2
- Logging y auditoría de procesos

### 📉 Consultas Analíticas
- Reportes de ventas por período
- Análisis de categorías y productos
- Segmentación de clientes
- Comparativos y tendencias
- Matriz BCG de productos

---

## 🗂️ Estructura del Proyecto

```
Estructura-de-datos-C-Sharp/
├── schema/
│   └── 01_schema_completo.sql        # Creación de todas las tablas
├── staging/
│   └── 01_poblar_dimensiones.sql     # Carga de datos de ejemplo
├── etl/
│   └── 01_procedimientos_etl.sql     # Procesos ETL automatizados
├── analytics/
│   └── 01_consultas_analiticas.sql   # Reportes y KPIs
└── README.md
```

---

## 🛠️ Cómo Ejecutar los Scripts

### Requisitos Previos

- **SQL Server 2016** o superior (Express, Developer, Enterprise)
- **SQL Server Management Studio (SSMS)** o **Azure Data Studio**
- Permisos de creación de bases de datos

### Paso a Paso

#### 1. Crear el Esquema del Data Warehouse

```sql
-- Ejecutar: schema/01_schema_completo.sql
-- Crea:
--   • Base de datos DW_VentasMinorista
--   • Esquemas: dim, fact, stg, etl
--   • 5 Dimensiones: Time, Producto, Cliente, Tienda, Empleado
--   • 3 Hechos: VentasDiarias, InventarioDiario, PresupuestoMensual
--   • 4 Staging: VentasPOS, ProductosERP, ClientesCRM, TiendasCorp
```

#### 2. Poblar Dimensiones con Datos

```sql
-- Ejecutar: staging/01_poblar_dimensiones.sql
-- Genera:
--   • DimTime: 2020-2030 (4,018 registros)
--   • DimProducto: 16 productos de ejemplo
--   • DimCliente: 1,000 clientes
--   • DimTienda: 8 tiendas
--   • DimEmpleado: 200 empleados
--   • FactVentasDiarias: ~36,000 ventas (1 año)
```

#### 3. Configurar Procesos ETL

```sql
-- Ejecutar: etl/01_procedimientos_etl.sql
-- Crea procedimientos:
--   • sp_CargarVentas: Carga hechos de ventas
--   • sp_CargarProductos: SCD Tipo 2 para productos
--   • sp_CargarClientes: SCD Tipo 2 para clientes
--   • sp_CargarTiendas: SCD Tipo 2 para tiendas
--   • sp_EjecutarETLCompleto: Orquestador principal
```

#### 4. Ejecutar Consultas Analíticas

```sql
-- Ejecutar: analytics/01_consultas_analiticas.sql
-- Incluye 10 análisis:
--   • Ventas por período (mes, trimestre, año)
--   • Ventas por categoría y producto
--   • Ventas por tienda y región
--   • Segmentación de clientes
--   • KPIs diarios y por día de semana
--   • Tendencias y comparativos
--   • Análisis de inventario
--   • Ventas vs Presupuesto
--   • Matriz BCG de productos
```

---

## 📝 Ejemplos de Uso

### Ejemplo 1: Cargar Datos en Staging y Ejecutar ETL

```sql
-- 1. Insertar datos de ejemplo en staging
INSERT INTO stg.VentasPOS (
    FechaVenta, TicketID, LineaTicket, ProductoID,
    Cantidad, PrecioUnitario, Descuento, Impuesto, TotalVenta,
    ClienteID, TiendaID, EmpleadoID, SistemaOrigen
)
VALUES (
    GETDATE(),
    'TCK-20250115-00001',
    1,
    1,
    2,
    899.99,
    0,
    107.99,
    1007.98,
    1,
    1,
    1,
    'POS_TIENDA1'
);

-- 2. Ejecutar ETL completo
DECLARE @RegProc INT, @RegErr INT;
EXEC etl.sp_CargarVentas @LoteCarga = 'CargaManual_001', 
                         @RegistrosProcesados = @RegProc OUTPUT,
                         @RegistrosError = @RegErr OUTPUT;
PRINT 'Procesados: ' + CAST(@RegProc AS VARCHAR) + ', Errores: ' + CAST(@RegErr AS VARCHAR);
```

### Ejemplo 2: Consulta de Ventas Mensuales

```sql
SELECT 
    t.Anio,
    t.Mes,
    t.NombreMes,
    COUNT(DISTINCT f.TicketID) AS Tickets,
    SUM(f.TotalVenta) AS Ventas,
    SUM(f.Margen) AS Margen,
    AVG(f.TotalVenta) AS TicketPromedio
FROM fact.VentasDiarias f
INNER JOIN dim.Time t ON f.TiempoKey = t.TimeKey
GROUP BY t.Anio, t.Mes, t.NombreMes
ORDER BY t.Anio, t.Mes;
```

### Ejemplo 3: Top 10 Productos Más Vendidos

```sql
SELECT TOP 10
    p.Nombre,
    p.Categoria,
    SUM(f.Cantidad) AS CantidadVendida,
    SUM(f.TotalVenta) AS VentasTotales,
    SUM(f.Margen) AS MargenTotal
FROM fact.VentasDiarias f
INNER JOIN dim.Producto p ON f.ProductoKey = p.ProductoKey
WHERE p.EsActual = 1
GROUP BY p.Nombre, p.Categoria
ORDER BY CantidadVendida DESC;
```

### Ejemplo 4: KPIs por Día de Semana

```sql
SELECT 
    t.NombreDiaSemana,
    COUNT(DISTINCT f.TicketID) AS Tickets,
    SUM(f.TotalVenta) AS Ventas,
    AVG(f.TotalVenta) AS TicketPromedio,
    CAST(COUNT(DISTINCT f.TicketID) * 100.0 / 
        (SELECT COUNT(DISTINCT TicketID) FROM fact.VentasDiarias) AS DECIMAL(10,2)) AS Porcentaje
FROM fact.VentasDiarias f
INNER JOIN dim.Time t ON f.TiempoKey = t.TimeKey
GROUP BY t.DiaSemana, t.NombreDiaSemana
ORDER BY t.DiaSemana;
```

---

## 🏗️ Modelo Dimensional

### Dimensiones

| Dimensión | Registros | Atributos Principales |
|-----------|-----------|----------------------|
| **Time** | 4,018 | Fecha, Día, Mes, Año, Trimestre, Semana |
| **Producto** | 16+ | SKU, Nombre, Categoría, Marca, Precio |
| **Cliente** | 1,000+ | Nombre, Email, Segmento, Ciudad |
| **Tienda** | 8 | Código, Nombre, Ciudad, Región, Tipo |
| **Empleado** | 200 | Nombre, Cargo, Departamento, Tienda |

### Tablas de Hechos

| Hecho | Granularidad | Métricas |
|-------|-------------|----------|
| **VentasDiarias** | Producto × Día × Tienda | Cantidad, Venta, Descuento, Margen |
| **InventarioDiario** | Producto × Día × Tienda | Stock, Entradas, Salidas, Mermas |
| **PresupuestoMensual** | Categoría × Mes × Tienda | VentasPresupuestadas, MargenPresupuestado |

---

## 🔄 Slowly Changing Dimensions (SCD)

### Tipo 1: Sobrescribir

El valor anterior se pierde. Usado para correcciones de errores.

```sql
UPDATE dim.Producto
SET PrecioVenta = @NuevoPrecio
WHERE SKU = @SKU;
```

### Tipo 2: Nueva Fila

Mantiene histórico creando nueva fila con fechas de vigencia.

```sql
-- 1. Cerrar versión anterior
UPDATE dim.Producto
SET FechaFin = DATEADD(DAY, -1, GETDATE()),
    EsActual = 0
WHERE SKU = @SKU AND EsActual = 1;

-- 2. Insertar nueva versión
INSERT INTO dim.Producto (
    SKU, Nombre, PrecioVenta, FechaInicio, FechaFin, EsActual
)
VALUES (
    @SKU, @Nombre, @PrecioVenta, GETDATE(), NULL, 1
);
```

---

## 📊 KPIs Incluidos

### Ventas

- **Ventas Totales**: Suma de todas las ventas
- **Ticket Promedio**: Venta total / número de tickets
- **Productos por Ticket**: Cantidad total / número de tickets
- **Margen Porcentaje**: (Margen / Venta) × 100

### Clientes

- **Clientes Únicos**: Count distinct de clientes
- **Valor por Cliente**: Venta total / clientes únicos
- **Frecuencia de Compra**: Tickets / clientes únicos

### Inventario

- **Rotación de Inventario**: Ventas / Stock promedio
- **Días de Inventario**: Stock / (Ventas / 30)
- **Mermas Porcentaje**: Mermas / Stock inicial × 100

---

## 🔧 Procedimientos ETL

### sp_CargarVentas

Carga datos desde staging a la tabla de hechos.

```sql
DECLARE @RegProc INT, @RegErr INT;
EXEC etl.sp_CargarVentas 
    @LoteCarga = 'ETL_20250115',
    @RegistrosProcesados = @RegProc OUTPUT,
    @RegistrosError = @RegErr OUTPUT;
```

### sp_CargarProductos (SCD Tipo 2)

Gestiona cambios en productos manteniendo histórico.

```sql
DECLARE @RegIns INT, @RegAct INT;
EXEC etl.sp_CargarProductos 
    @LoteCarga = 'ETL_20250115',
    @RegistrosInsertados = @RegIns OUTPUT,
    @RegistrosActualizados = @RegAct OUTPUT;
```

### sp_EjecutarETLCompleto

Orquestador que ejecuta todos los procesos ETL.

```sql
EXEC etl.sp_EjecutarETLCompleto @LoteCarga = 'ETL_DIARIO_20250115';
```

---

## 📈 Consultas Analíticas Destacadas

### Ventas vs Presupuesto

```sql
SELECT 
    t.Anio, t.Mes,
    SUM(f.TotalVenta) AS VentasReales,
    SUM(p.VentasPresupuestadas) AS VentasPresupuestadas,
    (SUM(f.TotalVenta) - SUM(p.VentasPresupuestadas)) * 100.0 / 
        SUM(p.VentasPresupuestadas) AS CumplimientoPorcentaje
FROM fact.VentasDiarias f
INNER JOIN dim.Time t ON f.TiempoKey = t.TimeKey
LEFT JOIN fact.PresupuestoMensual p ON t.TimeKey = p.TiempoKey
GROUP BY t.Anio, t.Mes;
```

### Matriz BCG de Productos

Clasifica productos en: Estrella, Vaca, Interrogante, Perro.

```sql
-- Ver consulta completa en analytics/01_consultas_analiticas.sql
-- Sección 9: Productos Estrella, Vaca, Interrogante y Perro
```

---

## 🎓 Conceptos Clave

### Modelo Estrella

- Una tabla de hechos central
- Múltiples tablas de dimensiones conectadas
- Desnormalización controlada para rendimiento

### Grain (Granularidad)

- Nivel de detalle de la tabla de hechos
- Ejemplo: Una fila por producto vendido por día por tienda

### Dimensiones Conformadas

- Dimensiones compartidas entre múltiples hechos
- Permiten análisis cruzado entre diferentes áreas

### Slowly Changing Dimension

- Estrategias para manejar cambios en dimensiones
- Tipo 1: Sobrescribir, Tipo 2: Nueva fila, Tipo 3: Nueva columna

---

## ⚠️ Mejores Prácticas

### Diseño

- Definir claramente la granularidad de cada hecho
- Usar claves subrogadas (surrogate keys) en dimensiones
- Incluir fechas de vigencia para SCD Tipo 2

### ETL

- Validar datos antes de cargar
- Registrar todos los procesos en tablas de log
- Implementar manejo de errores robusto

### Rendimiento

- Indexar claves foráneas de hechos
- Considerar particionamiento para tablas grandes
- Usar columnstore indexes para consultas analíticas

---

## 📖 Recursos Adicionales

### Libros Recomendados

- **The Data Warehouse Toolkit** - Ralph Kimball
- **Star Schema: The Complete Reference** - Christopher Adamson
- **SQL Server Integration Services** - Brian Knight

### Herramientas

- **SQL Server Integration Services (SSIS)** - ETL visual
- **SQL Server Analysis Services (SSAS)** - Cubos OLAP
- **Power BI** - Visualización de datos

---

## 🧪 Ejercicios Prácticos

### Nivel Básico

1. Ejecutar script de creación de esquema
2. Poblar dimensiones con datos de ejemplo
3. Consultar ventas por mes

### Nivel Intermedio

1. Implementar carga incremental en staging
2. Ejecutar ETL completo
3. Crear reporte de KPIs por tienda

### Nivel Avanzado

1. Implementar SCD Tipo 2 para todas las dimensiones
2. Crear vista analítica consolidada
3. Diseñar dashboard ejecutivo

---

## 👨‍💻 Desarrollado por Isaac Esteban Haro Torres

**Ingeniero en Sistemas · Full Stack · Automatización · Data**

- 📧 Email: zackharo1@gmail.com
- 📱 WhatsApp: 098805517
- 💻 GitHub: https://github.com/ieharo1
- 🌐 Portafolio: https://ieharo1.github.io/portafolio-isaac.haro/

---

© 2026 Isaac Esteban Haro Torres - Todos los derechos reservados.
