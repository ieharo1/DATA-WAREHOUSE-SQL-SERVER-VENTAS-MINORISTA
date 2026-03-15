-- ============================================================================
-- ESQUEMA DE DATA WAREHOUSE - VENTAS MINORISTA
-- ============================================================================
-- Este script crea el esquema completo de un Data Warehouse para una tienda
-- minorista, incluyendo dimensiones, tablas de hechos y tablas de staging.
-- Modelo estrella optimizado para consultas analíticas.
-- ============================================================================

USE master;
GO

-- Crear base de datos del Data Warehouse
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'DW_VentasMinorista')
BEGIN
    CREATE DATABASE DW_VentasMinorista;
    PRINT 'Base de datos DW_VentasMinorista creada exitosamente.';
END
GO

USE DW_VentasMinorista;
GO

-- ============================================================================
-- ESQUEMAS LÓGICOS
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS dim;      -- Dimensiones
CREATE SCHEMA IF NOT EXISTS fact;     -- Tablas de hechos
CREATE SCHEMA IF NOT EXISTS stg;      -- Staging
CREATE SCHEMA IF NOT EXISTS etl;      -- Procesos ETL
GO

-- ============================================================================
-- DIMENSIÓN 1: TIEMPO (DimTime)
-- ============================================================================
-- Tabla de tiempo esencial para análisis temporal

IF OBJECT_ID('dim.Time', 'U') IS NOT NULL
    DROP TABLE dim.Time;
GO

CREATE TABLE dim.Time (
    TimeKey INT PRIMARY KEY,              -- YYYYMMDD formato clave
    Fecha DATE NOT NULL,
    Dia INT NOT NULL,
    Mes INT NOT NULL,
    Anio INT NOT NULL,
    Trimestre INT NOT NULL,
    Semestre INT NOT NULL,
    DiaSemana INT NOT NULL,               -- 1=Domingo, 7=Sábado
    NombreDiaSemana NVARCHAR(15) NOT NULL,
    NombreMes NVARCHAR(15) NOT NULL,
    NombreTrimestre NVARCHAR(20) NOT NULL,
    EsFinDeSemana BIT NOT NULL,
    EsFeriado BIT DEFAULT 0,
    NombreFeriado NVARCHAR(50),
    EsUltimoDiaMes BIT NOT NULL,
    EsUltimoDiaAnio BIT NOT NULL,
    SemanaISO INT NOT NULL,
    AnioFiscal INT,
    MesFiscal INT,
    FechaCreacion DATETIME DEFAULT GETDATE(),
    FechaActualizacion DATETIME DEFAULT GETDATE()
);
GO

-- Índices para DimTime
CREATE INDEX IX_Time_Anio ON dim.Time(Anio);
CREATE INDEX IX_Time_Mes ON dim.Time(Anio, Mes);
CREATE INDEX IX_Time_Trimestre ON dim.Time(Anio, Trimestre);
GO

-- ============================================================================
-- DIMENSIÓN 2: PRODUCTO (DimProducto)
-- ============================================================================

IF OBJECT_ID('dim.Producto', 'U') IS NOT NULL
    DROP TABLE dim.Producto;
GO

CREATE TABLE dim.Producto (
    ProductoKey INT IDENTITY(1,1) PRIMARY KEY,
    ProductoID INT NOT NULL,              -- ID del sistema transaccional
    SKU NVARCHAR(50) NOT NULL,
    Nombre NVARCHAR(200) NOT NULL,
    Descripcion NVARCHAR(500),
    Categoria NVARCHAR(100) NOT NULL,
    SubCategoria NVARCHAR(100),
    Marca NVARCHAR(100),
    Modelo NVARCHAR(50),
    Talla NVARCHAR(20),
    Color NVARCHAR(50),
    Peso DECIMAL(10,2),
    Dimensiones NVARCHAR(50),
    PrecioCosto DECIMAL(10,2),
    PrecioVenta DECIMAL(10,2),
    MargenGanancia DECIMAL(5,2),
    Proveedor NVARCHAR(200),
    Estado NVARCHAR(20) DEFAULT 'Activo',
    FechaInicio DATE NOT NULL,            -- Slowly Changing Dimension
    FechaFin DATE,
    EsActual BIT DEFAULT 1,
    Version INT DEFAULT 1,
    FechaCreacion DATETIME DEFAULT GETDATE(),
    FechaActualizacion DATETIME DEFAULT GETDATE()
);
GO

-- Índices para DimProducto
CREATE INDEX IX_Producto_Categoria ON dim.Producto(Categoria);
CREATE INDEX IX_Producto_Marca ON dim.Producto(Marca);
CREATE INDEX IX_Producto_Estado ON dim.Producto(Estado, EsActual);
CREATE UNIQUE INDEX IX_Producto_SKU_Actual ON dim.Producto(SKU) WHERE EsActual = 1;
GO

-- ============================================================================
-- DIMENSIÓN 3: CLIENTE (DimCliente)
-- ============================================================================

IF OBJECT_ID('dim.Cliente', 'U') IS NOT NULL
    DROP TABLE dim.Cliente;
GO

CREATE TABLE dim.Cliente (
    ClienteKey INT IDENTITY(1,1) PRIMARY KEY,
    ClienteID INT NOT NULL,               -- ID del sistema transaccional
    NumeroDocumento NVARCHAR(20),
    TipoDocumento NVARCHAR(20),
    NombreCompleto NVARCHAR(200) NOT NULL,
    PrimerNombre NVARCHAR(50),
    SegundoNombre NVARCHAR(50),
    PrimerApellido NVARCHAR(50),
    SegundoApellido NVARCHAR(50),
    Email NVARCHAR(255),
    Telefono NVARCHAR(20),
    FechaNacimiento DATE,
    Edad INT,
    Genero CHAR(1),                       -- M, F, O
    EstadoCivil NVARCHAR(20),
    NivelEducativo NVARCHAR(50),
    Ocupacion NVARCHAR(100),
    Segmento NVARCHAR(50),                -- VIP, Premium, Regular, Nuevo
    FechaRegistro DATE,
    FechaInicio DATE NOT NULL,
    FechaFin DATE,
    EsActual BIT DEFAULT 1,
    Version INT DEFAULT 1,
    FechaCreacion DATETIME DEFAULT GETDATE(),
    FechaActualizacion DATETIME DEFAULT GETDATE()
);
GO

-- Índices para DimCliente
CREATE INDEX IX_Cliente_Segmento ON dim.Cliente(Segmento);
CREATE INDEX IX_Cliente_Genero ON dim.Cliente(Genero);
CREATE INDEX IX_Cliente_Edad ON dim.Cliente(Edad);
CREATE INDEX IX_Cliente_Actual ON dim.Cliente(ClienteID, EsActual);
GO

-- ============================================================================
-- DIMENSIÓN 4: TIENDA (DimTienda)
-- ============================================================================

IF OBJECT_ID('dim.Tienda', 'U') IS NOT NULL
    DROP TABLE dim.Tienda;
GO

CREATE TABLE dim.Tienda (
    TiendaKey INT IDENTITY(1,1) PRIMARY KEY,
    TiendaID INT NOT NULL,                -- ID del sistema transaccional
    CodigoTienda NVARCHAR(20) NOT NULL,
    NombreTienda NVARCHAR(200) NOT NULL,
    TipoTienda NVARCHAR(50),              -- Flagship, Outlet, Franquicia
    Formato NVARCHAR(50),                 -- Grande, Mediano, Pequeño
    Direccion NVARCHAR(300),
    Ciudad NVARCHAR(100) NOT NULL,
    Provincia NVARCHAR(100) NOT NULL,
    Region NVARCHAR(100) NOT NULL,
    CodigoPostal NVARCHAR(20),
    Pais NVARCHAR(50) DEFAULT 'Ecuador',
    Latitud DECIMAL(10,8),
    Longitud DECIMAL(11,8),
    Telefono NVARCHAR(20),
    Email NVARCHAR(255),
    Gerente NVARCHAR(200),
    AreaVentas DECIMAL(10,2),             -- m²
    NumeroEmpleados INT,
    FechaApertura DATE,
    FechaCierre DATE,
    Estado NVARCHAR(20) DEFAULT 'Activa',
    FechaInicio DATE NOT NULL,
    FechaFin DATE,
    EsActual BIT DEFAULT 1,
    Version INT DEFAULT 1,
    FechaCreacion DATETIME DEFAULT GETDATE(),
    FechaActualizacion DATETIME DEFAULT GETDATE()
);
GO

-- Índices para DimTienda
CREATE INDEX IX_Tienda_Region ON dim.Tienda(Region);
CREATE INDEX IX_Tienda_Ciudad ON dim.Tienda(Ciudad);
CREATE INDEX IX_Tienda_Estado ON dim.Tienda(Estado, EsActual);
CREATE UNIQUE INDEX IX_Tienda_Codigo_Actual ON dim.Tienda(CodigoTienda) WHERE EsActual = 1;
GO

-- ============================================================================
-- DIMENSIÓN 5: EMPLEADO (DimEmpleado)
-- ============================================================================

IF OBJECT_ID('dim.Empleado', 'U') IS NOT NULL
    DROP TABLE dim.Empleado;
GO

CREATE TABLE dim.Empleado (
    EmpleadoKey INT IDENTITY(1,1) PRIMARY KEY,
    EmpleadoID INT NOT NULL,
    NumeroEmpleado NVARCHAR(20) NOT NULL,
    NombreCompleto NVARCHAR(200) NOT NULL,
    Cargo NVARCHAR(100),
    Departamento NVARCHAR(100),
    TiendaID INT,
    SupervisorID INT,
    FechaContratacion DATE,
    FechaTerminacion DATE,
    Estado NVARCHAR(20) DEFAULT 'Activo',
    FechaInicio DATE NOT NULL,
    FechaFin DATE,
    EsActual BIT DEFAULT 1,
    Version INT DEFAULT 1,
    FechaCreacion DATETIME DEFAULT GETDATE(),
    FechaActualizacion DATETIME DEFAULT GETDATE()
);
GO

-- Índices para DimEmpleado
CREATE INDEX IX_Empleado_Departamento ON dim.Empleado(Departamento);
CREATE INDEX IX_Empleado_Tienda ON dim.Empleado(TiendaID);
CREATE INDEX IX_Empleado_Estado ON dim.Empleado(Estado, EsActual);
GO

-- ============================================================================
-- TABLA DE HECHOS 1: VENTAS DIARIAS (FactVentasDiarias)
-- ============================================================================
-- Granularidad: Una fila por producto vendido por día por tienda

IF OBJECT_ID('fact.VentasDiarias', 'U') IS NOT NULL
    DROP TABLE fact.VentasDiarias;
GO

CREATE TABLE fact.VentasDiarias (
    VentasKey BIGINT IDENTITY(1,1) PRIMARY KEY,
    -- Claves foráneas a dimensiones
    TiempoKey INT NOT NULL,
    ProductoKey INT NOT NULL,
    ClienteKey INT,
    TiendaKey INT NOT NULL,
    EmpleadoKey INT,
    -- Claves del sistema transaccional
    TicketID NVARCHAR(50),
    LineaTicket INT,
    -- Métricas
    Cantidad DECIMAL(18,4) NOT NULL,
    PrecioUnitario DECIMAL(18,2) NOT NULL,
    Descuento DECIMAL(18,2) DEFAULT 0,
    Impuesto DECIMAL(18,2) DEFAULT 0,
    TotalVenta DECIMAL(18,2) NOT NULL,
    CostoTotal DECIMAL(18,2),
    Margen DECIMAL(18,2),
    -- Métricas calculadas
    MargenPorcentaje AS (CASE WHEN TotalVenta > 0 THEN (Margen / TotalVenta) * 100 ELSE 0 END),
    -- Auditoría
    FechaProceso DATETIME DEFAULT GETDATE(),
    LoteCarga NVARCHAR(50),
    Origen NVARCHAR(50) DEFAULT 'POS'
);
GO

-- Índices para FactVentasDiarias
CREATE INDEX IX_Ventas_Tiempo ON fact.VentasDiarias(TiempoKey);
CREATE INDEX IX_Ventas_Producto ON fact.VentasDiarias(ProductoKey);
CREATE INDEX IX_Ventas_Cliente ON fact.VentasDiarias(ClienteKey);
CREATE INDEX IX_Ventas_Tienda ON fact.VentasDiarias(TiendaKey);
CREATE INDEX IX_Ventas_Tienda_Tiempo ON fact.VentasDiarias(TiendaKey, TiempoKey);
CREATE INDEX IX_Ventas_Producto_Tiempo ON fact.VentasDiarias(ProductoKey, TiempoKey);
CREATE INDEX IX_Ventas_Ticket ON fact.VentasDiarias(TicketID);
GO

-- ============================================================================
-- TABLA DE HECHOS 2: INVENTARIO DIARIO (FactInventarioDiario)
-- ============================================================================

IF OBJECT_ID('fact.InventarioDiario', 'U') IS NOT NULL
    DROP TABLE fact.InventarioDiario;
GO

CREATE TABLE fact.InventarioDiario (
    InventarioKey BIGINT IDENTITY(1,1) PRIMARY KEY,
    TiempoKey INT NOT NULL,
    ProductoKey INT NOT NULL,
    TiendaKey INT NOT NULL,
    -- Métricas
    StockInicial INT NOT NULL,
    Entradas INT DEFAULT 0,
    Salidas INT DEFAULT 0,
    Ventas INT DEFAULT 0,
    Devoluciones INT DEFAULT 0,
    Ajustes INT DEFAULT 0,
    Mermas INT DEFAULT 0,
    StockFinal INT NOT NULL,
    StockSeguridad INT,
    StockMaximo INT,
    StockMinimo INT,
    ValorInventario DECIMAL(18,2),
    -- Auditoría
    FechaProceso DATETIME DEFAULT GETDATE(),
    LoteCarga NVARCHAR(50)
);
GO

-- Índices para FactInventarioDiario
CREATE INDEX IX_Inventario_Tiempo ON fact.InventarioDiario(TiempoKey);
CREATE INDEX IX_Inventario_Producto ON fact.InventarioDiario(ProductoKey);
CREATE INDEX IX_Inventario_Tienda ON fact.InventarioDiario(TiendaKey);
CREATE INDEX IX_Inventario_Tienda_Tiempo ON fact.InventarioDiario(TiendaKey, TiempoKey);
GO

-- ============================================================================
-- TABLA DE HECHOS 3: PRESUPUESTO MENSUAL (FactPresupuestoMensual)
-- ============================================================================

IF OBJECT_ID('fact.PresupuestoMensual', 'U') IS NOT NULL
    DROP TABLE fact.PresupuestoMensual;
GO

CREATE TABLE fact.PresupuestoMensual (
    PresupuestoKey BIGINT IDENTITY(1,1) PRIMARY KEY,
    TiempoKey INT NOT NULL,               -- Primer día del mes
    ProductoKey INT,                      -- NULL = todos los productos
    CategoriaKey INT,                     -- NULL = todas las categorías
    TiendaKey INT NOT NULL,
    -- Métricas
    VentasPresupuestadas DECIMAL(18,2) NOT NULL,
    CantidadPresupuestada DECIMAL(18,4),
    MargenPresupuestado DECIMAL(18,2),
    GastosOperativos DECIMAL(18,2),
    -- Auditoría
    FechaCreacion DATETIME DEFAULT GETDATE(),
    FechaActualizacion DATETIME DEFAULT GETDATE(),
    UsuarioResponsable NVARCHAR(100)
);
GO

-- Índices para FactPresupuestoMensual
CREATE INDEX IX_Presupuesto_Tiempo ON fact.PresupuestoMensual(TiempoKey);
CREATE INDEX IX_Presupuesto_Tienda ON fact.PresupuestoMensual(TiendaKey);
CREATE INDEX IX_Presupuesto_Categoria ON fact.PresupuestoMensual(CategoriaKey);
GO

-- ============================================================================
-- TABLAS DE STAGING
-- ============================================================================

-- Staging: Ventas desde sistema POS
IF OBJECT_ID('stg.VentasPOS', 'U') IS NOT NULL
    DROP TABLE stg.VentasPOS;
GO

CREATE TABLE stg.VentasPOS (
    StagingID BIGINT IDENTITY(1,1) PRIMARY KEY,
    -- Datos crudos del sistema origen
    FechaVenta DATETIME,
    HoraVenta TIME,
    TicketID NVARCHAR(50),
    LineaTicket INT,
    ProductoID INT,
    SKU NVARCHAR(50),
    Cantidad DECIMAL(18,4),
    PrecioUnitario DECIMAL(18,2),
    Descuento DECIMAL(18,2),
    Impuesto DECIMAL(18,2),
    TotalVenta DECIMAL(18,2),
    ClienteID INT,
    TiendaID INT,
    EmpleadoID INT,
    CajeroID INT,
    -- Metadatos
    SistemaOrigen NVARCHAR(50),
    FechaCarga DATETIME DEFAULT GETDATE(),
    Procesado BIT DEFAULT 0,
    Errores NVARCHAR(MAX),
    LoteCarga NVARCHAR(50)
);
GO

-- Staging: Productos desde ERP
IF OBJECT_ID('stg.ProductosERP', 'U') IS NOT NULL
    DROP TABLE stg.ProductosERP;
GO

CREATE TABLE stg.ProductosERP (
    StagingID BIGINT IDENTITY(1,1) PRIMARY KEY,
    ProductoID INT,
    SKU NVARCHAR(50),
    Nombre NVARCHAR(200),
    Descripcion NVARCHAR(500),
    Categoria NVARCHAR(100),
    SubCategoria NVARCHAR(100),
    Marca NVARCHAR(100),
    Modelo NVARCHAR(50),
    Talla NVARCHAR(20),
    Color NVARCHAR(50),
    PrecioCosto DECIMAL(10,2),
    PrecioVenta DECIMAL(10,2),
    Proveedor NVARCHAR(200),
    Estado NVARCHAR(20),
    FechaActualizacion DATETIME,
    -- Metadatos
    FechaCarga DATETIME DEFAULT GETDATE(),
    Procesado BIT DEFAULT 0,
    LoteCarga NVARCHAR(50)
);
GO

-- Staging: Clientes desde CRM
IF OBJECT_ID('stg.ClientesCRM', 'U') IS NOT NULL
    DROP TABLE stg.ClientesCRM;
GO

CREATE TABLE stg.ClientesCRM (
    StagingID BIGINT IDENTITY(1,1) PRIMARY KEY,
    ClienteID INT,
    NumeroDocumento NVARCHAR(20),
    TipoDocumento NVARCHAR(20),
    NombreCompleto NVARCHAR(200),
    Email NVARCHAR(255),
    Telefono NVARCHAR(20),
    FechaNacimiento DATE,
    Genero CHAR(1),
    Direccion NVARCHAR(300),
    Ciudad NVARCHAR(100),
    Provincia NVARCHAR(100),
    FechaRegistro DATE,
    Segmento NVARCHAR(50),
    -- Metadatos
    FechaCarga DATETIME DEFAULT GETDATE(),
    Procesado BIT DEFAULT 0,
    LoteCarga NVARCHAR(50)
);
GO

-- Staging: Tiendas desde sistema corporativo
IF OBJECT_ID('stg.TiendasCorp', 'U') IS NOT NULL
    DROP TABLE stg.TiendasCorp;
GO

CREATE TABLE stg.TiendasCorp (
    StagingID BIGINT IDENTITY(1,1) PRIMARY KEY,
    TiendaID INT,
    CodigoTienda NVARCHAR(20),
    NombreTienda NVARCHAR(200),
    TipoTienda NVARCHAR(50),
    Direccion NVARCHAR(300),
    Ciudad NVARCHAR(100),
    Provincia NVARCHAR(100),
    Region NVARCHAR(100),
    CodigoPostal NVARCHAR(20),
    Telefono NVARCHAR(20),
    Gerente NVARCHAR(200),
    FechaApertura DATE,
    Estado NVARCHAR(20),
    -- Metadatos
    FechaCarga DATETIME DEFAULT GETDATE(),
    Procesado BIT DEFAULT 0,
    LoteCarga NVARCHAR(50)
);
GO

-- ============================================================================
-- VISTAS DE NEGOCIO
-- ============================================================================

-- Vista: Ventas consolidadas por día
IF OBJECT_ID('dim.vw_VentasDiariasConsolidado', 'V') IS NOT NULL
    DROP VIEW dim.vw_VentasDiariasConsolidado;
GO

CREATE VIEW dim.vw_VentasDiariasConsolidado
AS
SELECT 
    t.Anio,
    t.Mes,
    t.Dia,
    t.Fecha,
    p.Categoria,
    p.Marca,
    ti.Region,
    ti.Ciudad,
    ti.NombreTienda,
    COUNT(DISTINCT f.TicketID) AS TicketsUnicos,
    SUM(f.Cantidad) AS CantidadVendida,
    SUM(f.TotalVenta) AS VentasNetas,
    SUM(f.Descuento) AS TotalDescuentos,
    SUM(f.Impuesto) AS TotalImpuestos,
    SUM(f.Margen) AS MargenTotal,
    AVG(f.TotalVenta) AS TicketPromedio
FROM fact.VentasDiarias f
INNER JOIN dim.Time t ON f.TiempoKey = t.TimeKey
INNER JOIN dim.Producto p ON f.ProductoKey = p.ProductoKey
INNER JOIN dim.Tienda ti ON f.TiendaKey = ti.TiendaKey
WHERE p.EsActual = 1 AND ti.EsActual = 1
GROUP BY t.Anio, t.Mes, t.Dia, t.Fecha, p.Categoria, p.Marca, ti.Region, ti.Ciudad, ti.NombreTienda;
GO

-- Vista: KPIs principales
IF OBJECT_ID('dim.vw_KPIsPrincipales', 'V') IS NOT NULL
    DROP VIEW dim.vw_KPIsPrincipales;
GO

CREATE VIEW dim.vw_KPIsPrincipales
AS
SELECT 
    t.Anio,
    t.Mes,
    t.Trimestre,
    ti.Region,
    p.Categoria,
    -- Métricas de ventas
    SUM(f.TotalVenta) AS VentasTotales,
    SUM(f.Cantidad) AS UnidadesVendidas,
    COUNT(DISTINCT f.TicketID) AS TotalTickets,
    COUNT(DISTINCT f.ClienteKey) AS ClientesUnicos,
    -- KPIs calculados
    SUM(f.TotalVenta) / COUNT(DISTINCT f.TicketID) AS TicketPromedio,
    SUM(f.TotalVenta) / SUM(f.Cantidad) AS PrecioPromedio,
    SUM(f.Margen) / SUM(f.TotalVenta) * 100 AS MargenPorcentaje,
    SUM(f.Margen) AS MargenTotal
FROM fact.VentasDiarias f
INNER JOIN dim.Time t ON f.TiempoKey = t.TimeKey
INNER JOIN dim.Producto p ON f.ProductoKey = p.ProductoKey
INNER JOIN dim.Tienda ti ON f.TiendaKey = ti.TiendaKey
WHERE p.EsActual = 1 AND ti.EsActual = 1
GROUP BY t.Anio, t.Mes, t.Trimestre, ti.Region, p.Categoria;
GO

PRINT '=== ESQUEMA DE DATA WAREHOUSE CREADO EXITOSAMENTE ===';
PRINT 'Dimensiones: Time, Producto, Cliente, Tienda, Empleado';
PRINT 'Hechos: VentasDiarias, InventarioDiario, PresupuestoMensual';
PRINT 'Staging: VentasPOS, ProductosERP, ClientesCRM, TiendasCorp';
GO
