-- ============================================================================
-- POBLAR DIMENSIÓN DE TIEMPO
-- ============================================================================
-- Script para generar automáticamente la dimensión de tiempo con datos
-- desde 2020 hasta 2030, incluyendo cálculos de fechas especiales.
-- ============================================================================

USE DW_VentasMinorista;
GO

-- ============================================================================
-- PROCEDIMIENTO PARA POBLAR DIMENSIÓN DE TIEMPO
-- ============================================================================

IF OBJECT_ID('etl.sp_PoblarDimTime', 'P') IS NOT NULL
    DROP PROCEDURE etl.sp_PoblarDimTime;
GO

CREATE PROCEDURE etl.sp_PoblarDimTime
    @AnioInicio INT = 2020,
    @AnioFin INT = 2030
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @FechaActual DATE;
    DECLARE @FechaFin DATE;
    
    SET @FechaActual = CAST(CAST(@AnioInicio AS VARCHAR) + '-01-01' AS DATE);
    SET @FechaFin = CAST(CAST(@AnioFin AS VARCHAR) + '-12-31' AS DATE);
    
    -- Limpiar datos existentes
    TRUNCATE TABLE dim.Time;
    
    -- Insertar días
    WHILE @FechaActual <= @FechaFin
    BEGIN
        INSERT INTO dim.Time (
            TimeKey, Fecha, Dia, Mes, Anio, Trimestre, Semestre,
            DiaSemana, NombreDiaSemana, NombreMes, NombreTrimestre,
            EsFinDeSemana, EsUltimoDiaMes, EsUltimoDiaAnio, SemanaISO
        )
        VALUES (
            CAST(CONVERT(VARCHAR(8), @FechaActual, 112) AS INT),  -- YYYYMMDD
            @FechaActual,
            DAY(@FechaActual),
            MONTH(@FechaActual),
            YEAR(@FechaActual),
            DATEPART(QUARTER, @FechaActual),
            CASE WHEN MONTH(@FechaActual) <= 6 THEN 1 ELSE 2 END,
            DATEPART(WEEKDAY, @FechaActual),
            DATENAME(WEEKDAY, @FechaActual),
            DATENAME(MONTH, @FechaActual),
            'T' + CAST(DATEPART(QUARTER, @FechaActual) AS VARCHAR),
            CASE WHEN DATEPART(WEEKDAY, @FechaActual) IN (1, 7) THEN 1 ELSE 0 END,
            CASE WHEN DAY(EOMONTH(@FechaActual)) = DAY(@FechaActual) THEN 1 ELSE 0 END,
            CASE WHEN MONTH(@FechaActual) = 12 AND DAY(@FechaActual) = 31 THEN 1 ELSE 0 END,
            DATEPART(ISO_WEEK, @FechaActual)
        );
        
        SET @FechaActual = DATEADD(DAY, 1, @FechaActual);
    END
    
    -- Actualizar feriados fijos
    UPDATE dim.Time SET EsFeriado = 1, NombreFeriado = 'Año Nuevo' WHERE Mes = 1 AND Dia = 1;
    UPDATE dim.Time SET EsFeriado = 1, NombreFeriado = 'Día del Trabajo' WHERE Mes = 5 AND Dia = 1;
    UPDATE dim.Time SET EsFeriado = 1, NombreFeriado = 'Independencia de Quito' WHERE Mes = 12 AND Dia = 6;
    UPDATE dim.Time SET EsFeriado = 1, NombreFeriado = 'Navidad' WHERE Mes = 12 AND Dia = 25;
    
    PRINT 'Dimensión de tiempo poblada desde ' + CAST(@AnioInicio AS VARCHAR) + ' hasta ' + CAST(@AnioFin AS VARCHAR);
    PRINT 'Total de registros: ' + CAST((SELECT COUNT(*) FROM dim.Time) AS VARCHAR);
END
GO

-- Ejecutar procedimiento
EXEC etl.sp_PoblarDimTime @AnioInicio = 2020, @AnioFin = 2030;
GO

-- ============================================================================
-- GENERAR DATOS DE EJEMPLO PARA DIMENSIONES
-- ============================================================================

-- Poblar DimProducto con datos de ejemplo
IF OBJECT_ID('tempdb..#ProductosEjemplo') IS NOT NULL
    DROP TABLE #ProductosEjemplo;

CREATE TABLE #ProductosEjemplo (
    SKU NVARCHAR(50),
    Nombre NVARCHAR(200),
    Categoria NVARCHAR(100),
    SubCategoria NVARCHAR(100),
    Marca NVARCHAR(100),
    PrecioVenta DECIMAL(10,2)
);

INSERT INTO #ProductosEjemplo VALUES
('ELEC-001', 'Smart TV 55" 4K', 'Electrónica', 'Televisores', 'Samsung', 899.99),
('ELEC-002', 'Smart TV 65" 4K', 'Electrónica', 'Televisores', 'LG', 1299.99),
('ELEC-003', 'Laptop 15" i7', 'Electrónica', 'Computadoras', 'Dell', 1199.99),
('ELEC-004', 'Laptop 13" i5', 'Electrónica', 'Computadoras', 'HP', 899.99),
('ELEC-005', 'Smartphone 128GB', 'Electrónica', 'Celulares', 'Apple', 999.99),
('ELEC-006', 'Smartphone 256GB', 'Electrónica', 'Celulares', 'Samsung', 1199.99),
('ROPA-001', 'Camiseta Básica', 'Ropa', 'Hombre', 'Nike', 29.99),
('ROPA-002', 'Pantalón Jeans', 'Ropa', 'Hombre', 'Levis', 59.99),
('ROPA-003', 'Vestido Casual', 'Ropa', 'Mujer', 'Zara', 49.99),
('ROPA-004', 'Chaqueta Impermeable', 'Ropa', 'Unisex', 'North Face', 149.99),
('HOGA-001', 'Juego de Sábanas', 'Hogar', 'Dormitorio', 'HomeStyle', 79.99),
('HOGA-002', 'Set de Ollas', 'Hogar', 'Cocina', 'Tefal', 199.99),
('DEPO-001', 'Balón de Fútbol', 'Deportes', 'Fútbol', 'Adidas', 34.99),
('DEPO-002', 'Raqueta de Tenis', 'Deportes', 'Tenis', 'Wilson', 89.99),
('JUGU-001', 'Lego Star Wars', 'Juguetes', 'Construcción', 'Lego', 79.99),
('JUGU-002', 'Muñeca Fashion', 'Juguetes', 'Muñecas', 'Mattel', 24.99);

INSERT INTO dim.Producto (
    ProductoID, SKU, Nombre, Descripcion, Categoria, SubCategoria,
    Marca, PrecioVenta, PrecioCosto, MargenGanancia,
    Estado, FechaInicio, EsActual
)
SELECT 
    ROW_NUMBER() OVER (ORDER BY SKU) AS ProductoID,
    SKU,
    Nombre,
    'Producto de ejemplo de la categoría ' + Categoria,
    Categoria,
    SubCategoria,
    Marca,
    PrecioVenta,
    PrecioVenta * 0.7 AS PrecioCosto,
    30.00 AS MargenGanancia,
    'Activo' AS Estado,
    '2024-01-01' AS FechaInicio,
    1 AS EsActual
FROM #ProductosEjemplo;

PRINT 'DimProducto poblada con ' + CAST((SELECT COUNT(*) FROM dim.Producto) AS VARCHAR) + ' registros.';
GO

-- Poblar DimTienda con datos de ejemplo
IF OBJECT_ID('tempdb..#TiendasEjemplo') IS NOT NULL
    DROP TABLE #TiendasEjemplo;

CREATE TABLE #TiendasEjemplo (
    CodigoTienda NVARCHAR(20),
    NombreTienda NVARCHAR(200),
    TipoTienda NVARCHAR(50),
    Ciudad NVARCHAR(100),
    Provincia NVARCHAR(100),
    Region NVARCHAR(100)
);

INSERT INTO #TiendasEjemplo VALUES
('T001', 'Tienda Quito Centro', 'Flagship', 'Quito', 'Pichincha', 'Sierra'),
('T002', 'Tienda Cumbayá', 'Mediano', 'Cumbayá', 'Pichincha', 'Sierra'),
('T003', 'Tienda Guayaquil Mall', 'Flagship', 'Guayaquil', 'Guayas', 'Costa'),
('T004', 'Tienda Samborondón', 'Mediano', 'Samborondón', 'Guayas', 'Costa'),
('T005', 'Tienda Cuenca', 'Pequeño', 'Cuenca', 'Azuay', 'Sierra'),
('T006', 'Tienda Manta', 'Mediano', 'Manta', 'Manabí', 'Costa'),
('T007', 'Tienda Ambato', 'Pequeño', 'Ambato', 'Tungurahua', 'Sierra'),
('T008', 'Tienda Loja', 'Pequeño', 'Loja', 'Loja', 'Sierra');

INSERT INTO dim.Tienda (
    TiendaID, CodigoTienda, NombreTienda, TipoTienda,
    Ciudad, Provincia, Region, Pais,
    Estado, FechaInicio, EsActual
)
SELECT 
    ROW_NUMBER() OVER (ORDER BY CodigoTienda) AS TiendaID,
    CodigoTienda,
    NombreTienda,
    TipoTienda,
    Ciudad,
    Provincia,
    Region,
    'Ecuador' AS Pais,
    'Activa' AS Estado,
    '2024-01-01' AS FechaInicio,
    1 AS EsActual
FROM #TiendasEjemplo;

PRINT 'DimTienda poblada con ' + CAST((SELECT COUNT(*) FROM dim.Tienda) AS VARCHAR) + ' registros.';
GO

-- Poblar DimCliente con datos de ejemplo
INSERT INTO dim.Cliente (
    ClienteID, NumeroDocumento, TipoDocumento, NombreCompleto,
    Email, Telefono, FechaNacimiento, Genero,
    Segmento, FechaRegistro, FechaInicio, EsActual
)
SELECT TOP 1000
    ROW_NUMBER() OVER (ORDER BY NEWID()) AS ClienteID,
    CAST(ABS(CHECKSUM(NEWID()) % 1000000000) AS NVARCHAR(10)) AS NumeroDocumento,
    CASE WHEN ABS(CHECKSUM(NEWID()) % 3) = 0 THEN 'RUC' ELSE 'Cédula' END AS TipoDocumento,
    'Cliente ' + CAST(ROW_NUMBER() OVER (ORDER BY NEWID()) AS NVARCHAR) AS NombreCompleto,
    'cliente' + CAST(ROW_NUMBER() OVER (ORDER BY NEWID()) AS NVARCHAR) + '@email.com' AS Email,
    '(' + CAST(ABS(CHECKSUM(NEWID()) % 90 + 2) AS NVARCHAR) + ') ' + 
        CAST(ABS(CHECKSUM(NEWID()) % 9000 + 1000) AS NVARCHAR) + '-' + 
        CAST(ABS(CHECKSUM(NEWID()) % 9000 + 1000) AS NVARCHAR) AS Telefono,
    DATEADD(YEAR, -ABS(CHECKSUM(NEWID()) % 50 + 18), GETDATE()) AS FechaNacimiento,
    CASE WHEN ABS(CHECKSUM(NEWID()) % 2) = 0 THEN 'M' ELSE 'F' END AS Genero,
    CASE 
        WHEN ABS(CHECKSUM(NEWID()) % 100) < 5 THEN 'VIP'
        WHEN ABS(CHECKSUM(NEWID()) % 100) < 20 THEN 'Premium'
        WHEN ABS(CHECKSUM(NEWID()) % 100) < 60 THEN 'Regular'
        ELSE 'Nuevo'
    END AS Segmento,
    DATEADD(DAY, -ABS(CHECKSUM(NEWID()) % 365), GETDATE()) AS FechaRegistro,
    '2024-01-01' AS FechaInicio,
    1 AS EsActual
FROM sys.objects o1
CROSS JOIN sys.objects o2;

PRINT 'DimCliente poblada con ' + CAST((SELECT COUNT(*) FROM dim.Cliente) AS VARCHAR) + ' registros.';
GO

-- Poblar DimEmpleado con datos de ejemplo
INSERT INTO dim.Empleado (
    EmpleadoID, NumeroEmpleado, NombreCompleto, Cargo,
    Departamento, TiendaID, FechaContratacion,
    Estado, FechaInicio, EsActual
)
SELECT TOP 200
    ROW_NUMBER() OVER (ORDER BY NEWID()) AS EmpleadoID,
    'EMP' + RIGHT('0000' + CAST(ROW_NUMBER() OVER (ORDER BY NEWID()) AS NVARCHAR), 4) AS NumeroEmpleado,
    'Empleado ' + CAST(ROW_NUMBER() OVER (ORDER BY NEWID()) AS NVARCHAR) AS NombreCompleto,
    CASE WHEN ABS(CHECKSUM(NEWID()) % 5) = 0 THEN 'Gerente'
         WHEN ABS(CHECKSUM(NEWID()) % 5) = 1 THEN 'Supervisor'
         WHEN ABS(CHECKSUM(NEWID()) % 5) = 2 THEN 'Vendedor'
         WHEN ABS(CHECKSUM(NEWID()) % 5) = 3 THEN 'Cajero'
         ELSE 'Auxiliar' END AS Cargo,
    CASE WHEN ABS(CHECKSUM(NEWID()) % 4) = 0 THEN 'Ventas'
         WHEN ABS(CHECKSUM(NEWID()) % 4) = 1 THEN 'Caja'
         WHEN ABS(CHECKSUM(NEWID()) % 4) = 2 THEN 'Almacén'
         ELSE 'Atención al Cliente' END AS Departamento,
    ABS(CHECKSUM(NEWID()) % (SELECT COUNT(*) FROM dim.Tienda)) + 1 AS TiendaID,
    DATEADD(DAY, -ABS(CHECKSUM(NEWID()) % 730), GETDATE()) AS FechaContratacion,
    'Activo' AS Estado,
    '2024-01-01' AS FechaInicio,
    1 AS EsActual
FROM sys.objects o1
CROSS JOIN sys.objects o2;

PRINT 'DimEmpleado poblada con ' + CAST((SELECT COUNT(*) FROM dim.Empleado) AS VARCHAR) + ' registros.';
GO

-- ============================================================================
-- GENERAR DATOS DE VENTAS DE EJEMPLO
-- ============================================================================

DECLARE @FechaInicio DATE = '2024-01-01';
DECLARE @FechaFin DATE = '2024-12-31';
DECLARE @FechaActual2 DATE = @FechaInicio;
DECLARE @VentasGeneradas INT = 0;

WHILE @FechaActual2 <= @FechaFin
BEGIN
    -- Generar entre 50 y 200 ventas por día
    DECLARE @CantidadVentas INT = ABS(CHECKSUM(NEWID()) % 150) + 50;
    
    INSERT INTO fact.VentasDiarias (
        TiempoKey, ProductoKey, ClienteKey, TiendaKey, EmpleadoKey,
        TicketID, LineaTicket, Cantidad, PrecioUnitario,
        Descuento, Impuesto, TotalVenta, CostoTotal, Margen, LoteCarga
    )
    SELECT TOP (@CantidadVentas)
        CAST(CONVERT(VARCHAR(8), @FechaActual2, 112) AS INT) AS TiempoKey,
        ABS(CHECKSUM(NEWID()) % (SELECT COUNT(*) FROM dim.Producto)) + 1 AS ProductoKey,
        ABS(CHECKSUM(NEWID()) % (SELECT COUNT(*) FROM dim.Cliente)) + 1 AS ClienteKey,
        ABS(CHECKSUM(NEWID()) % (SELECT COUNT(*) FROM dim.Tienda)) + 1 AS TiendaKey,
        ABS(CHECKSUM(NEWID()) % (SELECT COUNT(*) FROM dim.Empleado)) + 1 AS EmpleadoKey,
        'TCK-' + CONVERT(VARCHAR(8), @FechaActual2, 112) + '-' + 
            RIGHT('00000' + CAST(ABS(CHECKSUM(NEWID()) % 10000) AS VARCHAR), 5) AS TicketID,
        1 AS LineaTicket,
        ABS(CHECKSUM(NEWID()) % 5) + 1 AS Cantidad,
        p.PrecioVenta AS PrecioUnitario,
        CASE WHEN ABS(CHECKSUM(NEWID()) % 10) < 2 THEN p.PrecioVenta * 0.1 ELSE 0 END AS Descuento,
        p.PrecioVenta * 0.12 AS Impuesto,
        p.PrecioVenta * (1 + 0.12) - 
            CASE WHEN ABS(CHECKSUM(NEWID()) % 10) < 2 THEN p.PrecioVenta * 0.1 ELSE 0 END AS TotalVenta,
        p.PrecioCosto * (ABS(CHECKSUM(NEWID()) % 5) + 1) AS CostoTotal,
        (p.PrecioVenta - p.PrecioCosto) * (ABS(CHECKSUM(NEWID()) % 5) + 1) AS Margen,
        'CargaInicial_' + CONVERT(VARCHAR(8), GETDATE(), 112) AS LoteCarga
    FROM dim.Producto p
    ORDER BY NEWID();
    
    SET @VentasGeneradas = @VentasGeneradas + @CantidadVentas;
    SET @FechaActual2 = DATEADD(DAY, 1, @FechaActual2);
    
    -- Progreso cada 30 días
    IF DATEDIFF(DAY, @FechaInicio, @FechaActual2) % 30 = 0
        PRINT 'Generando ventas... Fecha: ' + CONVERT(VARCHAR, @FechaActual2) + 
              ' | Total: ' + CAST(@VentasGeneradas AS VARCHAR);
END

PRINT '=== DATOS DE EJEMPLO GENERADOS ===';
PRINT 'Total de ventas generadas: ' + CAST((SELECT COUNT(*) FROM fact.VentasDiarias) AS VARCHAR);
PRINT 'Rango de fechas: ' + CONVERT(VARCHAR, @FechaInicio) + ' a ' + CONVERT(VARCHAR, @FechaFin);
GO

-- ============================================================================
-- RESUMEN DE DATOS
-- ============================================================================

SELECT 'DimTime' AS Tabla, COUNT(*) AS Registros FROM dim.Time
UNION ALL
SELECT 'DimProducto', COUNT(*) FROM dim.Producto
UNION ALL
SELECT 'DimCliente', COUNT(*) FROM dim.Cliente
UNION ALL
SELECT 'DimTienda', COUNT(*) FROM dim.Tienda
UNION ALL
SELECT 'DimEmpleado', COUNT(*) FROM dim.Empleado
UNION ALL
SELECT 'FactVentasDiarias', COUNT(*) FROM fact.VentasDiarias;
GO
