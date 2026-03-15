-- ============================================================================
-- CONSULTAS ANALÍTICAS PARA DATA WAREHOUSE
-- ============================================================================
-- Scripts de consultas analíticas y reportes de negocio para el
-- Data Warehouse de Ventas Minorista.
-- ============================================================================

USE DW_VentasMinorista;
GO

-- ============================================================================
-- ANÁLISIS 1: VENTAS TOTALES POR PERÍODO
-- ============================================================================

-- Ventas mensuales
SELECT 
    t.Anio,
    t.Mes,
    t.NombreMes,
    COUNT(DISTINCT f.TicketID) AS TotalTickets,
    SUM(f.Cantidad) AS UnidadesVendidas,
    SUM(f.TotalVenta) AS VentasNetas,
    SUM(f.Descuento) AS TotalDescuentos,
    SUM(f.Margen) AS MargenTotal,
    AVG(f.TotalVenta) AS TicketPromedio
FROM fact.VentasDiarias f
INNER JOIN dim.Time t ON f.TiempoKey = t.TimeKey
GROUP BY t.Anio, t.Mes, t.NombreMes
ORDER BY t.Anio, t.Mes;
GO

-- Ventas por trimestre
SELECT 
    t.Anio,
    t.Trimestre,
    t.NombreTrimestre,
    COUNT(DISTINCT f.TicketID) AS TotalTickets,
    SUM(f.Cantidad) AS UnidadesVendidas,
    SUM(f.TotalVenta) AS VentasNetas,
    SUM(f.Margen) AS MargenTotal,
    CAST(SUM(f.Margen) AS DECIMAL(18,2)) / NULLIF(CAST(SUM(f.TotalVenta) AS DECIMAL(18,2)), 0) * 100 AS MargenPorcentaje
FROM fact.VentasDiarias f
INNER JOIN dim.Time t ON f.TiempoKey = t.TimeKey
GROUP BY t.Anio, t.Trimestre, t.NombreTrimestre
ORDER BY t.Anio, t.Trimestre;
GO

-- ============================================================================
-- ANÁLISIS 2: VENTAS POR CATEGORÍA DE PRODUCTO
-- ============================================================================

SELECT 
    p.Categoria,
    p.SubCategoria,
    COUNT(DISTINCT f.ProductoKey) AS ProductosUnicos,
    SUM(f.Cantidad) AS UnidadesVendidas,
    SUM(f.TotalVenta) AS VentasNetas,
    SUM(f.Margen) AS MargenTotal,
    CAST(SUM(f.Margen) AS DECIMAL(18,2)) / NULLIF(SUM(f.TotalVenta), 0) * 100 AS MargenPorcentaje,
    AVG(f.TotalVenta) AS TicketPromedio
FROM fact.VentasDiarias f
INNER JOIN dim.Producto p ON f.ProductoKey = p.ProductoKey
WHERE p.EsActual = 1
GROUP BY p.Categoria, p.SubCategoria
ORDER BY VentasNetas DESC;
GO

-- Top 20 productos más vendidos
SELECT TOP 20
    p.SKU,
    p.Nombre,
    p.Categoria,
    p.Marca,
    SUM(f.Cantidad) AS CantidadVendida,
    SUM(f.TotalVenta) AS VentasTotales,
    SUM(f.Margen) AS MargenTotal,
    COUNT(DISTINCT f.TiendaKey) AS TiendasQueVenden
FROM fact.VentasDiarias f
INNER JOIN dim.Producto p ON f.ProductoKey = p.ProductoKey
WHERE p.EsActual = 1
GROUP BY p.SKU, p.Nombre, p.Categoria, p.Marca
ORDER BY CantidadVendida DESC;
GO

-- ============================================================================
-- ANÁLISIS 3: VENTAS POR TIENDA Y REGIÓN
-- ============================================================================

-- Ventas por región
SELECT 
    t.Region,
    COUNT(DISTINCT f.TiendaKey) AS TiendasActivas,
    COUNT(DISTINCT f.TicketID) AS TotalTickets,
    SUM(f.Cantidad) AS UnidadesVendidas,
    SUM(f.TotalVenta) AS VentasNetas,
    SUM(f.Margen) AS MargenTotal,
    AVG(f.TotalVenta) AS TicketPromedio
FROM fact.VentasDiarias f
INNER JOIN dim.Tienda t ON f.TiendaKey = t.TiendaKey
WHERE t.EsActual = 1
GROUP BY t.Region
ORDER BY VentasNetas DESC;
GO

-- Ventas por tienda
SELECT TOP 20
    t.CodigoTienda,
    t.NombreTienda,
    t.Ciudad,
    t.Region,
    t.TipoTienda,
    COUNT(DISTINCT f.TicketID) AS TotalTickets,
    SUM(f.Cantidad) AS UnidadesVendidas,
    SUM(f.TotalVenta) AS VentasNetas,
    SUM(f.Margen) AS MargenTotal,
    AVG(f.TotalVenta) AS TicketPromedio
FROM fact.VentasDiarias f
INNER JOIN dim.Tienda t ON f.TiendaKey = t.TiendaKey
WHERE t.EsActual = 1
GROUP BY t.CodigoTienda, t.NombreTienda, t.Ciudad, t.Region, t.TipoTienda
ORDER BY VentasNetas DESC;
GO

-- ============================================================================
-- ANÁLISIS 4: ANÁLISIS DE CLIENTES
-- ============================================================================

-- Segmentación de clientes
SELECT 
    c.Segmento,
    COUNT(DISTINCT c.ClienteKey) AS TotalClientes,
    COUNT(DISTINCT f.TicketID) AS TotalTickets,
    SUM(f.TotalVenta) AS VentasTotales,
    AVG(f.TotalVenta) AS TicketPromedio,
    SUM(f.TotalVenta) / COUNT(DISTINCT c.ClienteKey) AS ValorPorCliente
FROM fact.VentasDiarias f
INNER JOIN dim.Cliente c ON f.ClienteKey = c.ClienteKey
WHERE c.EsActual = 1
GROUP BY c.Segmento
ORDER BY VentasTotales DESC;
GO

-- Top 20 clientes por valor
SELECT TOP 20
    c.ClienteID,
    c.NombreCompleto,
    c.Segmento,
    c.Ciudad,
    COUNT(DISTINCT f.TicketID) AS TotalCompras,
    SUM(f.Cantidad) AS TotalProductos,
    SUM(f.TotalVenta) AS ValorTotal,
    AVG(f.TotalVenta) AS TicketPromedio,
    MIN(CAST(t.Fecha AS DATE)) AS PrimeraCompra,
    MAX(CAST(t.Fecha AS DATE)) AS UltimaCompra
FROM fact.VentasDiarias f
INNER JOIN dim.Cliente c ON f.ClienteKey = c.ClienteKey
INNER JOIN dim.Time t ON f.TiempoKey = t.TimeKey
WHERE c.EsActual = 1
GROUP BY c.ClienteID, c.NombreCompleto, c.Segmento, c.Ciudad
ORDER BY ValorTotal DESC;
GO

-- ============================================================================
-- ANÁLISIS 5: MÉTRICAS DE RENDIMIENTO (KPIs)
-- ============================================================================

-- KPIs diarios
SELECT 
    t.Fecha,
    t.NombreDiaSemana,
    COUNT(DISTINCT f.TicketID) AS Tickets,
    COUNT(DISTINCT f.ClienteKey) AS ClientesUnicos,
    SUM(f.Cantidad) AS Unidades,
    SUM(f.TotalVenta) AS Ventas,
    SUM(f.TotalVenta) / COUNT(DISTINCT f.TicketID) AS TicketPromedio,
    SUM(f.Cantidad) / COUNT(DISTINCT f.TicketID) AS ProductosPorTicket,
    SUM(f.Margen) / SUM(f.TotalVenta) * 100 AS MargenPorcentaje
FROM fact.VentasDiarias f
INNER JOIN dim.Time t ON f.TiempoKey = t.TimeKey
WHERE t.Fecha >= DATEADD(MONTH, -1, GETDATE())
GROUP BY t.Fecha, t.NombreDiaSemana
ORDER BY t.Fecha;
GO

-- KPIs por día de semana
SELECT 
    t.DiaSemana,
    t.NombreDiaSemana,
    COUNT(DISTINCT f.TicketID) AS TotalTickets,
    SUM(f.TotalVenta) AS VentasTotales,
    AVG(f.TotalVenta) AS TicketPromedio,
    SUM(f.TotalVenta) / COUNT(DISTINCT f.TicketID) AS PromedioPorDia,
    CAST(COUNT(DISTINCT f.TicketID) AS DECIMAL(18,2)) * 100 / 
        (SELECT COUNT(DISTINCT TicketID) FROM fact.VentasDiarias) AS PorcentajeTickets
FROM fact.VentasDiarias f
INNER JOIN dim.Time t ON f.TiempoKey = t.TimeKey
GROUP BY t.DiaSemana, t.NombreDiaSemana
ORDER BY t.DiaSemana;
GO

-- ============================================================================
-- ANÁLISIS 6: TENDENCIAS Y COMPARATIVOS
-- ============================================================================

-- Comparativo mes actual vs mes anterior
WITH VentasMensuales AS (
    SELECT 
        t.Anio,
        t.Mes,
        SUM(f.TotalVenta) AS VentasTotales,
        COUNT(DISTINCT f.TicketID) AS TotalTickets,
        SUM(f.Margen) AS MargenTotal
    FROM fact.VentasDiarias f
    INNER JOIN dim.Time t ON f.TiempoKey = t.TimeKey
    GROUP BY t.Anio, t.Mes
)
SELECT 
    Anio,
    Mes,
    VentasTotales,
    TotalTickets,
    MargenTotal,
    LAG(VentasTotales) OVER (ORDER BY Anio, Mes) AS VentasMesAnterior,
    VentasTotales - LAG(VentasTotales) OVER (ORDER BY Anio, Mes) AS VariacionAbsoluta,
    CAST((VentasTotales - LAG(VentasTotales) OVER (ORDER BY Anio, Mes)) AS DECIMAL(18,2)) * 100 / 
        NULLIF(LAG(VentasTotales) OVER (ORDER BY Anio, Mes), 0) AS VariacionPorcentaje
FROM VentasMensuales
ORDER BY Anio, Mes DESC;
GO

-- Comparativo año actual vs año anterior
WITH VentasAnuales AS (
    SELECT 
        t.Anio,
        SUM(f.TotalVenta) AS VentasTotales,
        COUNT(DISTINCT f.TicketID) AS TotalTickets,
        SUM(f.Margen) AS MargenTotal,
        COUNT(DISTINCT f.ClienteKey) AS ClientesUnicos
    FROM fact.VentasDiarias f
    INNER JOIN dim.Time t ON f.TiempoKey = t.TimeKey
    GROUP BY t.Anio
)
SELECT 
    Anio,
    VentasTotales,
    TotalTickets,
    MargenTotal,
    ClientesUnicos,
    VentasTotales - LAG(VentasTotales) OVER (ORDER BY Anio) AS VariacionVentas,
    CAST((VentasTotales - LAG(VentasTotales) OVER (ORDER BY Anio)) AS DECIMAL(18,2)) * 100 / 
        NULLIF(LAG(VentasTotales) OVER (ORDER BY Anio), 0) AS VariacionPorcentaje
FROM VentasAnuales
ORDER BY Anio DESC;
GO

-- ============================================================================
-- ANÁLISIS 7: ANÁLISIS DE INVENTARIO
-- ============================================================================

-- Rotación de inventario por producto
SELECT TOP 20
    p.SKU,
    p.Nombre,
    p.Categoria,
    SUM(iv.Ventas) AS TotalVentas,
    AVG(iv.StockFinal) AS StockPromedio,
    CASE WHEN AVG(iv.StockFinal) > 0 
         THEN SUM(iv.Ventas) / AVG(iv.StockFinal) 
         ELSE 0 END AS RotacionInventario,
    SUM(iv.Mermas) AS TotalMermas
FROM fact.InventarioDiario iv
INNER JOIN dim.Producto p ON iv.ProductoKey = p.ProductoKey
WHERE p.EsActual = 1
GROUP BY p.SKU, p.Nombre, p.Categoria
ORDER BY RotacionInventario DESC;
GO

-- Stock actual por tienda
SELECT 
    t.CodigoTienda,
    t.NombreTienda,
    t.Ciudad,
    SUM(iv.StockFinal) AS StockTotal,
    SUM(iv.ValorInventario) AS ValorInventario,
    COUNT(DISTINCT iv.ProductoKey) AS ProductosConStock
FROM fact.InventarioDiario iv
INNER JOIN dim.Tienda t ON iv.TiendaKey = t.TiendaKey
INNER JOIN dim.Time ti ON iv.TiempoKey = ti.TimeKey
WHERE t.EsActual = 1
  AND ti.Fecha = (SELECT MAX(Fecha) FROM dim.Time WHERE TimeKey IN (SELECT DISTINCT TiempoKey FROM fact.InventarioDiario))
GROUP BY t.CodigoTienda, t.NombreTienda, t.Ciudad
ORDER BY StockTotal DESC;
GO

-- ============================================================================
-- ANÁLISIS 8: VENTAS VS PRESUPUESTO
-- ============================================================================

SELECT 
    t.Anio,
    t.Mes,
    t.NombreMes,
    ti.Region,
    SUM(f.TotalVenta) AS VentasReales,
    SUM(p.VentasPresupuestadas) AS VentasPresupuestadas,
    SUM(f.TotalVenta) - SUM(p.VentasPresupuestadas) AS Variacion,
    CAST((SUM(f.TotalVenta) - SUM(p.VentasPresupuestadas)) AS DECIMAL(18,2)) * 100 / 
        NULLIF(SUM(p.VentasPresupuestadas), 0) AS CumplimientoPorcentaje
FROM fact.VentasDiarias f
INNER JOIN dim.Time t ON f.TiempoKey = t.TimeKey
INNER JOIN dim.Tienda ti ON f.TiendaKey = ti.TiendaKey
LEFT JOIN fact.PresupuestoMensual p ON t.TimeKey = p.TiempoKey AND ti.TiendaKey = p.TiendaKey
WHERE ti.EsActual = 1
GROUP BY t.Anio, t.Mes, t.NombreMes, ti.Region
ORDER BY t.Anio, t.Mes, ti.Region;
GO

-- ============================================================================
-- ANÁLISIS 9: PRODUCTOS ESTRELLA, VACA, INTERROGANTE Y PERRO
-- ============================================================================
-- Matriz BCG basada en crecimiento y participación de mercado

WITH ProductoMetricas AS (
    SELECT 
        p.ProductoKey,
        p.SKU,
        p.Nombre,
        p.Categoria,
        SUM(f.TotalVenta) AS VentasTotales,
        SUM(f.Cantidad) AS CantidadVendida,
        -- Crecimiento (comparando últimos 3 meses vs 3 anteriores)
        SUM(CASE WHEN f.TiempoKey >= YEAR(GETDATE()) * 10000 + MONTH(DATEADD(MONTH, -3, GETDATE())) * 100 
                 THEN f.TotalVenta ELSE 0 END) AS VentasRecientes,
        SUM(CASE WHEN f.TiempoKey < YEAR(GETDATE()) * 10000 + MONTH(DATEADD(MONTH, -3, GETDATE())) * 100 
                 AND f.TiempoKey >= YEAR(GETDATE()) * 10000 + MONTH(DATEADD(MONTH, -6, GETDATE())) * 100 
                 THEN f.TotalVenta ELSE 0 END) AS VentasAnteriores
    FROM fact.VentasDiarias f
    INNER JOIN dim.Producto p ON f.ProductoKey = p.ProductoKey
    WHERE p.EsActual = 1
    GROUP BY p.ProductoKey, p.SKU, p.Nombre, p.Categoria
),
TotalVentas AS (
    SELECT SUM(VentasTotales) AS TotalGeneral FROM ProductoMetricas
)
SELECT 
    pm.SKU,
    pm.Nombre,
    pm.Categoria,
    pm.VentasTotales,
    CAST(pm.VentasTotales * 100.0 / tg.TotalGeneral AS DECIMAL(10,2)) AS ParticipacionMercado,
    CASE WHEN pm.VentasAnteriores > 0 
         THEN CAST((pm.VentasRecientes - pm.VentasAnteriores) * 100.0 / pm.VentasAnteriores AS DECIMAL(10,2))
         ELSE 0 END AS Crecimiento,
    CASE 
        WHEN pm.VentasTotales * 100.0 / tg.TotalGeneral >= 10 AND 
             (pm.VentasRecientes - pm.VentasAnteriores) * 100.0 / NULLIF(pm.VentasAnteriores, 0) >= 10 
            THEN 'ESTRELLA'
        WHEN pm.VentasTotales * 100.0 / tg.TotalGeneral >= 10 AND 
             (pm.VentasRecientes - pm.VentasAnteriores) * 100.0 / NULLIF(pm.VentasAnteriores, 0) < 10 
            THEN 'VACA'
        WHEN pm.VentasTotales * 100.0 / tg.TotalGeneral < 10 AND 
             (pm.VentasRecientes - pm.VentasAnteriores) * 100.0 / NULLIF(pm.VentasAnteriores, 0) >= 10 
            THEN 'INTERROGANTE'
        ELSE 'PERRO'
    END AS CategoriaBCG
FROM ProductoMetricas pm
CROSS JOIN TotalVentas tg
ORDER BY pm.VentasTotales DESC;
GO

-- ============================================================================
-- ANÁLISIS 10: RESUMEN EJECUTIVO
-- ============================================================================

SELECT 
    'Ventas Totales' AS Metrica,
    FORMAT(SUM(TotalVenta), 'N2') AS Valor
FROM fact.VentasDiarias
UNION ALL
SELECT 
    'Margen Total',
    FORMAT(SUM(Margen), 'N2')
FROM fact.VentasDiarias
UNION ALL
SELECT 
    'Tickets Únicos',
    FORMAT(COUNT(DISTINCT TicketID), 'N0')
FROM fact.VentasDiarias
UNION ALL
SELECT 
    'Productos Vendidos',
    FORMAT(SUM(Cantidad), 'N0')
FROM fact.VentasDiarias
UNION ALL
SELECT 
    'Clientes Únicos',
    FORMAT(COUNT(DISTINCT ClienteKey), 'N0')
FROM fact.VentasDiarias
UNION ALL
SELECT 
    'Ticket Promedio',
    FORMAT(AVG(TotalVenta), 'N2')
FROM fact.VentasDiarias
UNION ALL
SELECT 
    'Margen Porcentaje',
    FORMAT(SUM(Margen) * 100.0 / SUM(TotalVenta), 'N2') + '%'
FROM fact.VentasDiarias;
GO

PRINT '=== CONSULTAS ANALÍTICAS COMPLETADAS ===';
PRINT 'Ejecute las consultas según las necesidades de reporte.';
GO
