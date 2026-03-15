-- ============================================================================
-- PROCEDIMIENTOS ETL PARA DATA WAREHOUSE
-- ============================================================================
-- Scripts para procesos de Extracción, Transformación y Carga (ETL)
-- del Data Warehouse de Ventas Minorista.
-- ============================================================================

USE DW_VentasMinorista;
GO

-- ============================================================================
-- ETL 1: CARGA DE VENTAS DESDE STAGING
-- ============================================================================

IF OBJECT_ID('etl.sp_CargarVentas', 'P') IS NOT NULL
    DROP PROCEDURE etl.sp_CargarVentas;
GO

CREATE PROCEDURE etl.sp_CargarVentas
    @LoteCarga NVARCHAR(50) = NULL,
    @RegistrosProcesados INT OUTPUT,
    @RegistrosError INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @FechaInicio DATE, @FechaFin DATE;
    DECLARE @ErrorCount INT = 0;
    DECLARE @SuccessCount INT = 0;
    
    IF @LoteCarga IS NULL
        SET @LoteCarga = 'ETL_' + CONVERT(VARCHAR(8), GETDATE(), 112) + '_' + 
                         REPLACE(CONVERT(VARCHAR(8), GETDATE(), 108), ':', '');
    
    PRINT 'Iniciando ETL de Ventas - Lote: ' + @LoteCarga;
    
    BEGIN TRY
        BEGIN TRANSACTION;
        
        -- Validar datos en staging
        IF NOT EXISTS (SELECT 1 FROM stg.VentasPOS WHERE Procesado = 0)
        BEGIN
            PRINT 'No hay datos pendientes en staging.';
            SET @RegistrosProcesados = 0;
            SET @RegistrosError = 0;
            RETURN;
        END
        
        -- Insertar ventas válidas en la tabla de hechos
        INSERT INTO fact.VentasDiarias (
            TiempoKey, ProductoKey, ClienteKey, TiendaKey, EmpleadoKey,
            TicketID, LineaTicket, Cantidad, PrecioUnitario,
            Descuento, Impuesto, TotalVenta, CostoTotal, Margen,
            LoteCarga, Origen
        )
        SELECT 
            CAST(CONVERT(VARCHAR(8), v.FechaVenta, 112) AS INT) AS TiempoKey,
            ISNULL(p.ProductoKey, 1) AS ProductoKey,
            c.ClienteKey,
            ISNULL(t.TiendaKey, 1) AS TiendaKey,
            e.EmpleadoKey,
            v.TicketID,
            v.LineaTicket,
            v.Cantidad,
            v.PrecioUnitario,
            v.Descuento,
            v.Impuesto,
            v.TotalVenta,
            v.PrecioUnitario * v.Cantidad * 0.7 AS CostoTotal,
            (v.PrecioUnitario - v.PrecioUnitario * 0.7) * v.Cantidad AS Margen,
            @LoteCarga AS LoteCarga,
            v.SistemaOrigen
        FROM stg.VentasPOS v
        LEFT JOIN dim.Producto p ON v.ProductoID = p.ProductoID AND p.EsActual = 1
        LEFT JOIN dim.Cliente c ON v.ClienteID = c.ClienteID AND c.EsActual = 1
        LEFT JOIN dim.Tienda t ON v.TiendaID = t.TiendaID AND t.EsActual = 1
        LEFT JOIN dim.Empleado e ON v.EmpleadoID = e.EmpleadoID AND e.EsActual = 1
        WHERE v.Procesado = 0
          AND v.FechaVenta IS NOT NULL
          AND v.Cantidad > 0
          AND v.PrecioUnitario > 0;
        
        SET @SuccessCount = @@ROWCOUNT;
        
        -- Marcar registros como procesados
        UPDATE stg.VentasPOS 
        SET Procesado = 1,
            FechaCarga = GETDATE()
        WHERE Procesado = 0;
        
        COMMIT TRANSACTION;
        
        SET @RegistrosProcesados = @SuccessCount;
        SET @RegistrosError = @ErrorCount;
        
        PRINT 'ETL de Ventas completado.';
        PRINT 'Registros procesados exitosamente: ' + CAST(@SuccessCount AS VARCHAR);
        PRINT 'Registros con error: ' + CAST(@ErrorCount AS VARCHAR);
        
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
        
        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
        DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
        DECLARE @ErrorState INT = ERROR_STATE();
        
        PRINT 'Error en ETL de Ventas: ' + @ErrorMessage;
        
        -- Registrar error
        INSERT INTO etl.LogProcesos (
            NombreProceso, Estado, MensajeError,
            FechaInicio, FechaFin, RegistrosProcesados
        )
        VALUES (
            'sp_CargarVentas',
            'Error',
            @ErrorMessage,
            GETDATE(),
            GETDATE(),
            0
        );
        
        SET @RegistrosProcesados = 0;
        SET @RegistrosError = 1;
        
        RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);
    END CATCH
END
GO

-- ============================================================================
-- ETL 2: CARGA DE PRODUCTOS (SCD TIPO 2)
-- ============================================================================

IF OBJECT_ID('etl.sp_CargarProductos', 'P') IS NOT NULL
    DROP PROCEDURE etl.sp_CargarProductos;
GO

CREATE PROCEDURE etl.sp_CargarProductos
    @LoteCarga NVARCHAR(50) = NULL,
    @RegistrosInsertados INT OUTPUT,
    @RegistrosActualizados INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @InsertCount INT = 0;
    DECLARE @UpdateCount INT = 0;
    
    IF @LoteCarga IS NULL
        SET @LoteCarga = 'ETL_' + CONVERT(VARCHAR(8), GETDATE(), 112);
    
    PRINT 'Iniciando ETL de Productos - Lote: ' + @LoteCarga;
    
    BEGIN TRY
        BEGIN TRANSACTION;
        
        -- Paso 1: Cerrar versiones anteriores de productos modificados (SCD Tipo 2)
        UPDATE p
        SET p.FechaFin = DATEADD(DAY, -1, CAST(s.FechaActualizacion AS DATE)),
            p.EsActual = 0,
            p.Version = p.Version + 1
        FROM dim.Producto p
        INNER JOIN stg.ProductosERP s ON p.SKU = s.SKU AND p.EsActual = 1
        WHERE (
            p.Nombre <> s.Nombre OR
            p.PrecioVenta <> s.PrecioVenta OR
            p.Categoria <> s.Categoria OR
            p.Estado <> s.Estado
        );
        
        SET @UpdateCount = @@ROWCOUNT;
        
        -- Paso 2: Insertar nuevas versiones de productos modificados
        INSERT INTO dim.Producto (
            ProductoID, SKU, Nombre, Descripcion, Categoria, SubCategoria,
            Marca, PrecioVenta, PrecioCosto, MargenGanancia,
            Proveedor, Estado, FechaInicio, FechaFin, EsActual, Version
        )
        SELECT 
            s.ProductoID,
            s.SKU,
            s.Nombre,
            s.Descripcion,
            s.Categoria,
            s.SubCategoria,
            s.Marca,
            s.PrecioVenta,
            s.PrecioCosto,
            (s.PrecioVenta - ISNULL(s.PrecioCosto, 0)) / NULLIF(s.PrecioVenta, 0) * 100,
            s.Proveedor,
            s.Estado,
            CAST(s.FechaActualizacion AS DATE) AS FechaInicio,
            NULL AS FechaFin,
            1 AS EsActual,
            1 AS Version
        FROM stg.ProductosERP s
        WHERE NOT EXISTS (
            SELECT 1 FROM dim.Producto p 
            WHERE p.SKU = s.SKU AND p.EsActual = 1
        )
        OR EXISTS (
            SELECT 1 FROM dim.Producto p 
            WHERE p.SKU = s.SKU AND p.EsActual = 0 
            AND p.FechaFin = DATEADD(DAY, -1, CAST(s.FechaActualizacion AS DATE))
        );
        
        SET @InsertCount = @@ROWCOUNT;
        
        -- Marcar staging como procesado
        UPDATE stg.ProductosERP 
        SET Procesado = 1 
        WHERE Procesado = 0;
        
        COMMIT TRANSACTION;
        
        SET @RegistrosInsertados = @InsertCount;
        SET @RegistrosActualizados = @UpdateCount;
        
        PRINT 'ETL de Productos completado.';
        PRINT 'Productos insertados: ' + CAST(@InsertCount AS VARCHAR);
        PRINT 'Productos actualizados (SCD): ' + CAST(@UpdateCount AS VARCHAR);
        
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
        
        DECLARE @ErrorMessage2 NVARCHAR(4000) = ERROR_MESSAGE();
        PRINT 'Error en ETL de Productos: ' + @ErrorMessage2;
        
        SET @RegistrosInsertados = 0;
        SET @RegistrosActualizados = 0;
        
        RAISERROR(@ErrorMessage2, 16, 1);
    END CATCH
END
GO

-- ============================================================================
-- ETL 3: CARGA DE CLIENTES (SCD TIPO 2)
-- ============================================================================

IF OBJECT_ID('etl.sp_CargarClientes', 'P') IS NOT NULL
    DROP PROCEDURE etl.sp_CargarClientes;
GO

CREATE PROCEDURE etl.sp_CargarClientes
    @LoteCarga NVARCHAR(50) = NULL,
    @RegistrosInsertados INT OUTPUT,
    @RegistrosActualizados INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @InsertCount INT = 0;
    DECLARE @UpdateCount INT = 0;
    
    IF @LoteCarga IS NULL
        SET @LoteCarga = 'ETL_' + CONVERT(VARCHAR(8), GETDATE(), 112);
    
    PRINT 'Iniciando ETL de Clientes - Lote: ' + @LoteCarga;
    
    BEGIN TRY
        BEGIN TRANSACTION;
        
        -- Cerrar versiones anteriores de clientes modificados
        UPDATE c
        SET c.FechaFin = DATEADD(DAY, -1, GETDATE()),
            c.EsActual = 0
        FROM dim.Cliente c
        INNER JOIN stg.ClientesCRM s ON c.ClienteID = s.ClienteID AND c.EsActual = 1
        WHERE (
            c.NombreCompleto <> s.NombreCompleto OR
            c.Email <> s.Email OR
            c.Segmento <> s.Segmento
        );
        
        SET @UpdateCount = @@ROWCOUNT;
        
        -- Insertar nuevas versiones
        INSERT INTO dim.Cliente (
            ClienteID, NumeroDocumento, TipoDocumento, NombreCompleto,
            Email, Telefono, FechaNacimiento, Genero,
            Segmento, FechaRegistro, FechaInicio, FechaFin, EsActual
        )
        SELECT 
            s.ClienteID,
            s.NumeroDocumento,
            s.TipoDocumento,
            s.NombreCompleto,
            s.Email,
            s.Telefono,
            s.FechaNacimiento,
            s.Genero,
            s.Segmento,
            s.FechaRegistro,
            GETDATE() AS FechaInicio,
            NULL AS FechaFin,
            1 AS EsActual
        FROM stg.ClientesCRM s
        WHERE NOT EXISTS (
            SELECT 1 FROM dim.Cliente c 
            WHERE c.ClienteID = s.ClienteID AND c.EsActual = 1
        )
        OR EXISTS (
            SELECT 1 FROM dim.Cliente c 
            WHERE c.ClienteID = s.ClienteID AND c.EsActual = 0
        );
        
        SET @InsertCount = @@ROWCOUNT;
        
        -- Marcar staging como procesado
        UPDATE stg.ClientesCRM SET Procesado = 1 WHERE Procesado = 0;
        
        COMMIT TRANSACTION;
        
        SET @RegistrosInsertados = @InsertCount;
        SET @RegistrosActualizados = @UpdateCount;
        
        PRINT 'ETL de Clientes completado.';
        PRINT 'Clientes insertados: ' + CAST(@InsertCount AS VARCHAR);
        PRINT 'Clientes actualizados: ' + CAST(@UpdateCount AS VARCHAR);
        
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
        
        PRINT 'Error en ETL de Clientes: ' + ERROR_MESSAGE();
        SET @RegistrosInsertados = 0;
        SET @RegistrosActualizados = 0;
    END CATCH
END
GO

-- ============================================================================
-- ETL 4: CARGA DE TIENDAS (SCD TIPO 2)
-- ============================================================================

IF OBJECT_ID('etl.sp_CargarTiendas', 'P') IS NOT NULL
    DROP PROCEDURE etl.sp_CargarTiendas;
GO

CREATE PROCEDURE etl.sp_CargarTiendas
    @LoteCarga NVARCHAR(50) = NULL,
    @RegistrosInsertados INT OUTPUT,
    @RegistrosActualizados INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    
    IF @LoteCarga IS NULL
        SET @LoteCarga = 'ETL_' + CONVERT(VARCHAR(8), GETDATE(), 112);
    
    PRINT 'Iniciando ETL de Tiendas - Lote: ' + @LoteCarga;
    
    BEGIN TRY
        BEGIN TRANSACTION;
        
        -- Cerrar versiones anteriores
        UPDATE t
        SET t.FechaFin = DATEADD(DAY, -1, GETDATE()),
            t.EsActual = 0
        FROM dim.Tienda t
        INNER JOIN stg.TiendasCorp s ON t.CodigoTienda = s.CodigoTienda AND t.EsActual = 1
        WHERE t.NombreTienda <> s.NombreTienda OR t.Estado <> s.Estado;
        
        DECLARE @UpdateCount INT = @@ROWCOUNT;
        
        -- Insertar nuevas versiones
        INSERT INTO dim.Tienda (
            TiendaID, CodigoTienda, NombreTienda, TipoTienda,
            Ciudad, Provincia, Region, Pais,
            Estado, FechaInicio, FechaFin, EsActual
        )
        SELECT 
            s.TiendaID,
            s.CodigoTienda,
            s.NombreTienda,
            s.TipoTienda,
            s.Ciudad,
            s.Provincia,
            s.Region,
            'Ecuador' AS Pais,
            s.Estado,
            GETDATE() AS FechaInicio,
            NULL AS FechaFin,
            1 AS EsActual
        FROM stg.TiendasCorp s
        WHERE NOT EXISTS (
            SELECT 1 FROM dim.Tienda t 
            WHERE t.CodigoTienda = s.CodigoTienda AND t.EsActual = 1
        );
        
        DECLARE @InsertCount INT = @@ROWCOUNT;
        
        UPDATE stg.TiendasCorp SET Procesado = 1 WHERE Procesado = 0;
        
        COMMIT TRANSACTION;
        
        SET @RegistrosInsertados = @InsertCount;
        SET @RegistrosActualizados = @UpdateCount;
        
        PRINT 'ETL de Tiendas completado.';
        
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
        
        PRINT 'Error en ETL de Tiendas: ' + ERROR_MESSAGE();
        SET @RegistrosInsertados = 0;
        SET @RegistrosActualizados = 0;
    END CATCH
END
GO

-- ============================================================================
-- TABLA DE LOG DE PROCESOS ETL
-- ============================================================================

IF OBJECT_ID('etl.LogProcesos', 'U') IS NOT NULL
    DROP TABLE etl.LogProcesos;
GO

CREATE TABLE etl.LogProcesos (
    LogID BIGINT IDENTITY(1,1) PRIMARY KEY,
    NombreProceso NVARCHAR(100) NOT NULL,
    Estado NVARCHAR(20) NOT NULL,       -- Inicio, Fin, Error
    MensajeError NVARCHAR(MAX),
    FechaInicio DATETIME NOT NULL,
    FechaFin DATETIME,
    RegistrosProcesados BIGINT,
    RegistrosInsertados BIGINT,
    RegistrosActualizados BIGINT,
    LoteCarga NVARCHAR(50),
    Usuario NVARCHAR(100) DEFAULT SYSTEM_USER,
    HostName NVARCHAR(100) DEFAULT HOST_NAME()
);
GO

CREATE INDEX IX_LogProcesos_Fecha ON etl.LogProcesos(FechaInicio DESC);
CREATE INDEX IX_LogProcesos_Proceso ON etl.LogProcesos(NombreProceso, FechaInicio);
GO

-- ============================================================================
-- PROCEDIMIENTO DE LOG
-- ============================================================================

IF OBJECT_ID('etl.sp_RegistrarLog', 'P') IS NOT NULL
    DROP PROCEDURE etl.sp_RegistrarLog;
GO

CREATE PROCEDURE etl.sp_RegistrarLog
    @NombreProceso NVARCHAR(100),
    @Estado NVARCHAR(20),
    @MensajeError NVARCHAR(MAX) = NULL,
    @RegistrosProcesados BIGINT = NULL,
    @RegistrosInsertados BIGINT = NULL,
    @RegistrosActualizados BIGINT = NULL,
    @LoteCarga NVARCHAR(50) = NULL,
    @LogID BIGINT OUTPUT
AS
BEGIN
    IF @Estado = 'Inicio'
    BEGIN
        INSERT INTO etl.LogProcesos (
            NombreProceso, Estado, FechaInicio, LoteCarga
        )
        VALUES (
            @NombreProceso, @Estado, GETDATE(), @LoteCarga
        );
        
        SET @LogID = SCOPE_IDENTITY();
    END
    ELSE
    BEGIN
        UPDATE etl.LogProcesos
        SET Estado = @Estado,
            MensajeError = @MensajeError,
            FechaFin = GETDATE(),
            RegistrosProcesados = @RegistrosProcesados,
            RegistrosInsertados = @RegistrosInsertados,
            RegistrosActualizados = @RegistrosActualizados
        WHERE LogID = @LogID;
    END
END
GO

-- ============================================================================
-- ETL PRINCIPAL: ORQUESTADOR
-- ============================================================================

IF OBJECT_ID('etl.sp_EjecutarETLCompleto', 'P') IS NOT NULL
    DROP PROCEDURE etl.sp_EjecutarETLCompleto;
GO

CREATE PROCEDURE etl.sp_EjecutarETLCompleto
    @LoteCarga NVARCHAR(50) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @LogID BIGINT;
    DECLARE @RegistrosProc INT, @RegistrosErr INT;
    DECLARE @RegistrosIns INT, @RegistrosAct INT;
    DECLARE @FechaInicio DATETIME = GETDATE();
    
    IF @LoteCarga IS NULL
        SET @LoteCarga = 'ETL_COMPLETO_' + CONVERT(VARCHAR(8), GETDATE(), 112);
    
    PRINT '========================================';
    PRINT 'INICIANDO ETL COMPLETO';
    PRINT 'Lote: ' + @LoteCarga;
    PRINT 'Fecha: ' + CONVERT(VARCHAR, @FechaInicio);
    PRINT '========================================';
    
    BEGIN TRY
        -- 1. Cargar Dimensiones
        PRINT '';
        PRINT '--- Cargando Dimensiones ---';
        
        EXEC etl.sp_CargarProductos @LoteCarga, @RegistrosIns OUTPUT, @RegistrosAct OUTPUT;
        PRINT 'Productos: ' + CAST(@RegistrosIns AS VARCHAR) + ' insertados, ' + 
              CAST(@RegistrosAct AS VARCHAR) + ' actualizados';
        
        EXEC etl.sp_CargarClientes @LoteCarga, @RegistrosIns OUTPUT, @RegistrosAct OUTPUT;
        PRINT 'Clientes: ' + CAST(@RegistrosIns AS VARCHAR) + ' insertados, ' + 
              CAST(@RegistrosAct AS VARCHAR) + ' actualizados';
        
        EXEC etl.sp_CargarTiendas @LoteCarga, @RegistrosIns OUTPUT, @RegistrosAct OUTPUT;
        PRINT 'Tiendas: ' + CAST(@RegistrosIns AS VARCHAR) + ' insertados, ' + 
              CAST(@RegistrosAct AS VARCHAR) + ' actualizados';
        
        -- 2. Cargar Hechos
        PRINT '';
        PRINT '--- Cargando Tablas de Hechos ---';
        
        EXEC etl.sp_CargarVentas @LoteCarga, @RegistrosProc OUTPUT, @RegistrosErr OUTPUT;
        PRINT 'Ventas: ' + CAST(@RegistrosProc AS VARCHAR) + ' procesadas, ' + 
              CAST(@RegistrosErr AS VARCHAR) + ' errores';
        
        PRINT '';
        PRINT '========================================';
        PRINT 'ETL COMPLETO FINALIZADO';
        PRINT 'Duración: ' + CAST(DATEDIFF(SECOND, @FechaInicio, GETDATE()) AS VARCHAR) + ' segundos';
        PRINT '========================================';
        
    END TRY
    BEGIN CATCH
        PRINT 'Error en ETL Completo: ' + ERROR_MESSAGE();
        
        INSERT INTO etl.LogProcesos (
            NombreProceso, Estado, MensajeError,
            FechaInicio, FechaFin
        )
        VALUES (
            'sp_EjecutarETLCompleto',
            'Error',
            ERROR_MESSAGE(),
            @FechaInicio,
            GETDATE()
        );
        
        THROW;
    END CATCH
END
GO

PRINT '=== PROCEDIMIENTOS ETL CREADOS EXITOSAMENTE ===';
GO
