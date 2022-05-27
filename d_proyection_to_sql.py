
from sys import prefix
from tkinter import E
import pandas as pd
import datetime as dt


from pylib.py_lib import bulkInsert, deleteDataToSql, excecute_query, excutionTime, insertDataToSql_Alchemy, removeColumnsIn, workDirectory, parameters, stringConnect


ROOT = workDirectory()

DST = 'Proyeccion'


def read_conexion(name='general', prefix='id'):
    dbCon, files, is_test, bulk_space = parameters(
        ROOT=ROOT,
        config_file='configdb.json',
        service_name=name
    )

    return (is_test, dbCon, bulk_space)


@excutionTime
def run():

    is_test_FDIM, ServFDIM_DB_Planeacion, bulk_space_FDIM = read_conexion(
        'ServFDIM_DB_Planeacion')
    str_con_FDIM_Planeacion = stringConnect(ServFDIM_DB_Planeacion)

    # print('ServFDIM', ServFDIM_DB_Planeacion, bulk_space_FDIM)

    is_test_FDIM, ServDB10_DB_FDIM, bulk_space_DB10 = read_conexion(
        'ServDB10_DB_FDIM')
    str_con_DB10_FDIM = stringConnect(ServDB10_DB_FDIM)

    is_test_FDIM, ServJP13M_DB_ADW, bulk_space_JP13M = read_conexion(
        'ServJP13M_DB_AnalysisDW')
    str_con_JP13M_ADW = stringConnect(ServJP13M_DB_ADW)

    # print('ServFDIM', ServFDIM_DB_Planeacion, bulk_space_FDIM)

    ahora = dt.datetime.now()
    dif = dt.timedelta(weeks=8)
    hasta = ahora + dif

    t_desde = ahora.strftime('%Y-%m-%d')
    # '2020-01-01'

    t_hasta = hasta.strftime('%Y-%m-%d')

    df_fecha = excecute_query(
        strCon=str_con_JP13M_ADW,
        schema='Common',
        table='Fecha_Tabla_ProcesoLocal',
        fields=['IniSem', 'FinSem'],
        where=[
            "FechaTipo = 'ISO'",
            "Fecha in( '{}', '{}')".format(t_desde, t_hasta)
        ]
    )

    intFecha = int(ahora.strftime('%Y%m%d'))

    inicio = df_fecha['IniSem'].min()
    fin = df_fecha['FinSem'].max()

    print(inicio, fin)
    exp = 0
    # Consulta Estimados DB10_FDIM
    queryEst = '''

		

	DECLARE @FECINI DATE = '{}'
	DECLARE @FECFIN DATE = '{}'
	DECLARE @Exportable AS INT  = {}


	------------------------------------------------------------------------------
	--	Tabla temporal para capturar la proyección diaria												--
	------------------------------------------------------------------------------
	DROP TABLE IF EXISTS #TMPPROYDIARIA, #TMPPROYDIARIASINGRADOS 
	CREATE TABLE #TMPPROYDIARIA 
	(
		FECHAPRODUCCION 		SMALLDATETIME,
		IDYEAR					SMALLINT,
		IDWEEK					SMALLINT,
		IDFINCA 				INT, 
		IDPRODUCTO 				INT, 
		IDVARIEDAD 				INT,
		PORCENTAJE_NACIONAL		NUMERIC(6,2),  
		PROYECCION	 			INT,
		IdGrado					INT,
		Grado					varchar(40),
		ROW						INT
	)

	 CREATE TABLE #TMPPROYDIARIASINGRADOS 
	(
		FECHAPRODUCCION 		SMALLDATETIME,
		IDYEAR					SMALLINT,
		IDWEEK					SMALLINT,
		IDFINCA 				INT, 
		IDPRODUCTO 				INT, 
		IDVARIEDAD 				INT,
		PORCENTAJE_NACIONAL		NUMERIC(6,2),  
		PROYECCION	 			INT
	)


	----------------------------------------------------------------------------------
   	--	Captura la Proyección de la Producción Diaria exportable CON GRADOS				--
  ----------------------------------------------------------------------------------
	INSERT INTO #TMPPROYDIARIA 
	(
		FECHAPRODUCCION,
		IDYEAR,
		IDWEEK,
		IDFINCA, 
		IDPRODUCTO, 
		IDVARIEDAD,
		PROYECCION,
		IdGrado,
		Grado,
		ROW
	)
	SELECT	PRD.FECHAINICIO AS FECHA, 
			UP.IDYEAR,
			UP.IDWEEK,
			UP.IDFINCA, 
			UP.IDPRODUCTO,
			UP.IDVARIEDAD,
--			CASE WHEN ISNULL(APN.IDPRODUCTO, 0) <> 0 THEN ISNULL(PN.PORNACIONAL, 0) ELSE 0 END,
			UP.[TALLOS],
			UP.IdGrado,
			UP.Grado,
			ROW_NUMBER() OVER(PARTITION BY UP.IDVARIEDAD,PRD.FECHAINICIO, UP.IDFINCA,UP.IDPRODUCTO ORDER BY PRD.FECHAINICIO DESC) AS 'Row'
	FROM 	(
				SELECT	PD.IDFINCA,
						PD.IDPRODUCTO,
						PD.IDVARIEDAD,
						PD.IDYEAR,
						PD.IDWEEK,
						PD.IdGrado AS IdGrado,
						GR.DescripcionIngles AS Grado,
						SUM(PD.TALLOS_LUNES) AS [1],
						SUM(PD.TALLOS_MARTES) AS [2],
						SUM(PD.TALLOS_MIERCOLES) AS [3],
						SUM(PD.TALLOS_JUEVES) AS [4],
						SUM(PD.TALLOS_VIERNES) AS [5],
						SUM(PD.TALLOS_SABADO) AS [6],
						SUM(PD.TALLOS_DOMINGO) AS [7]
				FROM 	FDIM.PSI.[ProyeccionProduccionDiariaCopyConGrados]  PD with(nolock) 
				inner join PSI.GradosdeCalidad GR  with(nolock) ON GR.IdGrado = PD.IdGrado
				JOIN	PRESUPUESTO.GENERAL.PERIODOSPRESUPUESTOS PR  with(nolock) ON PD.IDYEAR = PR.IDYEAR AND PD.IDWEEK = PR.ORDEN AND PR.IDPERIODICIDAD = 1
				WHERE	PR.FECHAINICIO BETWEEN @FECINI AND @FECFIN OR PR.FECHAFINAL BETWEEN @FECINI AND @FECFIN
				GROUP BY PD.IDFINCA,PD.IDYEAR,PD.IDWEEK,PD.IDPRODUCTO,PD.IDVARIEDAD,PD.IdGrado,GR.DescripcionIngles
				
			) PRODIA
	UNPIVOT
			(
				TALLOS FOR DIA IN ([1], [2], [3], [4], [5], [6], [7])
			) AS UP
	join PRESUPUESTO.GENERAL.PERIODOSPRESUPUESTOS PRD  with(nolock) on PRD.Orden = UP.IdWeek and 
	UP.DIA = DATEPART(DW, PRD.FECHAINICIO) and
	PRD.idperiodicidad = 7
	where PRD.idYear in (YEAR(@FECINI),YEAR(@FECFIN)) and PRD.Fechainicio Between @FECINI AND @FECFIN

	-----------------------------------------------------------------------------------
	--Select * from PRESUPUESTO.GENERAL.PERIODOSPRESUPUESTOS		where idPeriodicidad = 7


	UPDATE 	UP
	SET 	UP.PORCENTAJE_NACIONAL = CASE WHEN ISNULL(APN.IDPRODUCTO, 0) <> 0 THEN ISNULL(PN.PORNACIONAL, 0) ELSE 0 END
	FROM	#TMPPROYDIARIA UP
	LEFT JOIN	PSI.PORNALSEMANAL PN  with(nolock) ON UP.IDFINCA = PN.IDFINCA AND UP.IDPRODUCTO = PN.IDPRODUCTO AND UP.IDVARIEDAD = PN.IDVARIEDAD AND UP.IDYEAR = PN.IDYEAR AND UP.IDWEEK = PN.IDWEEK
	LEFT JOIN	PSI.APPORNAL APN  with(nolock) ON PN.IDPRODUCTO = APN.IDPRODUCTO
	
	------------------------------------------------------------------------------------------------
  --		Captura la Proyección de la Producción Diaria exportable	SIN GRADOS	PARA DIFERENCIA		--
  ------------------------------------------------------------------------------------------------
	INSERT INTO #TMPPROYDIARIASINGRADOS 
	(
		FECHAPRODUCCION,
		IDYEAR,
		IDWEEK,
		IDFINCA, 
		IDPRODUCTO, 
		IDVARIEDAD,
		PROYECCION
	)
	SELECT	PRD.FECHAINICIO AS FECHA, 
			UP.IDYEAR,
			UP.IDWEEK,
			UP.IDFINCA, 
			UP.IDPRODUCTO,
			UP.IDVARIEDAD,
--			CASE WHEN ISNULL(APN.IDPRODUCTO, 0) <> 0 THEN ISNULL(PN.PORNACIONAL, 0) ELSE 0 END,
			UP.[TALLOS]
	FROM 	(
				SELECT	PPD.IDFINCA,
						PPD.IDPRODUCTO,
						PPD.IDVARIEDAD,
						PPD.IDYEAR,
						PPD.IDWEEK,
						SUM(PPD.TALLOS_LUNES) AS [1],
						SUM(PPD.TALLOS_MARTES) AS [2],
						SUM(PPD.TALLOS_MIERCOLES) AS [3],
						SUM(PPD.TALLOS_JUEVES) AS [4],
						SUM(PPD.TALLOS_VIERNES) AS [5],
						SUM(PPD.TALLOS_SABADO) AS [6],
						SUM(PPD.TALLOS_DOMINGO) AS [7]
				FROM 	PSI.PROYECCIONPRODUCCIONDIARIA PPD with(nolock) 
					JOIN	PRESUPUESTO.GENERAL.PERIODOSPRESUPUESTOS PR  WITH(NOLOCK)  
						ON	PPD.IDYEAR = PR.IDYEAR AND 
							PPD.IDWEEK = PR.ORDEN AND 
							PR.IDPERIODICIDAD = 1
				WHERE	PR.FECHAINICIO BETWEEN @FECINI AND @FECFIN OR 
						PR.FECHAFINAL BETWEEN @FECINI AND @FECFIN
				GROUP BY PPD.IDFINCA,PPD.IDYEAR,PPD.IDWEEK,PPD.IDPRODUCTO,PPD.IDVARIEDAD,PPD.TIPOCORTEAGRUPADO
				
			) PRODIA
	UNPIVOT
			(
				TALLOS FOR DIA IN ([1], [2], [3], [4], [5], [6], [7])
			) AS UP
		JOIN PRESUPUESTO.GENERAL.PERIODOSPRESUPUESTOS PRD  WITH(NOLOCK)  
			ON	PRD.Orden = UP.IdWeek AND 
				UP.DIA = DATEPART(DW, PRD.FECHAINICIO) AND
				PRD.idperiodicidad = 7
	WHERE	PRD.idYear IN (YEAR(@FECINI),YEAR(@FECFIN)) AND 
			PRD.Fechainicio BETWEEN @FECINI AND @FECFIN
	
	
	UPDATE 	UP
		SET 	 UP.PORCENTAJE_NACIONAL = CASE WHEN ISNULL(APN.IDPRODUCTO, 0) <> 0 THEN ISNULL(PN.PORNACIONAL, 0) ELSE 0 END
				,UP.PROYECCION = 
					CASE WHEN @Exportable = 1  
						THEN   
							UP.PROYECCION 
						ELSE 
							FLOOR(  UP.PROYECCION  * 100 / ( 100 - ISNULL(PN.PORNACIONAL, 0))  )  
						END 							
		FROM	#TMPPROYDIARIASINGRADOS UP
		LEFT JOIN	PSI.PORNALSEMANAL PN  WITH(NOLOCK)  
			ON	UP.IDFINCA = PN.IDFINCA AND 
				UP.IDPRODUCTO = PN.IDPRODUCTO AND 
				UP.IDVARIEDAD = PN.IDVARIEDAD AND 
				UP.IDYEAR = PN.IDYEAR AND 
				UP.IDWEEK = PN.IDWEEK
		LEFT JOIN	PSI.APPORNAL APN  WITH(NOLOCK)  ON PN.IDPRODUCTO = APN.IDPRODUCTO



	UPDATE 	UP
		SET 	UP.PROYECCION = 
					CASE WHEN @Exportable = 1  
					THEN   
						UP.PROYECCION 
					ELSE 
						FLOOR(  UP.PROYECCION  * 100 / ( 100 - ISNULL(PN.PORNACIONAL, 0))  )  
					END 						
		FROM	#TMPPROYDIARIA UP
			LEFT JOIN	PSI.PORNALSEMANAL PN  WITH(NOLOCK) 
				ON	UP.IDFINCA = PN.IDFINCA AND 
					UP.IDPRODUCTO = PN.IDPRODUCTO AND 
					UP.IDVARIEDAD = PN.IDVARIEDAD AND 
					UP.IDYEAR = PN.IDYEAR AND 
					UP.IDWEEK = PN.IDWEEK
			LEFT JOIN	PSI.APPORNAL APN  WITH(NOLOCK) 
				ON PN.IDPRODUCTO = APN.IDPRODUCTO
	

	------------------------------------------------------------------------------
  --						SUMAR DIFERENCIA DE TALLOS AL PRIMER REGISTRO PROYECCION			--
  ------------------------------------------------------------------------------
	
										
	UPDATE ST 
		SET ST.PROYECCION = ST.PROYECCION + INTERNA.DIFERENCIA
		FROM #TMPPROYDIARIA ST 
		JOIN (
													
				select  
				TMP.FECHAPRODUCCION,
				TMP.IDFINCA,
				TMP.IDPRODUCTO,
				TMP.IDVARIEDAD,	
				SUM(TMP.PROYECCION) as 'SUMTALLOS' ,
				R.PROYECCION - SUM(TMP.PROYECCION)  as 'DIFERENCIA' 	,
				R.PROYECCION AS PROYECCIONINICIO														
			FROM #TMPPROYDIARIA  TMP INNER JOIN #TMPPROYDIARIASINGRADOS R 
			ON	TMP.IDFINCA = R.IdFinca AND 
				TMP.IDPRODUCTO = R.IdProducto AND 
				TMP.IDVARIEDAD = R.IdVariedad AND 
				TMP.FECHAPRODUCCION = R.FECHAPRODUCCION
			GROUP BY
			TMP.FECHAPRODUCCION,
			TMP.IDFINCA,
			TMP.IDPRODUCTO,
			TMP.IDVARIEDAD,	
			R.PROYECCION	
			) AS INTERNA
			ON	ST.FECHAPRODUCCION = INTERNA.FECHAPRODUCCION AND
				ST.IDFINCA = INTERNA.IDFINCA AND 
				ST.IDPRODUCTO = INTERNA.IDPRODUCTO AND 
				ST.IDVARIEDAD = INTERNA.IDVARIEDAD
			WHERE ST.Row = 1


	SELECT 
	CAST(CONVERT(varchar,FECHAPRODUCCION,112) as INT) [FechaInt],
	idFinca,
	idVariedad,
	[PORCENTAJE_NACIONAL] p_Nal,
	idGrado,
	PROYECCION [Tallos]
	FROM #TMPPROYDIARIA

	
	DROP TABLE IF EXISTS #TMPPROYDIARIA, #TMPPROYDIARIASINGRADOS 

        '''.format(inicio, fin, exp)

    df_proyectado = excecute_query(
        strCon=str_con_DB10_FDIM,
        query=queryEst
    )

    if df_proyectado.shape[0] > 0:

        deleteDataToSql(
            strCon=str_con_FDIM_Planeacion,
            schema='fact',
            table=DST,
            where=['FechaInt >= {}'.format(intFecha)]

        )

        queryEmp = '''

				SELECT f.IdFinca, f.FincaCompleto, e.IdEmpresa
            FROM GLB.Fincas f
                LEFT JOIN GLB.vwEmpresa e
                    ON f.IdEmpresa = e.IdEmpresa

        '''

        df_empresas = excecute_query(
            strCon=str_con_DB10_FDIM,
            query=queryEmp
        )

        df = df_proyectado.merge(df_empresas, how='left',
                                 left_on='idFinca', right_on='IdFinca')

        queryGeo = '''
            SELECT [idFinca], MIN([idGeo]) [idGeo]
            FROM  [dim].[Geografia]
            GROUP BY [idFinca]
        '''

        df_geo = excecute_query(
            strCon=str_con_FDIM_Planeacion,
            query=queryGeo
        )

        df = df.merge(df_geo, how='left',
                      left_on='idFinca', right_on='idFinca')

        df['FechaEjecucion'] = ahora
        df['p_Nal'] = df['p_Nal'].astype(float)

        df = removeColumnsIn(
            dataFrame=df,
            listToRemove=['Finca'],
            literal=False
        )

        bulk_path = bulk_space_FDIM + 'ts_{}.txt'.format(DST)

        print(df.dtypes)

        # df = pd.DataFrame()
        df.to_csv(bulk_path, encoding='utf-8', sep=';', index=False)

        bulkInsert(
            strCon=str_con_FDIM_Planeacion,
            schema='fact',
            table=DST,
            data=df,
            file_path=bulk_path
        )

    else:
        print('No hay datos para insertar')

    # bulk_path = bulk_space_FDIM + 'ts_{}.txt'.format(DST)

    # df_estimado = pd.read_csv(
    #     bulk_path, encoding='utf-8', sep=';', index_col=False)

    # print(df_estimado.dtypes)


if __name__ == '__main__':
    run()
