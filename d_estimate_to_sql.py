
from tkinter import E
import pandas as pd
import datetime as dt


from pylib.py_lib import bulkInsert, deleteDataToSql, excecute_query, excutionTime, insertDataToSql_Alchemy, removeColumnsIn, workDirectory, parameters, stringConnect


ROOT = workDirectory()

DST = 'Estimado'


def read_conexion(name='general'):
    dbCon, files, is_test, bulk_space = parameters(
        ROOT=ROOT,
        config_file='configdb.json',
        service_name=name
    )

    return (is_test, dbCon, bulk_space)


def add_new_element(strCon, schema, table, df_new_elements,
                    df_old_elements, column):

    data = pd.DataFrame(df_new_elements[column].unique(), columns=[column])

    if df_old_elements.empty == False:
        data = pd.DataFrame(data, columns=[column])

        data = data.merge(df_old_elements, on=column,
                          how='left', indicator=True)
        data = data[data['_merge'] == 'left_only']

    if data.empty == False:
        data = removeColumnsIn(
            dataFrame=data,
            listToRemove=['_merge', 'id' + column]
        )
        data = data.sort_values(column)

        insertDataToSql_Alchemy(strCon=strCon, schema=schema,
                                table=table, data=data, index=True)

    return excecute_query(
        strCon=strCon,
        schema=schema,
        table=table,
        fields=['id' + column, column]
    )


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
    exp = 1
    # Consulta Estimados DB10_FDIM
    queryEst = '''


	DECLARE @FECHA_IN DATE = '{}'
	DECLARE @FECHA_FI DATE = '{}'

	------------------------------------------------------------------------------
	--	Tabla temporal para guardar los resultados del proceso					--
	------------------------------------------------------------------------------
	IF	OBJECT_ID ('tempdb..#tmpResultado') IS NOT NULL
	BEGIN
		DROP TABLE #tmpResultado
	END
	CREATE TABLE #tmpResultado
	(
		IDFINCA					SMALLINT NOT NULL,
		IDPRODUCTO				SMALLINT NOT NULL,
		IDVARIEDAD				SMALLINT NOT NULL,
		IDYEAR					SMALLINT,
		IDWEEK					SMALLINT,
		FECHA					SMALLDATETIME ,
		PORCENTAJE_NACIONAL		NUMERIC(6,2),
		ESTIM2PM				INT ,
		ESTIM2PMPERIODO1		INT ,
		ESTIM2PMPERIODO2		INT ,
		IdGrado					INT ,
		PorcGrado				numeric(5,2),
		DescripcionIngles       VARCHAR(Max),
		ROW						INT
--		CONSTRAINT PK_tmpResultadot_TMP PRIMARY KEY (IDFINCA, IDPRODUCTO, IDVARIEDAD, FECHA)
	)


	------------------------------------------------------------------------------------------------------------------------------------------------------------
   	--                                                     Captura el Estimado Diario de Producción		 		    	                                      --
   	------------------------------------------------------------------------------------------------------------------------------------------------------------

	INSERT INTO #tmpResultado
	SELECT		E.IDFINCA,
				E.IDPRODUCTO,
				E.IDVARIEDAD,
				PP.IDYEAR,
				PP.ORDEN,
				E.FECHAESTIMACION FECHA,
				ISNULL(PN.PORNACIONAL, 0) PORCENTAJE_NACIONAL,
			    FLOOR( E.TALLOSPERIODO03 - (E.TALLOSPERIODO03 * ISNULL(PN.PORNACIONAL, 0) / 100 ))  AS ESTIM2PM,
				0 AS ESTIM2PMPERIODO1,
				0  AS ESTIM2PMPERIODO2,
				E.IdGrado,
				E.ValorGrado PorcGrado,
				G.DescripcionIngles	 ,
				ROW_NUMBER() OVER(PARTITION BY E.IDVARIEDAD,E.FECHAESTIMACION,E.IDFINCA,E.IDPRODUCTO ORDER BY E.FECHAESTIMACION DESC) [Row]

	FROM		PSI.EstimadoDiarioProduccionCopy E	 with(nolock)
	JOIN		PSI.GradosdeCalidad G  with(nolock)  on E.idGrado = G.IdGrado
	JOIN		PRESUPUESTO.GENERAL.PERIODOSPRESUPUESTOS PP with(nolock)
				ON
					E.FECHAESTIMACION BETWEEN PP.FECHAINICIO AND PP.FECHAFINAL AND
					PP.IDPERIODICIDAD = 1
	LEFT JOIN		PSI.PORNALSEMANAL PN  with(nolock)
				ON E.IDFINCA = PN.IDFINCA AND
					E.IDPRODUCTO = PN.IDPRODUCTO AND
					E.IDVARIEDAD = PN.IDVARIEDAD AND
					PP.IDYEAR = PN.IDYEAR AND
					PP.ORDEN = PN.IDWEEK
	LEFT JOIN	PSI.APPORNAL APN  with(nolock) ON
					PN.IDPRODUCTO = APN.IDPRODUCTO
	WHERE	 PP.Fechainicio >= CONCAT(@FECHA_IN , ' 00:00:00') and pp.Fechafinal <=  CONCAT(@FECHA_FI , ' 23:59:59')


	------------------------------------------------------------------------------------------------------------------------------------------------------------
   	--                                              SUMAR DIFERENCIA DE TALLOS AL PRIMER REGISTRO ESTIMADO					                                  --
   	------------------------------------------------------------------------------------------------------------------------------------------------------------


	UPDATE ST
		SET ST.ESTIM2PM = ESTIM2PM + INTERNA.DIFERENCIA3

	FROM #tmpResultado ST
	JOIN (

		SELECT
			TMP.FECHA,
			TMP.IDFINCA,
			TMP.IDPRODUCTO,
			TMP.IDVARIEDAD,
			SUM(TMP.ESTIM2PM) as [SUMTALLOS] ,
			FLOOR( R.TallosPeriodo03 - (R.TallosPeriodo03 * ISNULL(TMP.PORCENTAJE_NACIONAL, 0) / 100 ))  -  SUM(TMP.ESTIM2PM) 'DIFERENCIA3',
			avg(R.TallosPeriodo03) TallosPeriodo03,
			FLOOR( R.TallosPeriodo03 - (R.TallosPeriodo03 * ISNULL(TMP.PORCENTAJE_NACIONAL, 0) / 100 ))  PROYECCIONINICIO
		FROM #tmpResultado  TMP
			INNER JOIN PSI.EstimadoDiarioProduccion R  with(nolock)
				ON TMP.IDFINCA = R.IdFinca AND TMP.IDPRODUCTO = R.IdProducto AND TMP.IDVARIEDAD = R.IdVariedad AND TMP.FECHA = R.FechaEstimacion
		--WHERE tmp.IdFinca = 2 and tmp.IdProducto = 1 and tmp.IdVariedad = 132 and tmp.FECHA = '2022-05-10 00:00:00'
		GROUP BY
			TMP.FECHA,
			TMP.IDFINCA,
			TMP.IDPRODUCTO,
			TMP.IDVARIEDAD,
			R.TallosPeriodo03,
			R.TallosPeriodo01,
			R.TallosPeriodo02,
			TMP.PORCENTAJE_NACIONAL


	) AS INTERNA
		ON ST.FECHA = INTERNA.FECHA
			AND ST.IDFINCA = INTERNA.IDFINCA
			AND ST.IDPRODUCTO = INTERNA.IDPRODUCTO
			AND ST.IDVARIEDAD = INTERNA.IDVARIEDAD
	WHERE ST.Row = 1


		--------------- Distribuir Grados Periodo 1 y 2 con los grados ------------------

	UPDATE ST
		SET ST.ESTIM2PMPERIODO1 = ( E.TALLOSPERIODO01 * st.PorcGrado / 100  )  ,
			ST.ESTIM2PMPERIODO2 = ( E.TallosPeriodo02 * st.PorcGrado / 100 )
	FROM #tmpResultado ST
		INNER JOIN	PSI.EstimadoDiarioProduccion E  with(nolock)
			ON	E.IdFinca = ST.IDFINCA AND
				E.IdProducto = ST.IDPRODUCTO AND
				E.IdVariedad = ST.IDVARIEDAD AND
				E.FechaEstimacion = ST.FECHA

		  --------------- Distribuir Grados Periodo 1 y 2 con los grados ------------------

	UPDATE ST
			SET ST.ESTIM2PMPERIODO1 = ESTIM2PMPERIODO1 + INTERNA.DIFERENCIA1,
				ST.ESTIM2PMPERIODO2 = ESTIM2PMPERIODO2 + INTERNA.DIFERENCIA2
	FROM #tmpResultado ST
		JOIN (

				SELECT
					TMP.FECHA,
					TMP.IDFINCA,
					TMP.IDPRODUCTO,
					TMP.IDVARIEDAD,
					SUM(TMP.ESTIM2PM) as 'SUMTALLOS' ,
					(CASE when 1 = {} THEN FLOOR( R.TallosPeriodo01 - (R.TallosPeriodo01 * ISNULL(TMP.PORCENTAJE_NACIONAL, 0) / 100 ))  ELSE R.TallosPeriodo01 END) -  SUM(TMP.ESTIM2PMPERIODO1)      as 'DIFERENCIA1' ,
					(CASE when 1 = {} THEN FLOOR( R.TallosPeriodo02 - (R.TallosPeriodo02 * ISNULL(TMP.PORCENTAJE_NACIONAL, 0) / 100 ))  ELSE R.TallosPeriodo02 END) -  SUM(TMP.ESTIM2PMPERIODO2)      as 'DIFERENCIA2'
							-- @Exportable =
				FROM #tmpResultado  TMP
					INNER JOIN PSI.EstimadoDiarioProduccion R  with(nolock)
						ON	TMP.IDFINCA = R.IdFinca AND
							TMP.IDPRODUCTO = R.IdProducto AND
							TMP.IDVARIEDAD = R.IdVariedad AND
							TMP.FECHA = R.FechaEstimacion
				GROUP BY
					TMP.FECHA,
					TMP.IDFINCA,
					TMP.IDPRODUCTO,
					TMP.IDVARIEDAD,
					R.TallosPeriodo03,
					R.TallosPeriodo01,
					R.TallosPeriodo02,
					TMP.PORCENTAJE_NACIONAL


			) AS INTERNA
				ON	ST.FECHA = INTERNA.FECHA AND
					ST.IDFINCA = INTERNA.IDFINCA AND
					ST.IDPRODUCTO = INTERNA.IDPRODUCTO AND
					ST.IDVARIEDAD = INTERNA.IDVARIEDAD
			WHERE ST.Row = 1


			SELECT	CAST(CONVERT(varchar,[Fecha],112) as INT) [FechaInt],
					[idFinca],
					[idVariedad],
					[idGrado],
					[PORCENTAJE_NACIONAL] [p_Nal],
					[PorcGrado] [p_Gra],
					[ESTIM2PM] Estimado_1,
					[ESTIM2PMPERIODO1] Estimado_2,
					[ESTIM2PMPERIODO2] Estimado,
					1 [Estado]
			FROM #tmpResultado


        '''.format(inicio, fin, exp, exp)

    df_estimado = excecute_query(
        strCon=str_con_DB10_FDIM,
        query=queryEst
    )

    if df_estimado.shape[0] > 0:

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

        df = df_estimado.merge(df_empresas, how='left',
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
        # .strftime('%Y-%m-%d %H:%M:%S')
        df['p_Gra'] = df['p_Gra'].astype(float)
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
