
from importlib.metadata import files
from importlib.resources import path
import pandas as pd
import datetime as dt


from pylib.py_lib import add_new_element, bulkInsert, deleteDataToSql, excecute_query, excutionTime, removeColumnsIn, workDirectory, parameters, stringConnect


ROOT = workDirectory()

DST = 'Produccion'


def read_conexion(name='general'):
    dbCon, files, is_test, bulk_space = parameters(
        ROOT=ROOT,
        config_file='configdb.json',
        service_name=name
    )

    return (is_test, dbCon, bulk_space)


@excutionTime
def run():
    is_test_DB12, ServDB12_DB_FDIM_Reports, bulk_space_DB12 = read_conexion(
        'ServDB12_DB_FDIM_Reports')
    str_con_FDIM_Reports = stringConnect(ServDB12_DB_FDIM_Reports)

    # print('ServDB12', str_con_FDIM_Reports, bulk_space_DB12)

    is_test_FDIM, ServFDIM_DB_Planeacion, bulk_space_FDIM = read_conexion(
        'ServFDIM_DB_Planeacion')
    str_con_FDIM_Planeacion = stringConnect(ServFDIM_DB_Planeacion)

    # print('ServFDIM', ServFDIM_DB_Planeacion, bulk_space_FDIM)

    is_test_FDIM, ServDB10_DB_FDIM, bulk_space_DB10 = read_conexion(
        'ServDB10_DB_FDIM')
    str_con_DB10_FDIM = stringConnect(ServDB10_DB_FDIM)

    # print('ServFDIM', ServFDIM_DB_Planeacion, bulk_space_FDIM)

    ahora = dt.datetime.now()

    diferencia = dt.timedelta(
        days=0,
        hours=0,
        minutes=10
    )
    print(diferencia)
    desde = ahora - diferencia

    t_desde = desde.strftime('%Y-%m-%d %H:00:00')  # '2020-01-01 00:00:00'
    t_hasta = ahora.strftime('%Y-%m-%d %H:%M:%S')
    print(t_desde)
    print(t_hasta)

    intDay = int(desde.strftime('%Y%m%d'))
    intToday = int(ahora.strftime('%Y%m%d'))

    intHour = int(desde.strftime('%H'))
    print(intDay, intToday, intDay != intToday)
    if intDay != intToday:
        intHour = 0
        t_desde = desde.strftime('%Y-%m-%d 00:00:00')  # '2020-01-01 00:00:00'

    print(intHour)

    # Consulta Producción
    queryPro = '''

    		SELECT
    			[IdTipoMovimiento]
    			,[Tipo]
    			,CAST(CONVERT(varchar,[FechaJornada],112) as INT) [FechaInt]
    			,[idPostcosecha]
    			,[idBloque]
                ,[idFinca]
    			,[idVariedad]
    			,[TipoCorte]
    			,SUM([TotalTallos]) Tallos
    			,DATEPART(HOUR, [FechaSistema]) Hora
    			,[Marca]
    			,[idGradodeCalidad]
    			,GETDATE() [FechaEjecucion]
    		FROM [FDIM_Reports].[dbo].[MovimientoInventario] WITH(NOLOCK)
    		WHERE 	IDTIPOMOVIMIENTO IN ('EP', 'RI', 'AP') AND
    						[FechaSistema] between '{}' and '{}'
    		GROUP BY
    			[IdTipoMovimiento]
    			,[Tipo]
    			,[FechaJornada]
    			,[idPostcosecha]
    			,[idBloque]
                ,[idFinca]
    			,[idVariedad]
    			,[TipoCorte]
    			,DATEPART(HOUR, [FechaSistema])
    			,[Marca]
    			,[idGradodeCalidad]

    '''.format(t_desde, t_hasta)

    df_produccion = excecute_query(
        strCon=str_con_FDIM_Reports,
        query=queryPro
    )

    if df_produccion.empty == False:

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

        df = df_produccion.merge(df_empresas, how='left',
                                 left_on='idFinca', right_on='IdFinca')

        queryGeo = '''
            SELECT [idBloque], MIN(idGeo) [idGeo] 
            FROM [dim].[Geografia] 
            GROUP BY [idBloque]
        '''

        df_geo = excecute_query(
            strCon=str_con_FDIM_Planeacion,
            query=queryGeo
        )

        df = df.merge(df_geo, how='left',
                      left_on='idBloque', right_on='idBloque')

        df_marca = excecute_query(
            strCon=str_con_FDIM_Planeacion,
            schema='dim',
            table='Marca',
            fields=['idMarca', 'Marca']

        )

        df_marca = add_new_element(
            strCon=str_con_FDIM_Planeacion,
            schema='dim',
            table='Marca',
            df_new_elements=df_produccion,
            df_old_elements=df_marca,
            column='Marca'
        )

        df_corte = excecute_query(
            strCon=str_con_FDIM_Planeacion,
            schema='dim',
            table='TipoCorte',
            fields=['idTipoCorte', 'TipoCorte']
        )

        df_corte = add_new_element(
            strCon=str_con_FDIM_Planeacion,
            schema='dim',
            table='TipoCorte',
            df_new_elements=df_produccion,
            df_old_elements=df_corte,
            column='TipoCorte'
        )

        df = df.merge(df_corte, how='left',
                      on='TipoCorte')

        df = df.merge(df_marca, how='left',
                      on='Marca')

        df = removeColumnsIn(
            dataFrame=df,
            listToRemove=['Marca', 'TipoCorte'],
            literal=True
        )

        df = removeColumnsIn(
            dataFrame=df,
            listToRemove=['Finca', 'Bloque'],
            literal=False
        )

        bulk_path = bulk_space_FDIM + 'ts_{}.txt'.format(DST)

        # df = pd.DataFrame()
        print('df Size:', df.shape)
        df.to_csv(bulk_path, encoding='utf-8', sep=';', index=False)

        deleteDataToSql(
            strCon=str_con_FDIM_Planeacion,
            schema='fact',
            table=DST,
            # 'produccion',
            where=['[FechaInt] >= {}'.format(intDay),
                   '[Hora] >= {}'.format(intHour)
                   ]
        )

        bulkInsert(
            strCon=str_con_FDIM_Planeacion,
            schema='fact',
            table=DST,
            data=df,
            file_path=bulk_path
        )

    else:
        print('No hay datos para insertar')

    # df = pd.read_csv(path, sep=';', encoding='utf-8')

    # insertDataToSql_Alchemy(

    #     strCon=str_con_FDIM_Planeacion,
    #     schema='fact',
    #     table='produccion',
    #     data=df,
    #     index=False

    # )


if __name__ == '__main__':
    run()
