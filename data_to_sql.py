
from importlib.metadata import files
from importlib.resources import path
import pandas as pd
import datetime as dt


from pylib.py_lib import bulkInsert, deleteDataToSql, excecute_query, excutionTime, insertDataToSql_Alchemy, removeColumnsIn, workDirectory, parameters, stringConnect


ROOT = workDirectory()

DST = 'planeacion_new'


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
    # diferencia = dt.timedelta(hours=12, minutes=00)
    # ahora = ahora - diferencia

    diferencia = dt.timedelta(hours=3395, minutes=8)
    print(diferencia)
    desde = ahora - diferencia

    t_desde = desde.strftime('%Y-%m-%d %H:00:00')
    t_hasta = ahora.strftime('%Y-%m-%d %H:%M:%S')
    print(t_desde)
    print(t_hasta)

    intDay = int(desde.strftime('%Y%m%d'))
    intToday = int(desde.strftime('%Y%m%d'))
    
    intHour = int(desde.strftime('%H'))
    if intDay != intToday:
        intHour = 0
        t_desde = desde.strftime('%Y-%m-%d 00:00:00')

    print(intHour)

    # desde = '2022-05-19'
    # hasta = '2022-05-19'

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

    deleteDataToSql(
        strCon=str_con_FDIM_Planeacion,
        schema='fact',
        table=DST,
        # 'produccion',
        where=['[FechaInt] >= {}'.format(intDay),
               '[Hora] >= {}'.format(intHour)
               ]
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

        df_marca = excecute_query(
            strCon=str_con_FDIM_Planeacion,
            schema='dim',
            table='Marca',
            fields=['ID_Marca', 'Marca']

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
            fields=['ID_TipoCorte', 'TipoCorte']
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
            listToRemove=['Finca'],
            literal=False
        )

        bulk_path = bulk_space_FDIM + 'ts_produccion.txt'

        df.to_csv(bulk_path, encoding='utf-8', sep=';', index=False)

        bulkInsert(
            strCon=str_con_FDIM_Planeacion,
            schema='fact',
            table=DST,
            # --'produccion_new',
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
