
from importlib.metadata import files
from importlib.resources import path
import pandas as pd
import datetime as dt

from pylib.py_lib import bulkInsert, excecute_query, excutionTime, insertDataToSql, insertDataToSql_Alchemy, removeColumnsIn, trimAllColumns, workDirectory, parameters, stringConnect


ROOT = workDirectory()


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
            listToRemove=['_merge', 'ID_' + column]
        )

        insertDataToSql_Alchemy(strCon=strCon, schema=schema,
                                table=table, data=data, index=True)

    return excecute_query(
        strCon=strCon,
        schema=schema,
        table=table,
        fields=['ID_' + column, column]
    )


@excutionTime
def run():
    is_test_DB12, ServDB12_DB_FDIM_Reports, bulk_space = read_conexion(
        'ServDB12_DB_FDIM_Reports')
    str_con_FDIM_Reports = stringConnect(ServDB12_DB_FDIM_Reports)

    is_test_FDIM, ServFDIM_DB_Planeacion, bulk_space_FDIM = read_conexion(
        'ServFDIM_DB_Planeacion')
    str_con_FDIM_Planeacion = stringConnect(ServFDIM_DB_Planeacion)

    desde = '2022-01-01'
    hasta = '2022-05-20'
    # desde = '2022-05-19'
    # hasta = '2022-05-19'

    query = '''

    		SELECT
    			[IdTipoMovimiento]
    			,[Tipo]
    			,[FechaJornada]
    			,[idPostcosecha]
    			,[idBloque]
    			,[idVariedad]
    			,[TipoCorte]
    			,SUM([TotalTallos]) Tallos
    			,DATEPART(HOUR, [FechaSistema]) Hora
    			,[Marca]
    			,[idGradodeCalidad]
    			,GETDATE() [FechaEjecucion]
    		FROM [FDIM_Reports].[dbo].[MovimientoInventario] WITH(NOLOCK)
    		WHERE 	IDTIPOMOVIMIENTO IN ('EP', 'RI', 'AP') AND
    						FechaJornada between '{}' and '{}'
    		GROUP BY
    			[IdTipoMovimiento]
    			,[Tipo]
    			,[FechaJornada]
    			,[idPostcosecha]
    			,[idBloque]
    			,[idVariedad]
    			,[TipoCorte]
    			,DATEPART(HOUR, [FechaSistema])
    			,[Marca]
    			,[idGradodeCalidad]

    '''.format(desde, hasta)

    df_produccion = excecute_query(
        strCon=str_con_FDIM_Reports,
        query=query
    )

    df_calendario = excecute_query(
        strCon=str_con_FDIM_Planeacion,
        schema='dim',
        table='Calendario',
        fields=['IdDate', 'Date']

    )

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

    df = df_produccion.merge(df_calendario, how='left',
                             left_on='FechaJornada', right_on='Date')

    df = removeColumnsIn(
        dataFrame=df,
        listToRemove=['FechaJornada', 'Date'],
        literal=True
    )

    df = df.merge(df_corte, how='left',
                  on='TipoCorte')

    df = removeColumnsIn(
        dataFrame=df,
        listToRemove=['TipoCorte'],
        literal=True
    )

    df = df.merge(df_marca, how='left',
                  on='Marca')

    df = removeColumnsIn(
        dataFrame=df,
        listToRemove=['Marca'],
        literal=True
    )

    path = bulk_space_FDIM + 'ts_produccion.txt'

    df.to_csv(path, encoding='utf-8', sep=';', index=False)

    bulkInsert(
        strCon=str_con_FDIM_Planeacion,
        schema='fact',
        table='produccion',
        file_path=path
    )

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
