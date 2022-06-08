
from sys import prefix
from tkinter import E
import pandas as pd
import datetime as dt


from pylib.py_lib import bulkInsert, deleteDataToSql, excecute_query, excutionTime, insertDataToSql_Alchemy, removeColumnsIn, workDirectory, parameters, stringConnect


ROOT = workDirectory()

DST = 'budget'


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
    dif = dt.timedelta(year=-1)
    anio = ahora + dif

    anio = ahora.strftime('%Y')
    ahoraSQL = ahora.strftime('%Y-%M-%d %H:%M:%S')
    # '2020-01-01'

    queryBud = \
        '''
        DECLARE @fecha  date = cast(getdate() as date)

        DECLARE @idYear as int = year(@fecha)
        DECLARE @idSemana as varchar(max) = datepart(ISO_WEEK, @fecha)

        EXEC PSI.PA_ConsultaproyeccionProduccionDiversificadosVSProduccionBruta @idYear,@idSemana, '','','2018,2019,2020,2021,2022,2023,2024,2025,2026'

    			'''
    # .format(anio)

    df_presupuesto = excecute_query(
        strCon=str_con_DB10_FDIM,
        query=queryBud
    )

    if df_presupuesto.shape[0] > 0:

        deleteDataToSql(
            strCon=str_con_FDIM_Planeacion,
            schema='fact',
            table=DST,
            where=['idYear = {}'.format(anio)]

        )

        bulk_path = bulk_space_FDIM + 'ts_{}.txt'.format(DST)

        df_presupuesto['FechaProyectada'] = \
            pd.to_datetime(
                df_presupuesto['FechaProyectada']
        )

        df_presupuesto['Origen'] = 'Sistema'
        df_presupuesto['FechaEjecucion'] = ahoraSQL

        df_presupuesto.to_csv(bulk_path, encoding='utf-8',
                              sep=';', index=False)
        df_presupuesto = pd.read_csv(bulk_path, sep=';', encoding='utf-8')

        bulkInsert(
            strCon=str_con_FDIM_Planeacion,
            schema='TMP',
            table=DST,
            data=df_presupuesto,
            file_path=bulk_path
        )

    else:
        print('No hay datos para insertar')


if __name__ == '__main__':
    run()
