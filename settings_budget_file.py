
from sys import prefix
from tkinter import E
import pandas as pd
import datetime as dt


from pylib.py_lib import bulkInsert, deleteDataToSql, excecute_query, excutionTime, insertDataToSql_Alchemy, removeColumnsIn, workDirectory, parameters, stringConnect


ROOT = workDirectory()

DST = 'ppt_Excel_test'


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
    dif = 10

    t_desde = int(ahora.strftime('%Y')) - dif
    # '2020-01-01'

    t_hasta = int(ahora.strftime('%Y')) + dif

    df_presupuesto_ex = excecute_query(
        strCon=str_con_FDIM_Planeacion,
        schema='tmp',
        table='ppt_Excel_test',
        fields=None,
        where=[
            "FechaProyectada = '{}'".format('1990-01-01')
        ]
    )

    if df_presupuesto_ex.shape[0] > 0:

        df_fecha = excecute_query(
            strCon=str_con_JP13M_ADW,
            schema='Common',
            table='Fecha_Tabla_ProcesoLocal',
            fields=['MediaSem', 'Ano', 'Mes', 'Semana'],
            where=[
                "FechaTipo = 'ISO'",
                "Ano between {} and {}".format(t_desde, t_hasta)
            ], groupby=['MediaSem', 'Ano', 'Mes', 'Semana']
        )
        df_presupuesto = pd.merge(
            df_presupuesto_ex,
            df_fecha,
            how='left',
            left_on=['idYear', 'idSemana'],
            right_on=['Ano', 'Semana']
        )

        df_presupuesto['idMes'] = df_presupuesto['Mes']
        df_presupuesto['FechaProyectada'] = df_presupuesto['MediaSem']

        df_presupuesto = df_presupuesto.drop(
            columns=['Ano', 'Semana', 'Mes', 'MediaSem'])

        # df_presupuesto.to_csv('presupuesto.csv')

        bulk_path = bulk_space_FDIM + 'ts_{}.txt'.format(DST)

        deleteDataToSql(
            strCon=str_con_FDIM_Planeacion,
            schema='tmp',
            table=DST,
            where=[
                "FechaProyectada = '{}'".format('1990-01-01')
            ]
        )
        print(df_presupuesto.dtypes)

        df_presupuesto.to_csv(bulk_path, encoding='utf-8',
                              sep=';', index=False)

        bulkInsert(
            strCon=str_con_FDIM_Planeacion,
            schema='tmp',
            table=DST,
            data=df_presupuesto,
            file_path=bulk_path
        )
    else:
        print('No hay datos para insertar')


if __name__ == '__main__':
    run()
