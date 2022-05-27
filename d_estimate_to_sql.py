
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
    dif = dt.timedelta(minutes=1)
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

    intFecha = int(inicio.strftime('%Y%m%d'))

    print(inicio, fin)
    exp = 1
    # Consulta Estimados DB10_FDIM
    queryEst = '''
        SELECT  
                1 [Estado]
            ,ESTIMADO [Estimado]
            ,ESTIMADO1 [Estimado_1]
            ,ESTIMADO2 [Estimado_2]
            ,CAST(CONVERT(varchar,[FECHAPRODUCCION],112) AS INT) [FechaInt]
            ,[IdGradoMaestro] [idGrado]
            ,[idVariedad]
            ,[PORCENTAJE_NACIONAL] [p_Nal]
            ,0.0 [p_Gra]
            ,[idFinca]
            ,GETDATE() [FechaEjecucion]
        FROM [FDIM].[PSI].[Estimado_Diario_Por_Fecha_Con_Grados] e WITH(NOLOCK)
        WHERE FECHAPRODUCCION BETWEEN '{}' AND '{}'

        '''.format(inicio, fin)

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

        # print(df.dtypes)

        df = pd.DataFrame(df,
                          columns=['Estado', 'Estimado', 'Estimado_1', 'Estimado_2', 'FechaInt',
                                   'idGrado', 'idVariedad', 'p_Gra', 'p_Nal', 'IdEmpresa', 'idGeo', 'FechaEjecucion'])
        # df.to_csv(bulk_path, encoding='utf-8', sep=';', index=False)

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
