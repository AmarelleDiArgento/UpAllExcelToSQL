
import pandas as pd
import datetime as dt


from pylib.py_lib import bulkInsert, deleteDataToSql, excecute_query, excutionTime, insertDataToSql_Alchemy, removeColumnsIn, workDirectory, parameters, stringConnect


ROOT = workDirectory()

DST = 'Producto'


def read_conexion(name='general'):
    dbCon, files, is_test, bulk_space = parameters(
        ROOT=ROOT,
        config_file='configdb.json',
        service_name=name
    )

    return (is_test, dbCon, bulk_space)


# def ajustar_agrupador(x):
#     print(x, x['Producto'], x['Maestro'])
#     if x['Producto'].str.contains('PLANTA MADRE') == True:
#         x['Maestro'] = 'PLANTA MADRE'
#         x['idTipoProducto'] = 6
#         x['Tipo'] = 'ESQUEJES'
#         return x

#     if x['Producto'].str.contains('ENRAIZAMIENTO') == True:
#         x['Maestro'] = 'ENRAIZAMIENTO'
#         x['idTipoProducto'] = 6
#         x['Tipo'] = 'ESQUEJES'
#         return x

#     if x['Producto'].str.contains('ENRAIZAMIENTO', regex=False) == True:
#         x['Maestro'] = 'ENRAIZAMIENTO'
#         x['idTipoProducto'] = 6
#         x['Tipo'] = 'ESQUEJES'
#         return x

#     if x['Maestro'].str.contains('PROPAG', regex=False) == True:
#         x['Agrupador'] = 'PROPAG'
#         x['idTipoProducto'] = 0
#         x['Tipo'] = 'PROPAGACION'
#         return x

#     if x['Maestro'].str.contains('POTTED', regex=False) == True:
#         x['Agrupador'] = 'POTTED'
#         x['idTipoProducto'] = 0
#         x['Tipo'] = 'POTTED'
#         return x

#     else:
#         return x


@excutionTime
def run():

    is_test_FDIM, ServFDIM_DB_Planeacion, bulk_space_FDIM = read_conexion(
        'ServFDIM_DB_Planeacion')
    str_con_FDIM_Planeacion = stringConnect(ServFDIM_DB_Planeacion)

    # print('ServFDIM', ServFDIM_DB_Planeacion, bulk_space_FDIM)

    is_test_FDIM, ServDB10_DB_FDIM, bulk_space_DB10 = read_conexion(
        'ServDB10_DB_FDIM')
    str_con_DB10_FDIM = stringConnect(ServDB10_DB_FDIM)

    # print('ServFDIM', ServFDIM_DB_Planeacion, bulk_space_FDIM)

    ahora = dt.datetime.now()

    df_variedades = excecute_query(
        strCon=str_con_FDIM_Planeacion,
        schema='dim',
        table='Producto',
        fields=['idVariedad'],
        groupby=['idVariedad']
    )

    df_variedades_fdim = excecute_query(
        strCon=str_con_DB10_FDIM,
        schema='GLB',
        table='Variedades',
        fields=['idVariedad']
    )

    WHERE = ''

    if df_variedades.empty == False:

        df_variedades = df_variedades.merge(
            df_variedades_fdim,
            on='idVariedad',
            how='left',
            indicator=True
        )
        idsVar = ','.join(df_variedades['idVariedad'].astype(str))
        WHERE = 'WHERE idVariedad in (' + idsVar + ')'

        df_variedades = df_variedades[df_variedades['_merge'] == 'left_only']

    else:
        df_variedades = df_variedades_fdim

    if df_variedades.shape[0] > 0:

        # # # Consulta Productos DB10_FDIM
        queryPro = '''

            SELECT
                p.[idProductoMaestro] [idMaestro]
                ,pm.[ProductoMaestro] [Maestro]
                ,pm.[NombreEspañol] [Maestro ES]
                --,pm.[NombreTampa]
                --,pm.[NombreAtlas]
                --,pm.[idEstado]
                ,pm.[NombreCientifico] [Maestro Nom Botanico]
                ,tp.[idTipoProducto]
                ,tp.[TipoProducto] [Tipo]
                --,tp.[FactorDeslizadero]
                ,p.[idProducto]
                ,p.[Producto]
                ,ep.[abreprod] [Prod]
                ,p.[NomEspanol] [Producto ES]
                ,p.[NomEspanol] [Producto Nom Botanico]
                ,v.[idVariedad]
                ,v.[Variedad]
                --,p.[idAforador]
                --,p.[idProductoPresup]
                --,pp.[ProductoMaestro] [ProductoPresup]
                --,p.[Verde]
                --,p.[EntradaFDIM]
                --,p.[MuestreoCalidad]
                --,p.[SNAplicaMuestreo]
                --,p.[IndDiversificado]
                --,p.[Periodicidad]
                --,p.[SNAplicaProyeccion]
                --,p.[idEstado]
                --,p.[ParaPropagacion]
                --,p.[snNacionalAuto]
                --,p.[GradoPorVariedad]
                --,p.[idCategoriaProducto]
                --,v.[idProducto]
                ,v.[idColor]
                ,c.[Color]
                ,v.[idColorAgrupado]
                ,ca.[ColorAgrupado]
                --,v.[DiasVegetativo]
                --,v.[DiasVegetativoAuto]
                --,v.[PercentRetorno]
                --,v.[CicloProduccion]
                ,v.[Activo]
                ,v.[NitBreeder]
                --,v.[IndDiversificado]
                ,v.[snTinturada]
                ,v.[Owner]
                ,v.[DateOwner]
                --,v.[snAgrupaColor]
            FROM [FDIM].[GLB].[Variedades] v with(nolock)
                LEFT JOIN [FDIM].[GLB].[Productos] p with(nolock)
                    ON p.IDProducto = v.IDProducto
                LEFT JOIN [FDIM].[GLB].[Colores] c with(nolock)
                    ON v.IDColor = c.IDColor
                LEFT JOIN [FDIM].[GLB].[ProductosMaestros] pm with(nolock)
                    ON p.IdProductoMaestro = pm.IdProductoMaestro
                LEFT JOIN [FDIM].[GLB].[TipoProducto] tp with(nolock)
                    ON pm.idTipoProducto = tp.idTipoProducto
                LEFT JOIN [FDIM].[GLB].[ColoresAgrupados] ca with(nolock)
                    ON v.idColorAgrupado = ca.idColorAgrupado
                LEFT JOIN [EFLOWER].[dbo].[GLBproducto] ep with(nolock)
                    ON p.IDProducto = ep.codprod
            {}
            ORDER BY v.IDVariedad --ProductoMaestro, Producto,

        '''.format(WHERE)

        df_productos = excecute_query(
            strCon=str_con_DB10_FDIM,
            query=queryPro
        )

        df_agrupador = excecute_query(
            strCon=str_con_FDIM_Planeacion,
            schema='trn',
            table='ProductoAgrupador',
            fields=['idProductoMaestro', 'ProductoAgrupador']
        )

        df_productos = df_productos.merge(df_agrupador, how='left',
                                          left_on='idMaestro', right_on='idProductoMaestro')

        removeColumnsIn(df_productos, ['idProductoMaestro'])

        df_productos['Activo'] = df_productos['Activo'].apply(
            lambda x: 1 if x else 0)

        df_productos['snTinturada'] = df_productos['snTinturada'].apply(
            lambda x: 1 if x else 0)

        df_productos['Color-Rojo-Spray'] = df_productos['Color'].apply(
            lambda x: 'ROJOS' if x == 'RED' else 'COLORES')

        df_productos['Color-Rojo-Spray'] = df_productos.apply(
            lambda x: 'SPRAY' if x['Producto'] == 'ROSES SPRAY' else x['Color-Rojo-Spray'], axis=1)

        print('Productos', df_productos.shape)

        bulk_path = bulk_space_FDIM + 'ts_{}.txt'.format(DST)

        # df = pd.DataFrame()
        df_productos.to_csv(bulk_path, encoding='utf-8', sep=';', index=False)

        # df_productos['ProductoAgrupador'] = df_productos.apply(
        #     lambda variedad:  ajustar_agrupador(variedad), axis=1)

        bulkInsert(
            strCon=str_con_FDIM_Planeacion,
            schema='dim',
            table=DST,
            data=df_productos,
            file_path=bulk_path
        )

    else:
        print('No hay datos para insertar')


if __name__ == '__main__':
    run()
