

from pandas import DataFrame
import datetime as dt

from pylib.py_lib import blockExtractDataFile, convertXlsToCsv, excutionTime, extractDataFile, packageForFileError, removeDirectory, removeFile, searchFilesByContentInTitle, parameters, workDirectory, insertDataToSql_Alchemy, stringConnect


ROOT = workDirectory()
DB_CON, FILES, ISTEST, BULK_SPACE = parameters(ROOT)


strCon = stringConnect(DB_CON)


def create_url(file):
    return {
        True: r'{}\{}\\'.format(ROOT, file['dir']),
        False: file['dir']+'\\'
    }[file['local']]


def remove_csv(csv_path):
    print(csv_path)
    if csv_path is not None:
        removeDirectory(csv_path)


def ajustes(df, file):
    df['FechaProyectada'] = '1990-01-01'
    # df.to_csv(file['dir'] + 'test.csv', encoding='utf-8', sep=';')
    return df


def execute_files_planning():
    logs = ''
    try:

        for file in FILES:
            logs = file['logs']
            url = create_url(file)

            # remove_csv(file_path)
            df = DataFrame()

            if file['directory']:
                files = searchFilesByContentInTitle(
                    file_path=url,
                    parm=dict(file['regex']))

                if len(files) > 0:

                    df = blockExtractDataFile(
                        path=url,
                        files=files,
                        file=file,
                        # sheets=file['sheets'],
                        # archive=file['archive'],
                        # firstRow=file['firstRow'],
                    )
                else:
                    print('No files found for process')

            else:

                file_path = convertXlsToCsv(
                    url=url,
                    file=file['file'],
                    sheets=file['sheets'],
                    isTest=ISTEST
                )
                df = extractDataFile(
                    path=url,
                    file=file['file'],
                    sheets=file['sheets']
                )

                print(df.shape)
                if df.shape[0] > 0:
                    if file['regex']['content'] == 'ppt_':
                        df = ajustes(df, file=file)

                    df.rename(columns={'source': 'Origen'}, inplace=True)
                    df['Origen'] = 'Usuario'
                    df['FechaEjecucion'] = dt.datetime.now().strftime('%Y-%m-%d')
                    # # df.groupby(by=).()
                    # depure(df=df, where=file['depureColumns'])

                    insertDataToSql_Alchemy(
                        strCon=strCon,
                        schema=file['schema'],
                        table=file['table'],
                        data=df,
                        truncate=file['truncate'],
                        depureColumns=file['depureColumns'],
                        index=file['index']
                    )

    except (AttributeError, KeyError) as e:
        packageForFileError(
            logs,
            "Error: {}.\n".format(e),
            url
        )


@excutionTime
def run():
    # print(FILES)
    execute_files_planning()


if __name__ == '__main__':
    run()
