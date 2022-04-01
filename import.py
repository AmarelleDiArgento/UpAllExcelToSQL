
from pylib.file import blockExtractDataFile, convertXlsToCsv, createDirectory, extractDataFile, removeDirectory, removeFile, searchFilesByContentInTitle
from pylib.mod.error import packageForFileError
import re

from pylib.mod.utils import excutionTime, parameters, workDirectory
from pylib.pySQL import insertDataToSql_Alchemy, stringConnect

ROOT = workDirectory()
DB_CON, FILES, ISTEST = parameters()


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


@excutionTime
def run():
    try:
        createDirectory(
            workDirectory() + chr(92)+'Procesado' + chr(92)
        )

        for file in FILES:

            url = create_url(file)

            # remove_csv(file_path)
            if file['directory']:
                files = searchFilesByContentInTitle(
                    file_path=url,
                    parm=dict(file['regex']))

                df = blockExtractDataFile(
                    path=url,
                    files=files,
                    sheets=file['sheets'])

            else:

                file_path = convertXlsToCsv(
                    url=url,
                    file=file['file'],
                    sheets=file['sheets'],
                    isTest=ISTEST
                )
                df = extractDataFile(
                    file_path=file_path,
                    file=file['file'],
                    sheets=file['sheets']
                )

            insertDataToSql_Alchemy(
                strCon=strCon,
                schema=file['schema'],
                table=file['table'],
                data=df,
                truncate=file['truncate'],
                index=file['index']
            )

    except (AttributeError, KeyError) as e:
        packageForFileError(
            file_path,
            "Error: {}.\n".format(e),
            url
        )




if __name__ == '__main__':
    run()
