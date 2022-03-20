from itertools import count
from numpy import where
import pandas as pd
from pylib.file import extractDataFile

from pylib.mod.utils import parameters, workDirectory
from pylib.pySQL import deleteDataToSql, insertDataToSql, stringConnect, truncateTable

ROOT = workDirectory()
DB_CON, FILES = parameters()

strCon = stringConnect(DB_CON)


def run():

    for file in FILES:
        url = r'{}\{}\\'.format(ROOT, file['dir'])
        print(url)
        df = extractDataFile(
            url=url,
            file=file['excel_file'],
            sheet=file['sheet']
        )

        insertDataToSql(
            strCon=strCon,
            schema=file['schema'],
            table=file['table'],
            data=df,
            truncate=file['truncate'],
            index=file['index']
        )


if __name__ == '__main__':
    run()
