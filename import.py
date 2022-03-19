import pandas as pd
from pylib.file import extractDataFile

from pylib.mod.utils import parameters, workDirectory
from pylib.pySQL import insertDataToSql, stringConnect

ROOT = workDirectory()

ROOT = workDirectory()
DB_CON, FILES = parameters()

strCon = stringConnect(DB_CON)

for file in FILES:
    url = r'{}\{}\\'.format(ROOT, file['dir'])
    print(url)
    df = extractDataFile(
        url=url,
        file=file['excel_file'],
        sheet=file['sheet']
    )
    insertDataToSql(
        srtCon=strCon,
        schema=file['schema'],
        table=file['table'],
        data=df
    )
