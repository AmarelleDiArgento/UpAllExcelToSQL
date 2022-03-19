import pandas as pd
from pylib.file import extractDataFile
from pylib.pySQL import dbProjCon, stringConnect, insertDataToSql
print('Inicio!')

URL = r'D:\Code\G1-14\files\\'
FILE = 'G1-14.xlsx'

dbProjCon = {
    'server': 'elt-dbproj-fca\\testing',
    'db': 'Reports',
    'user': 'talend',
    'password': 'S3rvT4l3nd*',
}

df = extractDataFile(url=URL, file=FILE, sheet="Hoja1")
strCon = stringConnect(dbProjCon)



insertDataToSql(strCon, 'tmp', 'G1-14', df)
