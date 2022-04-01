# UpAllExcelToSQL
Subir archivos contenido de archivos de Excel creando la tabla si no existe

# instalar paquetes con:
$ py -m pip install -r requirements.txt 

# actualizar pip:
$ py -m pip install --upgrade pip

# config.json

{
	"db_con": {
		"server": ,
		"db": ,
		"user": ,
		"password": 
	},
	"files": [
		{
			"dir": ,
			"excel_file": ,
			"sheet": ,
			"schema": ,
			"table": ,
			"truncate": ,
			"index": 
		}
	]
}


