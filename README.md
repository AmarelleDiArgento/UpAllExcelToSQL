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


{
	"db_con": {
		"server": "elt-dbproj-fca\\testing",
		"db": "Reports",
		"user": "talend",
		"password": "S3rvT4l3nd*"
	},
	"files": [
		{
			"dir": "files",
			"excel_file": "G1-14.xlsx",
			"sheet": "Hoja1",
			"schema": "rep",
			"table": "G1-14",
			"truncate": true,
			"index": true
		}
	]
}
