import mysql.connector
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Conexión a la base de datos

db = mysql.connector.connect(
    host = "localhost",
    user = "root",
    password = "password",
    database="mopc"
)

cursor = db.cursor()

# Accesso al worksheet

scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

creds = ServiceAccountCredentials.from_json_keyfile_name('./credenciales.json', scope)

client = gspread.authorize(creds)
spreadsheet = client.open_by_key('1Q9sPgQ7pRrYWJn4FvIM2dBCU_t0qbQTlY2sEhdpkWjs') 
wks = spreadsheet.worksheet('Datos')

# Creación de la vista

try:
    cursor.execute("""
    CREATE OR REPLACE VIEW 
    datos_cliente AS 
    SELECT 
        periodos_tableros.IDCLIENTE, 
        periodos_tableros.PERIODO,
        SUM(pagos_unificados.MONTO) AS MONTO
    FROM 
        periodos_tableros    
    INNER JOIN 
        pagos_unificados
    GROUP BY IDCLIENTE, PERIODO
    """)

except:
    print("Error al crear la vista")

# Copiar los datos de la vista a la hoja de cálculo

try: 

    cursor.execute("SELECT * FROM datos_cliente")
    data = cursor.fetchall()

    df = pd.DataFrame(data, columns=[col[0] for col in cursor.description])

    wks.update([df.columns.values.tolist()] + df.values.tolist())

except:
    print("Error al obtener los datos de la vista")





