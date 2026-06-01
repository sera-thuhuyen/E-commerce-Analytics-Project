import pyodbc

conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=45.124.94.158,1433;"
    "DATABASE=xomdata_dataset;"
    "UID=thuhuyenftu2;"
    "PWD=Jn$O7jv@nYhQrB;"
    "TrustServerCertificate=yes;"
)
cur = conn.cursor()
cur.execute("""SELECT TABLE_SCHEMA, TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA ='web_analytics'
ORDER BY TABLE_SCHEMA, TABLE_NAME;""")
for row in cur: print(row)