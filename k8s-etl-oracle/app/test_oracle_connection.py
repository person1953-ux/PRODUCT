import oracledb

conn = oracledb.connect(
    user="HR",
    password="hr",
    host="localhost",
    port=1521,
    service_name="XE"
)

cursor = conn.cursor()
cursor.execute("SELECT SYSDATE FROM dual")
print("Connected successfully. SYSDATE =", cursor.fetchone()[0])

cursor.close()
conn.close()
