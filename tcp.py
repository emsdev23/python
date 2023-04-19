import mysql.connector
from datetime import datetime

# Establish a database connection
conn = mysql.connector.connect(
    host="localhost",     
    user="root", 
    password="22@teneT", 
    database="EMS"
)

cursor = conn.cursor()

sql = """
    INSERT INTO EMSUPSbatterycontrol (functioncode, starttime, endtime, capacity)
    VALUES (%s, %s, %s, %s)
"""

functioncode = 212121211
starttime = datetime(2023, 4, 17, 12, 30, 0)
endtime = datetime(2023, 4, 17, 15, 45, 0) 
capacity = 900

cursor.execute(sql, (functioncode, starttime, endtime, capacity))

# Commit the transaction
conn.commit()

# Close the cursor and database connection
cursor.close()
conn.close()