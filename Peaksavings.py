import mysql.connector
import time
from datetime import datetime, timedelta

ubuntudb = mysql.connector.connect(
  host="10.9.211.140",
  user="ganesh",
  password="Tenet@123",
  database='EMS',
  port=3306
)

while True:
    ubuncursor = ubuntudb.cursor()
    
    ubuncursor.execute("""SELECT upschargingenergy,received_time FROM EMSUPSbattery WHERE Date(received_time) = curdate() and upsbatterystatus = 'CHG'""")
    
    chgresult = ubuncursor.fetchall()
    
    for i in range(len(chgresult)):
        timeperiod = str(chgresult[i][1])[11:13]
        
        # print(int(timeperiod))
        if int(timeperiod)>=6 and int(timeperiod)<=10:
            print("Charge :",chgresult[i][0],chgresult[i][1])
        
        elif int(timeperiod)>=0 and int(timeperiod)<=6:
            print("Charge :",chgresult[i][0],chgresult[i][1])
            
        elif int(timeperiod)>=10 and int(timeperiod)<=23:
            print("Charge :",chgresult[i][0],chgresult[i][1])    
            
    ubuncursor.execute("""SELECT upschargingenergy,received_time FROM EMSUPSbattery WHERE Date(received_time) = curdate() and upsbatterystatus = 'DCHG'""")
    
    dchgresult = ubuncursor.fetchall()
    
    for i in range(len(dchgresult)):
        timeperiod = str(dchgresult[i][1])[11:13]
        
        # print(int(timeperiod))
        if int(timeperiod)>=6 and int(timeperiod)<=10:
            print("Discharge :",dchgresult[i][0],dchgresult[i][1])
        
        elif int(timeperiod)>=0 and int(timeperiod)<=6:
            print("Discharge :",dchgresult[i][0],dchgresult[i][1])
            
        elif int(timeperiod)>=10 and int(timeperiod)<=23:
            print("Discharge :",dchgresult[i][0],dchgresult[i][1])
    
    break
        
        