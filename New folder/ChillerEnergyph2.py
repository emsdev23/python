import mysql.connector
from datetime import date
import time

unprocesseddb = mysql.connector.connect(
  host="121.242.232.151",
  user="bmsrouser6",
  password="bmsrouser6@151",
  database='bmsmgmtprodv13',
  port=3306
)

ubuntudb = mysql.connector.connect(
        host="10.9.211.140",
        user="ganesh",
        password="Tenet@123",
        database='EMS',
        port=3306
      )

while True:
    unprocs = unprocesseddb.cursor()

    ubuncur = ubuntudb.cursor()

    try:
        unprocs.execute("""select acmeterenergy,acmetersubsystemid,acmeterpolledtimestamp from bmsmgmtprodv13.acmeterreadings where date(acmeterpolledtimestamp) = curdate();""")
    except mysql.connector.errors.OperationalError as e:
 
        print("Lost connection to MySQL server: {}".format(e))
        
        unprocesseddb.reconnect()
        
        unprocs.execute("""select acmeterenergy,acmetersubsystemid,acmeterpolledtimestamp from bmsmgmtprodv13.acmeterreadings where date(acmeterpolledtimestamp) = curdate();""")

    result = unprocs.fetchall()

    chiller5Energy = []
    chiller6Energy = []
    chiller7Energy = []
    chiller8Energy = []

    def cumulativeC5Energy(energyList,time):
            Energy = 0
            for i in range(1,len(energyList)):
                if energyList[-1] != None and energyList[-2] != None:
                    Energy = energyList[-1]-energyList[-2]
                    # print(energyList[i]-energyList[i-1])
            polledTime = str(time)[0:17]+"00"
            sql = """INSERT INTO chillerEnergyph2(chiller5Energy,polledDate) VALUES(%s,%s)"""
            values = (Energy,polledTime)
            try:
                ubuncur.execute(sql,values)
                ubuntudb.commit()
            except mysql.connector.errors.IntegrityError:
                sql = """UPDATE chillerEnergyph2 SET chiller5Energy = %s where polledDate = %s"""
                values = (Energy,polledTime)
                ubuncur.execute(sql,values)
                ubuntudb.commit()
            except mysql.connector.errors.OperationalError as e:
 
                print("Lost connection to MySQL server: {}".format(e))   
                
                try:
                    ubuncur.execute(sql,values)
                    ubuntudb.commit()
                except mysql.connector.errors.IntegrityError:
                    sql = """UPDATE chillerEnergyph2 SET chiller5Energy = %s where polledDate = %s"""
                    values = (Energy,polledTime)
                    ubuncur.execute(sql,values)
                    ubuntudb.commit()
                 
            print("Chiller 5 status inserted")
            
    def cumulativeC6Energy(energyList,time):
            Energy = 0
            for i in range(1,len(energyList)):
                if energyList[-1] != None and energyList[-2] != None:
                    Energy = energyList[-1]-energyList[-2]
                    # print(energyList[i]-energyList[i-1])
            polledTime = str(time)[0:17]+"00"
            # print(Energy,polledTime)
            sql = """UPDATE chillerEnergyph2 SET chiller6Energy = %s where polledDate = %s"""
            values = (Energy,polledTime)
            ubuncur.execute(sql,values)
            ubuntudb.commit()
            print("Chiller 6 status inserted")
            
    def cumulativeC7Energy(energyList,time):
            Energy = 0
            for i in range(1,len(energyList)):
                if energyList[-1] != None and energyList[-2] != None:
                    Energy = energyList[-1]-energyList[-2]
                    # print(energyList[i]-energyList[i-1])
            polledTime = str(time)[0:17]+"00"
            # print(Energy,polledTime)
            sql = """UPDATE chillerEnergyph2 SET chiller7Energy = %s where polledDate = %s"""
            values = (Energy,polledTime)
            ubuncur.execute(sql,values)
            ubuntudb.commit()
            print("Chiller 7 status inserted")
            
    def cumulativeC8Energy(energyList,time):
            Energy = 0
            for i in range(1,len(energyList)):
                if energyList[-1] != None and energyList[-2] != None:
                    Energy = energyList[-1]-energyList[-2]
                    # print(energyList[i]-energyList[i-1])
            polledTime = str(time)[0:17]+"00"
            # print(Energy,polledTime)
            sql = """UPDATE chillerEnergyph2 SET chiller8Energy = %s where polledDate = %s"""
            values = (Energy,polledTime)
            ubuncur.execute(sql,values)
            ubuntudb.commit()
            print("Chiller 8 status inserted")

    for i in result:
        # print(i[0],i[1])
        if i[1] == 1442:
            chiller5Energy.append(i[0])
            cumulativeC5Energy(chiller5Energy,i[2])
        elif i[1] == 1163:
            chiller6Energy.append(i[0])
            cumulativeC6Energy(chiller6Energy,i[2])
        elif i[1] == 1441:
            chiller7Energy.append(i[0])
            cumulativeC7Energy(chiller7Energy,i[2])
        elif i[1] == 1494:
            chiller8Energy.append(i[0])
            cumulativeC8Energy(chiller8Energy,i[2])
            
    time.sleep(600)
        