import mysql.connector
import time

unprocesseddb = mysql.connector.connect(
        host="121.242.232.151",
        user="bmsrouser6",
        password="bmsrouser6@151",
        database='bmsmgmt_olap_prod_v13',
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
    ubuncur = ubuntudb.cursor()

    bmcdbcur = unprocesseddb.cursor()

    bmcdbcur.execute("""select chiller1Energy,chiller2Energy,chiller3Energy,chiller4Energy,polledTime from hvacChillerCoolingPolling where date(polledTime)=curdate()""")

    result = bmcdbcur.fetchall()
    chiller1Energy = []
    chiller2Energy = []
    chiller3Energy = []
    chiller4Energy = []


    # Function to calculate cumulative of the energy and summing it
    def cumulativeC1Energy(energyList,time):
        Energy = []
        for i in range(1,len(energyList)):
            if energyList[i] != None and energyList[i-1] != None:
                Energy.append(energyList[i]-energyList[i-1])
                # print(energyList[i]-energyList[i-1])
        polledTime = str(time)[0:17]+"00"
        sql = """INSERT INTO chillerEnergy(chiller1Energy,polledDate) VALUES(%s,%s)"""
        values = (sum(Energy),polledTime)
        try:
            ubuncur.execute(sql,values)
            ubuntudb.commit()
        except mysql.connector.errors.IntegrityError:
            sql = """UPDATE chillerEnergy SET chiller1Energy = %s where polledDate = %s"""
            values = (sum(Energy),polledTime)
            ubuncur.execute(sql,values)
            ubuntudb.commit()    
        print("Chiller 1 status inserted")
        # print("C1",sum(Energy),polledTime)
        
    def cumulativeC2Energy(energyList,time):
        Energy = []
        for i in range(1,len(energyList)):
            if energyList[i] != None and energyList[i-1] != None:
                Energy.append(energyList[i]-energyList[i-1])
                # print(energyList[i]-energyList[i-1])
        polledTime = str(time)[0:17]+"00"
        sql = """UPDATE chillerEnergy SET chiller2Energy = %s where polledDate = %s"""
        values = (sum(Energy),polledTime)
        ubuncur.execute(sql,values)
        ubuntudb.commit()
        print("Chiller 2 status inserted")
        # print("C2",sum(Energy),polledTime)
        
    def cumulativeC3Energy(energyList,time):
        Energy = []
        for i in range(1,len(energyList)):
            if energyList[i] != None and energyList[i-1] != None:
                Energy.append(energyList[i]-energyList[i-1])
                # print(energyList[i]-energyList[i-1])
        polledTime = str(time)[0:17]+"00"
        sql = """UPDATE chillerEnergy SET chiller3Energy = %s where polledDate = %s"""
        values = (sum(Energy),polledTime)
        ubuncur.execute(sql,values)
        ubuntudb.commit()
        print("Chiller 3 status inserted")
        # print("C3",sum(Energy),polledTime)
        
        
    def cumulativeC4Energy(energyList,time):
        Energy = []
        for i in range(1,len(energyList)):
            if energyList[i] != None and energyList[i-1] != None:
                Energy.append(energyList[i]-energyList[i-1])
                # print(energyList[i]-energyList[i-1])
        polledTime = str(time)[0:17]+"00"
        sql = """UPDATE chillerEnergy SET chiller4Energy = %s where polledDate = %s"""
        values = (sum(Energy),polledTime)
        ubuncur.execute(sql,values)
        ubuntudb.commit()
        print("Chiller 4 status inserted")
        # print("C4",sum(Energy),polledTime)



    for res in result:
        chiller1Energy.append(res[0])
        chiller2Energy.append(res[1])
        chiller3Energy.append(res[2])
        chiller4Energy.append(res[3])
    
        # print(str(res[4])[14:16])
        # Seperating the ciller energy for fifteen minutes interval
        if str(res[4])[14:16] == "00":
            if str(res[4])[11:13] != "00":
                cumulativeC1Energy(chiller1Energy[-15:],res[4])
                cumulativeC2Energy(chiller2Energy[-15:],res[4])
                cumulativeC3Energy(chiller3Energy[-15:],res[4])
                cumulativeC4Energy(chiller4Energy[-15:],res[4])
        
        elif str(res[4])[14:16] == "15":
            cumulativeC1Energy(chiller1Energy[-15:],res[4])
            cumulativeC2Energy(chiller2Energy[-15:],res[4])
            cumulativeC3Energy(chiller3Energy[-15:],res[4])
            cumulativeC4Energy(chiller4Energy[-15:],res[4])
        
        elif str(res[4])[14:16] == "30":
            cumulativeC1Energy(chiller1Energy[-15:],res[4])
            cumulativeC2Energy(chiller2Energy[-15:],res[4])
            cumulativeC3Energy(chiller3Energy[-15:],res[4])
            cumulativeC4Energy(chiller4Energy[-15:],res[4])
            
        elif str(res[4])[14:16] == "45":
            cumulativeC1Energy(chiller1Energy[-15:],res[4])
            cumulativeC2Energy(chiller2Energy[-15:],res[4])
            cumulativeC3Energy(chiller3Energy[-15:],res[4])
            cumulativeC4Energy(chiller4Energy[-15:],res[4])
            
    time.sleep(600)
    