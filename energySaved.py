import mysql.connector
import time
from datetime import datetime, timedelta

while True:
        processeddb = mysql.connector.connect(
        host="121.242.232.151",
        user="bmsrouser6",
        password="bmsrouser6@151",
        database='bmsmgmt_olap_prod_v13',
        port=3306
        )

        unprocesseddb = mysql.connector.connect(
        host="121.242.232.151",
        user="bmsrouser6",
        password="bmsrouser6@151",
        database='bmsmgmtprodv13',
        port=3306
      )

        ubuntudb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="22@teneT",
        database='EMS',
        port=3306
      )
        proscur = processeddb.cursor()
        
        unpdbcursor1 = unprocesseddb.cursor()

        unpdbcursor2 = unprocesseddb.cursor()

        ubuncur1 = ubuntudb.cursor()

        ubuncur2 = ubuntudb.cursor()

        ubuncur3 = ubuntudb.cursor()
        
        ubunwrite = ubuntudb.cursor()
        
        ubuncur1.execute("""select upsdischargingenergy,received_time from EMSUPSbattery where date(received_time)=curdate() and upsbatterystatus = 'DCHG'""")
        
        result = ubuncur1.fetchall()

        finallis = []

        def cumulativeBat(lis):
                for i in range(1,len(lis)):
                        finallis.append(lis[i]-lis[i-1])

        lith = []
        for res in result:
        # print(int(str(res[1])[11:13]))
                if int(str(res[1])[11:13])>=6 and int(str(res[1])[11:13])<10:
                        lith.append(res[0])
                elif int(str(res[1])[11:13])>=18 and int(str(res[1])[11:13])<22:
                        lith.append(res[0])
        cumulativeBat(lith)

        liout = int(sum(finallis))

        # print("Li Energy :",liout)


        ubuncur2.execute("""select coolingEnergy,timeInHour from ThermalStorageProcessed where DATE(recordTime) = '2023-05-03'""") 

        result = ubuncur2.fetchall()

        thrml = []

        for i in result:
                # print(i[1])
                if i[1]>=6 and i[1]<10:
                        thrml.append(print(i[0]))
                elif i[1]>=18 and i[1]<22:
                        thrml.append(print(i[0]))
        thermal = sum(thrml)
        # print("Thermal Energy :",thermal)

        ubuncur3.execute("""select metertimestamp,meterenergy from EMS.EMSMeterData where date(metertimestamp) = curdate(); """)

        metres = ubuncur3.fetchall()


        finalmet =[]

        def cumulativeMeter(metlis):
                for i in range(1,len(metlis)):
                        finalmet.append(metlis[i]-metlis[i-1])

        metlis =[]
        for i in metres:
                if int(str(i[0])[11:13]) >= 6 and int(str(i[0])[11:13]) < 10:
                        metlis.append(i[1])

        cumulativeMeter(metlis)
        meterout = int(sum(finalmet)*1000)
        # print("Wheeledin :",meterout)

        unpdbcursor1.execute("""select FLOOR(acmeterenergy),acmeterpolledtimestamp from bmsmgmtprodv13.acmeterreadings where acmetersubsystemid = 1035 and Date(acmeterpolledtimestamp) = curdate()""")

        resroof1 = unpdbcursor1.fetchall()
        roofp1 = []
        def cumulativerRoof1(metlis):
                for i in range(1,len(metlis)):
                        roofp1.append(metlis[i]-metlis[i-1])
                        
        roofp2 = []
        def cumulativerRoof2(metlis):
                for i in range(1,len(metlis)):
                        roofp2.append(metlis[i]-metlis[i-1])


        roof1 =[]
        for res in resroof1:
                if int(str(res[1])[11:13])>=6 and int(str(res[1])[11:13])<10:
                        roof1.append(res[0])
                elif int(str(res[1])[11:13])>=18 and int(str(res[1])[11:13])<22:
                        roof1.append(res[0])

        cumulativerRoof1(roof1)

        unpdbcursor2.execute("""select FLOOR(acmeterenergy),acmeterpolledtimestamp from bmsmgmtprodv13.acmeterreadings where acmetersubsystemid = 1147 and Date(acmeterpolledtimestamp) = curdate() """)

        resroof2 = unpdbcursor2.fetchall()
        roof2 =[]
        for res in resroof2:
                if int(str(res[1])[11:13])>=6 and int(str(res[1])[11:13])<10:
                        roof2.append(res[0])
                elif int(str(res[1])[11:13])>=18 and int(str(res[1])[11:13])<22:
                        roof2.append(res[0])
        cumulativerRoof2(roof2)

        roofout = int((sum(roofp1)+sum(roofp2))/1000)
        # print("Solar :",roofout+meterout)
        
        total = liout+roofout+meterout+thermal
        
        ubuncur1.execute("""select date(now())""")
        
        polled = ubuncur1.fetchall()
        polledDate = polled[0][0]
        
        sql ="""INSERT INTO energySaved(polledTime,Energysaved) Values(%s,%s)"""
        values = (polledDate,total)
        
        try:
                ubunwrite.execute(sql,values)
                ubuntudb.commit()
                print("Data inserted")
        except mysql.connector.errors.IntegrityError:
                sql = """UPDATE energySaved SET Energysaved=%s where polledTime=%s"""
                values = (total,polledDate)
                ubunwrite.execute(sql,values)
                ubuntudb.commit()
                print("Data updated")
                
        
        print("Total : ",liout+roofout+meterout+thermal)
        
        time.sleep(180)
