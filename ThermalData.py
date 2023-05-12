import mysql.connector
from datetime import date
import time

while True:
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

    ubuncursor = ubuntudb.cursor()

    unpdbcursor = unprocesseddb.cursor()
    try:
        unpdbcursor.execute("""select polledTime,coolingEnergyConsumption from thermalStorageMQTTReadings where Date(polledTime)= curdate() and tsOutletADPvalveStatus = 1 and tsOutletBDPvalveStatus = 1 and HValve = 1 order by polledTime asc ;""")
    except mysql.connector.errors.OperationalError as e:
        print("Lost connection to MySQL server: {}".format(e))
        
        continue

    result = unpdbcursor.fetchall()


    dchgcummlates = []

    def avgCoolingEnergy(val,polledDate):
        for i in val:
            hour  = i
            coolingenergy = sum(val[i]["coolingenergy"])
            if len(str(hour))<2:
                recordtime = polledDate+" "+"0"+str(hour)+":00:00"
            else:
                recordtime = polledDate+" "+str(hour)+":00:00"
            sql = """INSERT INTO ThermalStorageProcessed (coolingEnergy,timeInHour,recordTime) VALUES (%s,%s,%s)"""
            values = (coolingenergy,hour,recordtime)
            try:
                ubuncursor.execute(sql,values)
                print("Thermal Processed data written")
                ubuntudb.commit()
            except mysql.connector.IntegrityError:
                sql = """ UPDATE ThermalStorageProcessed SET coolingEnergy = %s where recordTime = %s """
                values = (coolingenergy,recordtime)
                ubuncursor.execute(sql,values)
                ubuntudb.commit()
                print("Thermal storage data updated")
                
            except mysql.connector.errors.OperationalError as e:
                print("Lost connection to MySQL server: {}".format(e))
                continue	
                
            print(hour,coolingenergy,recordtime)
            


    def convertDischarge(val,polledDate):
        dictDchg = {}
        for i in val:
            time = i['time']
            hour = i['time'].hour
            if hour not in dictDchg:
                dictDchg[hour] = {'coolingenergy': []}
            dictDchg[hour]['coolingenergy'].append(i['coolingnergy'])
            polledDate = str(polledDate)[0:11]
        avgCoolingEnergy(dictDchg,polledDate)
        

    dischargelist = []
    def cummulativeValue(energy,time,count):
        try:
            Energy = dischargelist[-1] - energy
        except IndexError:
            dischargelist.append(energy)
            Energy = 0
        dischargelist.append(energy)
        #print(len(dchgcummlates),count-1)
        if len(dchgcummlates) == count-1:
            convertDischarge(dchgcummlates,time)
            dchgcummlates.clear()
        else:
            dchgcummlates.append({"coolingnergy":Energy,"time":time})

    for data in result:
        polledtime = data[0]
        coolingenergy = data[1]/100
        count = len(result)
        #print(coolingenergy)
        cummulativeValue(coolingenergy,polledtime,count)
        
        
    time.sleep(300)
    
    
