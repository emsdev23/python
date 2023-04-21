import mysql.connector
from datetime import date

unprocesseddb = mysql.connector.connect(
  host="121.242.232.151",
  user="bmsrouser6",
  password="bmsrouser6@151",
  database='bmsmgmtprodv13',
  port=3306
)

ubuntudb = mysql.connector.connect(
  host="121.242.232.211",
  user="root",
  password="22@teneT",
  database='EMS',
  port=3306
)

ubuncursor = ubuntudb.cursor()

unpdbcursor = unprocesseddb.cursor()

unpdbcursor.execute("""select polledTime,coolingEnergyConsumption from thermalStorageMQTTReadings where Date(polledTime)= '2023-03-14' and tsOutletADPvalveStatus = 1 and tsOutletBDPvalveStatus = 1 and HValve = 1 order by polledTime asc ;""")

result = unpdbcursor.fetchall()


dchgcummlates = []

def avgCoolingEnergy(val):
    for i in val:
        hour  = i
        coolingenergy = sum(val[i]["coolingenergy"])
        nowdate = date.today()
        if len(str(hour))<2:
            recordtime = str(nowdate)+" "+"0"+str(hour)+":00:00"
        else:
            recordtime = str(nowdate)+" "+str(hour)+":00:00"
        sql = """INSERT INTO ThermalStorageProcessed (coolingEnergy,timeInHour,,recordTime) VALUES (%f,%i,%s)"""
        values = (coolingenergy,hour,recordtime)
        ubuncursor.execute(sql,values)
        print("Thermal Processed data written")
        ubuntudb.commit()
        # print(hour,coolingenergy,recordtime)
        


def convertDischarge(val):
    dictDchg = {}
    for i in val:
         time = i['time']
         hour = i['time'].hour
         if hour not in dictDchg:
            dictDchg[hour] = {'coolingenergy': []}
         dictDchg[hour]['coolingenergy'].append(i['coolingnergy'])
    avgCoolingEnergy(dictDchg)
    

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
        convertDischarge(dchgcummlates)
        dchgcummlates.clear()
    else:
        dchgcummlates.append({"coolingnergy":Energy,"time":time})

for data in result:
    polledtime = data[0]
    coolingenergy = data[1]/100
    count = len(result)
    
    cummulativeValue(coolingenergy,polledtime,count)
    
    
