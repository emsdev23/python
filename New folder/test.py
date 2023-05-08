# from datetime import datetime, timedelta

# input_list = [
#     (10262397952.0, '2023-04-25 20:50:32'),
#     (7835080704.0, '2023-04-25 20:50:32'),
#     (10262404096.0, '2023-04-25 20:51:32'),
#     (7835088384.0, '2023-04-25 20:51:32'),
#     (10262410240.0, '2023-04-25 20:52:32'),
#     (7835096576.0, '2023-04-25 20:52:32')
# ]

# datetimes = [datetime.strptime(item[1], '%Y-%m-%d %H:%M:%S') for item in input_list]

# # Determine the start and end times of the time range
# start_time = datetimes[0].replace(second=0, microsecond=0)
# end_time = (datetimes[-1] + timedelta(minutes=1)).replace(second=0, microsecond=0)

# # Create a list of timestamps, one for each minute in the time range
# timestamps = []
# curr_time = start_time
# while curr_time < end_time:
#     timestamps.append(curr_time)
#     curr_time += timedelta(minutes=1)

# # Initialize a dictionary to store the aggregated values per minute
# values_per_minute = {timestamp: 0 for timestamp in timestamps}

# # Iterate over the input list and aggregate the values per minute
# for item in input_list:
#     value, timestamp_str = item
#     timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
#     timestamp = timestamp.replace(second=0, microsecond=0)
#     values_per_minute[timestamp] += value

# # Convert the dictionary to a list of (timestamp, value) tuples
# output_list = [(timestamp, value) for timestamp, value in values_per_minute.items()]



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

unprocs = unprocesseddb.cursor()

unprocs.execute("""select acmeterenergy,acmetersubsystemid,acmeterpolledtimestamp from bmsmgmtprodv13.acmeterreadings where date(acmeterpolledtimestamp) = curdate();""")

result = unprocs.fetchall()

chiller5Energy = []
chiller6Energy = []
chiller7Energy = []
chiller8Energy = []

def cumulativeC5Energy(energyList,time):
        Energy = []
        for i in range(1,len(energyList)):
            if energyList[-1] != None and energyList[-2] != None:
                Energy = energyList[-1]-energyList[-2]
                # print(energyList[i]-energyList[i-1])
        polledTime = str(time)[0:17]+"00"
        
        print()
        
        sql = """INSERT INTO chillerEnergy(chiller5Energy,polledDate) VALUES(%s,%s)"""
        values = (sum(Energy),polledTime)
        # try:
        #     ubuncur.execute(sql,values)
        #     ubuntudb.commit()
        # except mysql.connector.errors.IntegrityError:
        #     sql = """UPDATE chillerEnergy SET chiller1Energy = %s where polledDate = %s"""
        #     values = (sum(Energy),polledTime)
        #     ubuncur.execute(sql,values)
        #     ubuntudb.commit()    
        # print("Chiller 1 status inserted")
polledTime = ""
for i in result:
    # print(i[0],i[1])
    if i[1] == 1442:
        chiller5Energy.append(i[0])
        cumulativeC5Energy(chiller5Energy,i[2])
    elif i[1] == 1163:
        chiller6Energy.append(i[0])
    elif i[1] == 1441:
        chiller7Energy.append(i[0])
    elif i[1] == 1494:
        chiller8Energy.append(i[0])
