import mysql.connector
import time
from datetime import datetime, timedelta

processeddb = mysql.connector.connect(
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
    proscur = processeddb.cursor()

    proscur.execute("""select FLOOR(acmeterenergy),polledTime from bmsmgmt_olap_prod_v13.MVPPolling where mvpnum in ("MVP1","MVP2","MVP3","MVP4") and Date(polledTime) = curdate();""")

    data = proscur.fetchall()

    minute_list = []

    for datum in data:
        minute_list.append(datum)
        
    datetimes = [datetime.strptime(str(item[1]), '%Y-%m-%d %H:%M:%S') for item in minute_list]

    start_time = datetimes[0].replace(second=0, microsecond=0)
    end_time = (datetimes[-1] + timedelta(minutes=1)).replace(second=0, microsecond=0)


    timestamps = []
    curr_time = start_time
    while curr_time < end_time:
        timestamps.append(curr_time)
        curr_time += timedelta(minutes=1)
        
    values_per_minute = {timestamp: 0 for timestamp in timestamps}

    for item in minute_list:
        # print(item)
        value, timestamp_str = item
        timestamp = datetime.strptime(str(timestamp_str), '%Y-%m-%d %H:%M:%S')
        timestamp = timestamp.replace(second=0, microsecond=0)
        if value!=None:
            values_per_minute[timestamp] += value

    output_list = [(timestamp, value) for timestamp, value in values_per_minute.items()]

    value_list = []

    for i in output_list:
        value_list.append(i[1])

    final_list =[]

    for i in range(1,len(value_list)):
        final_list.append(value_list[i]-value_list[i-1])
        
    energy = sum(final_list)/1000
    polledDate = str(output_list[0][0])[0:10]

    # print(polledDate,energy)

    ubuncur = ubuntudb.cursor()

    sql = """INSERT INTO GridProcessed(polledDate,Energy) VALUES(%s,%s);"""
    values = (polledDate,energy)
    try:
        ubuncur.execute(sql,values)
        print("Grid data added")
        ubuntudb.commit()
    except mysql.connector.IntegrityError:
        sql = """UPDATE GridProcessed SET Energy = %s WHERE polledDate= %s;"""
        values = (energy,polledDate)
        ubuncur.execute(sql,values)
        print("Grid data updated")
        ubuntudb.commit()
        
    time.sleep(10)


     
# for i in final_list:
#     print(i)
     
# print(sum(value_list)/1000)
