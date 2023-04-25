import mysql.connector
# from datetime import date
import time

unprocesseddb = mysql.connector.connect(
  host="121.242.232.151",
  user="bmsrouser6",
  password="bmsrouser6@151",
  database='bmsmgmtprodv13',
  port=3306
)

# ubuntudb = mysql.connector.connect(
#   host="121.242.232.211",
#   user="root",
#   password="22@teneT",
#   database='EMS',
#   port=3306
# )

# ubuncursor = ubuntudb.cursor()

while True:
  unpdbcursor = unprocesseddb.cursor()

  unpdbcur = unprocesseddb.cursor()

  unpdbcursor.execute("""select FLOOR(acmeterenergy) from bmsmgmtprodv13.acmeterreadings where acmetersubsystemid = 1035 and Date(acmeterpolledtimestamp) = curdate() """)

  result1 = unpdbcursor.fetchall()

  print(result1)

  temp1 = []
  for i in range(1,len(result1)):
        temp1.append(abs(result1[i][0]-result1[i-1][0]))
  
  # print(temp1)

  energy1 = sum(temp1)
    
  unpdbcur.execute("""select FLOOR(acmeterenergy),acmetersubsystemid,acmeterpolledtimestamp from bmsmgmtprodv13.acmeterreadings where acmetersubsystemid = 1147 and Date(acmeterpolledtimestamp) = curdate() """)

  result2 = unpdbcur.fetchall()


  temp2 =[]
  for i in range(1,len(result2)):
        temp2.append(abs(result2[i][0]-result2[i-1][0]))
        
  energy2 = sum(temp2)
  
  #print(energy1/1000)
  #print(energy2/1000)
  
  print((energy1+energy2)/1000)
  
  time.sleep(20)
