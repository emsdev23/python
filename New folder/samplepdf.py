from reportlab.pdfgen import canvas
from io import BytesIO
from svglib.svglib import svg2rlg
from reportlab.graphics import renderPDF
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfbase import pdfmetrics
from reportlab.lib import colors
import matplotlib.pyplot as plt
import mysql.connector

processeddb = mysql.connector.connect(
        host="121.242.232.151",
        user="bmsrouser6",
        password="bmsrouser6@151",
        database='bmsmgmtprodv13',
        port=3306
        )

fileName = 'sample.pdf'
documentTitle = 'sample'
title = 'Technology'
subTitle = 'The largest thing now!!'
textLines = [
    'Technology makes us aware of the world around us.',
]

pdf = canvas.Canvas(fileName)

# setting the title of the document
pdf.setTitle(documentTitle)

pdf.setFont("Courier-Bold", 24)

pdf.drawCentredString(300, 770, title)

pdf.setFont("Courier-Bold", 20)

pdf.drawCentredString(290, 720, subTitle)

pdf.setFont("Courier-Bold", 14)
text = pdf.beginText(40, 680)
for line in textLines:
    text.textLine(line)
     
pdf.drawText(text)

dbcur = processeddb.cursor()

dbcur.execute("select acmeterenergy,acmeterpolledtimestamp from acmeterreadings where date(acmeterpolledtimestamp) = '2023-05-04' and acmetersubsystemid = 1147;")

result = dbcur.fetchall()

Energy =[]
Times = []
for i in range(1,len(result)):
    Energy.append(result[i][0] - result[i-1][0])
    Times.append(str(result[i][1])[12:])


fig = plt.figure(figsize=(4, 3))
plt.plot(Times,Energy)
plt.xlabel("Time")
plt.ylabel("Energy")
plt.xticks([Times[0],Times[len(Times)//2],Times[-1]])


print(Times[0],Times[len(Times)//2],Times[-1])

imgdata = BytesIO()
fig.savefig(imgdata, format='svg')
imgdata.seek(0)  # rewind the data

drawing=svg2rlg(imgdata)

renderPDF.draw(drawing,pdf, 100, 350 )

pdf.save()

print("File saved")


