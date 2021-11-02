from sys import exit
import sshtunnel
from sshtunnel import SSHTunnelForwarder
import numpy as np
import mysql.connector
import MySQLdb as db
import pandas as pd
import xlrd
import os
import csv
from datetime import datetime
from datetime import date
import time
import shutil
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

fteFactor = float(40)

sendto = 'coolname@someemail.com'
txt = ''
subject = 'Ajera Data Loader Error'

# notify someone if process has failed.
def send_email(sendto,txt,subject):
    user = 'coolname@someemail.com' # sends from
    password = 'CantGuessIT'
    smtpsrv = 'smtp.office365.com'
    smtpserver = smtplib.SMTP(smtpsrv,587)
    smtpserver.ehlo()
    smtpserver.starttls()
    smtpserver.ehlo
    smtpserver.login(user, password)
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = user
    msg['To'] = sendto
    text = txt
    html = """\
        <html>
          <head></head>
          <body>{}</body>
        </html>
        """.format(txt)
    part1 = MIMEText(text, 'plain')
    part2 = MIMEText(html, 'html')
    msg.attach(part1)
    msg.attach(part2)
    smtpserver.sendmail(user, sendto, msg.as_string())
    smtpserver.close()

# ssh variables
ssh_host = 'xxx.xxx.xxx.xxx'
localhost = '127.0.0.1'
ssh_username = 'forge'
ssh_password = 'passphrase'
ssh_pkey = r'path/to/your/ssh_key'
# database variables
sql_username='forge'
sql_password='superstong'
db_name='databasename'

# get current working directory
curDir = r'path/to/directory' # you can use os.getcwd()

# select the excel file (needs to be the only one in there)
ajeraExport = ''
ajeraArchive = ''
for xlsx in os.listdir(curDir):
    if xlsx.endswith('xlsx'):
        ajeraExport = os.path.join(curDir, xlsx)
        ajeraArchive = os.path.join(os.path.dirname(curDir),'archive', str(datetime.today().date())+"-AjeraExport.xlsx")

if os.path.isfile(ajeraExport):
    pass
else:
    txt = 'Ajera export excel file was not found'
    send_email(sendto,txt,subject)
    exit()

def cleanstring(string):
    val = string
    if val.startswith("b'"):
        val = val.replace("b'", '')
    if val.startswith('b"'):
        val = val.replace('b"', '')
    if val.endswith("'"):
        val = val[:-1]
    if val.endswith('"'):
        val = val[:-1]
    val='"'+val.strip()+'"'
    return val

# function to calculate and add FTE values
def calcFTES(startDate, endDate, hoursRemaining):
    try:
        FTE = 0.0
        todayDate = datetime.today().date() # get todays date
        if hoursRemaining == '': # populate all negative hoursRemaining with 0
            hoursRemaining = 0.0
        if endDate == '': # endDate is null
            return FTE
        if startDate == '': # startDate is null
            startDate = todayDate
        else:
            startDate = datetime(*xlrd.xldate_as_tuple(startDate, 0)) # convert excel date number tuple
            startDate = datetime.strptime(str(startDate), '%Y-%m-%d  %H:%M:%S') # convert tuple to datetime string
            startDate = startDate.strftime('%m/%d/%Y') # reformat datetime string to proper format
            startDate = datetime.strptime(startDate, '%m/%d/%Y').date() # convert datetime string to datetime object
        endDate = datetime(*xlrd.xldate_as_tuple(endDate, 0)) # convert excel date number tuple
        endDate = datetime.strptime(str(endDate), '%Y-%m-%d  %H:%M:%S') # convert tuple to datetime string
        endDate = endDate.strftime('%m/%d/%Y') # reformat datetime string to proper format
        endDate = datetime.strptime(endDate, '%m/%d/%Y').date() # convert datetime string to datetime object
        if endDate < todayDate: # endDate has already passed
            return FTE
        if startDate<todayDate: # startDate has alredy passed. change startDate to todaysDate
            startDate = todayDate
        # initialize FTE variables
        u = float(1)/fteFactor # calculate utilization
        d = (endDate-startDate).days # calculate total days left in project
        if d <= 0:
            return FTE
        h = hoursRemaining # get hoursRemaining
        w = float(7*h) # get total weeks

        FTE = (w/d)*u # calculate fte

        if FTE>0: # exclude all negative FTE values
            return round(FTE, 4)
        else:
            FTE = 0
            return FTE

    except Exception as e:
        txt = e
        send_email(sendto,txt,subject)
        exit()

# format the excel file to be a csv
def csv_from_excel():
    try:
        wb = xlrd.open_workbook(ajeraExport)
        sh = wb.sheet_by_index(0)
        csv_file = open(os.path.join(curDir, 'ajeraData.csv'), 'w')
        wr = csv.writer(csv_file, quoting=csv.QUOTE_ALL, lineterminator = '\n')
        rownum = 1 # clean the csv file removing the header col
        colOrder = [9,	10,	0,	1,	2,	3,	4,	5,	6,	7,	8,	11,	12,	13,	14,	15,	16,	17,	18,	19,	20,	21,	22,	23,	24,	25,	26,	27,	28]

        while rownum < sh.nrows:
            FTE = 0.0
            rowVals = []
            for col in colOrder:
                val = sh.cell(rownum, col).value
                if isinstance(val, str):
                    val = str(val.encode('utf-8'))
                rowVals.append(val)
            startDate = sh.cell(rownum, 17).value
            endDate = sh.cell(rownum, 18).value
            hoursRemaining = sh.cell(rownum, 21).value

            try:
                FTE = calcFTES(startDate, endDate, hoursRemaining)
            except Exception as e:
                print (e)

            rowVals.append(FTE)

            wr.writerow(rowVals)
            rownum+=1
        csv_file.close()
    except Exception as e:
        txt = str(e)
        send_email(sendto,txt,subject)
        exit()

csv_from_excel()

drop = 'DROP TABLE IF EXISTS `project_phases`;'
make = """
    CREATE TABLE `project_phases` (
        `id` int(11) NOT NULL AUTO_INCREMENT,
        `phase_order` int(11) DEFAULT NULL,
        `Ajera_Project_Key` int(11) DEFAULT NULL,
        `project_id` int(11) DEFAULT NULL,
        `Ajera_Project_ID` text,
        `Project_Description` text,
        `Ajera_Client_Key` int(11) DEFAULT NULL,
        `Client` text,
        `Ajera_PM_Key` int(11) DEFAULT NULL,
        `Project_Manager` text,
        `Ajera_PIC_Key` int(11) DEFAULT NULL,
        `Principal_In_Charge` text,
        `title` text,
        `Ajera_Dept_Key` int(11) DEFAULT NULL,
        `Project_Status` text,
        `Phase_Status` text,
        `Department` text,
        `Project_Type` text,
        `start` text,
        `end` text,
        `hours_budgeted` int(11) DEFAULT NULL,
        `Hours_Worked` decimal(20,2) DEFAULT NULL,
        `Hours_Remaining` decimal(20,2) DEFAULT NULL,
        `Total_Contract_Amount` decimal(20,2) DEFAULT NULL,
        `Billed` decimal(20,2) DEFAULT NULL,
        `Billed_Labor` decimal(20,2) DEFAULT NULL,
        `Billed_Hours` decimal(20,2) DEFAULT NULL,
        `WIP` decimal(20,2) DEFAULT NULL,
        `Spent` decimal(20,2) DEFAULT NULL,
        `Spent_Labor` decimal(20,2) DEFAULT NULL,
        `FTEs` decimal(20,4) DEFAULT NULL,
        PRIMARY KEY (`id`),
        UNIQUE KEY `id_UNIQUE` (`id`)
        ) ENGINE=InnoDB AUTO_INCREMENT=85801 DEFAULT CHARSET=utf8;
"""
# load csv file into database
CSV = os.path.join(curDir, 'ajeraData.csv')

print ('Logging into the server')
# ------------------------------------------------------------------------
server = SSHTunnelForwarder(
    (ssh_host, 22),
    ssh_username=ssh_username,
    ssh_password=ssh_password,
    ssh_private_key=ssh_pkey,
    remote_bind_address=('localhost', 3306)
)
server.start()

mydb = db.connect(
    port=server.local_bind_port,
    user=sql_username,
    passwd=sql_password,
    database=db_name
    )

print ('Connected to database')
mycursor = mydb.cursor()
print ('Cursor Created')
mycursor.execute(drop) # drop phases table
print ('Table dropped')
mycursor.execute(make) # create new phases table
print ('Table added')
print('Loading new data')
# ------------------------------------------------------------------------

with open (CSV, 'r') as f:
    reader = csv.reader(f)
    data = next(reader)
    #load new data
    for newrow in reader:
        row = []
        for val in newrow:
            try:
                val = float(val)
            except:
                val=cleanstring(val)

            row.append(val)

        id = row[0]
        phase_order = row[1]
        Ajera_Project_Key = row[2]
        project_id=0
        Ajera_Project_ID = str(row[3])
        if '.0' in Ajera_Project_ID:
            Ajera_Project_ID = Ajera_Project_ID.replace(".0", "")
        Project_Description = row[4]
        Ajera_Client_Key = row[5]
        Client = row[6]
        Ajera_PM_Key = row[7]
        Project_Manager = row[8]
        Ajera_PIC_Key = row[9]
        Principal_In_Charge = row[10]
        title = row[11]
        Ajera_Dept_Key = row[12]
        Project_Status = row[13]
        Phase_Status = row[14]
        Department = row[15]
        Project_Type = row[16]
        start = row[17]
        if start != "":
            try:
                start = int(start)
                start = datetime(*xlrd.xldate_as_tuple(start, 0)) # convert excel date number tuple
                start = datetime.strptime(str(start), '%Y-%m-%d  %H:%M:%S') # convert tuple to datetime string
                start = str('"'+start.strftime('%m/%d/%Y')+'"') # reformat datetime string to proper format
            except:
                start = start
        end = row[18]
        if end != "":
            try:
                end = int(end)
                end = datetime(*xlrd.xldate_as_tuple(end, 0)) # convert excel date number tuple
                end = datetime.strptime(str(end), '%Y-%m-%d  %H:%M:%S') # convert tuple to datetime string
                end = str('"'+end.strftime('%m/%d/%Y')+'"') # reformat datetime string to proper format
            except:
                end = end
        hours_budgeted = row[19]
        Hours_Worked = row[20]
        Hours_Remaining = row[21]
        Total_Contract_Amount = row[22]
        Billed = row[23]
        Billed_Labor = row[24]
        Billed_Hours = row[25]
        WIP = row[26]
        Spent = row[27]
        Spent_Labor = row[28]
        FTEs = row[29]
        if FTEs == "":
            FTEs = 0.0

        q = 'INSERT INTO project_phases VALUES ({0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12},{13},{14},{15},{16},{17},{18},{19},{20},{21},{22},{23},{24},{25},{26},{27},{28},{29},{30});'.format(id,phase_order,Ajera_Project_Key,project_id,Ajera_Project_ID, Project_Description,Ajera_Client_Key,Client,Ajera_PM_Key,Project_Manager,Ajera_PIC_Key,Principal_In_Charge,title,Ajera_Dept_Key,Project_Status,Phase_Status,Department,Project_Type,start,end,hours_budgeted,Hours_Worked,Hours_Remaining,Total_Contract_Amount,Billed,Billed_Labor,Billed_Hours,WIP,Spent,Spent_Labor,FTEs)
  
        try:
            mycursor.execute(q)
        except Exception as e:
            txt = (q+" "+str(e))
            send_email(sendto,txt,subject)
            exit()

    mydb.commit()
    print('Phase Data Loaded')

    clean = "DELETE FROM project_phases WHERE Ajera_Project_ID = 0 AND Principal_In_Charge LIKE 'No Principal In Charge' AND Project_Manager LIKE 'No Project Manager';"
    pto = "insert into project_phases(phase_order, Ajera_Project_Key, project_id, Ajera_Project_ID, Project_Description, Ajera_Client_Key, Client, Ajera_PM_Key, Project_Manager,Ajera_PIC_Key, Principal_In_Charge, title, Ajera_Dept_Key, Project_Status, Phase_Status, Department, Project_Type, start, end, hours_budgeted, Hours_Worked, Hours_Remaining, Total_Contract_Amount, Billed, Billed_Labor, Billed_Hours, WIP, Spent, Spent_Labor, FTEs) values(0, 1, 000, 000, 'Vacation', -1, NULL, 0, 'No Project Manager', 0, 'No Principal In Charge', 'Vacation', 0, 'Active', 'Active', 'company-company', NULL, NULL, NULL, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0), (0, 1, 000, 000, 'Overhead', -1, NULL, 0, 'No Project Manager', 0, 'No Principal In Charge', 'Overhead',0, 'Active', 'Active', 'company-company', NULL, NULL, NULL, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0), (0, 1, 000, 000, 'Proposal', -1, NULL, 0, 'No Project Manager', 0, 'No Principal In Charge', 'Proposal',0, 'Active', 'Active', 'company-company', NULL, NULL, NULL, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);"
    vaca = "UPDATE schedule SET phase_id = (SELECT id FROM project_phases WHERE Ajera_Project_Key=1 AND Project_Description='Vacation') WHERE project_key=1 AND project_phase='Vacation';"
    over = "UPDATE schedule SET phase_id = (SELECT id FROM project_phases WHERE Ajera_Project_Key=1 AND Project_Description='Overhead') WHERE project_key=1 AND project_phase='Overhead';"
    prop = "UPDATE schedule SET phase_id = (SELECT id FROM project_phases WHERE Ajera_Project_Key=1 AND Project_Description='Proposal') WHERE project_key=1 AND project_phase='Proposal';"
    
    try:
        mycursor.execute(clean)
    except Exception as e:
        txt = (q+" "+str(e))
        send_email(sendto,txt,subject)
        exit()
    mydb.commit()
    print('Phase Data Cleaned')

    try:
        mycursor.execute(pto)
    except Exception as e:
        txt = (q+" "+str(e))
        send_email(sendto,txt,subject)
        exit()
    mydb.commit()
    print('Additional Data Added')

    try:
        mycursor.execute(vaca)
    except Exception as e:
        txt = (q+" "+str(e))
        send_email(sendto,txt,subject)
        exit()
    mydb.commit()
    print('Vacation Data Adjusted')

    try:
        mycursor.execute(over)
    except Exception as e:
        txt = (q+" "+str(e))
        send_email(sendto,txt,subject)
        exit()
    mydb.commit()
    print('Overhead Data Adjusted')

    try:
        mycursor.execute(prop)
    except Exception as e:
        txt = (q+" "+str(e))
        send_email(sendto,txt,subject)
        exit()
    mydb.commit()
    print('Proposal Data Adjusted')



# delete csv file
try:
    os.remove(CSV)
except Exception as e:
    txt = str(e)
    send_email(sendto,txt,subject)
    exit()
# move excel file to done folder and rename the file to contain the current date (currentDate-AjeraExport.xlsx)
try:
    shutil.move(ajeraExport, ajeraArchive)
except Exception as e:
    txt = str(e)
    send_email(sendto,txt,subject)
    exit()

send_email(sendto,"Ajera Data Loaded Successfully","Ajera Data Loader Success")
exit()






