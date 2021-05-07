import pymongo
from pymongo import MongoClient
import pprint
import urllib 
import os
import fileinput
import openpyxl
import subprocess
import datetime
import numpy as np
import arrow
import sys
import json

#Step 1----- Updating HEC-DSS Database System ---------------------------
# Retrieving the GPM data from the MongoDB ke file ecmwf.csv
today00 = float(arrow.now().format('YYYYMMDD'+'000000')) ##20191231210000

client = MongoClient('mongodb://localhost:27017/smartwater')
db = client.smartwater
GPML = db.GPML
discharge = db.discharge

pathScript = 'C:/Users/Mamad/Documents/OneDrive/WORK/SMARTWATER/smartwater/HEC-HMS/DAS_Cisangkuy_trmm/'
HOME = os.path.abspath(os.path.join(os.path.dirname(__file__),'..','/'))
HOME = pathScript + "/../"
HOME_PATH = pathScript + '/'

#Precipitation gage

if os.path.exists(HOME_PATH + 'trmm.csv'):
  os.remove(HOME_PATH + 'trmm.csv')
else:
    print ("File does not exist.")

with open(HOME_PATH + 'trmm.csv', 'a') as f:
   f.write("Location Names, TRMM, West Java")
   f.write("\n"+ "Location Ids, TRMM, West Java")
   f.write("\n"+ "Time, Rainfall, Rainfall")

tglsort1={}
tglsort1[0]=0
tgl1 = {}
tgl1[0] = 0
ch = {}
ch[0] = 0
i = 0

x =  '{ "data": [{"date":"0", "time":"0", "ch": 0}] }'   
# parsing JSON string: 
data = json.loads(x)
temp = data['data'] 
no_temp = 0
for record in GPML.find({"geometry": {'$near': {'$geometry':{ 'type': "Point", 'coordinates': [107.34999999999999, -6.65000000] }, '$maxDistance': 1000}}, "tglsort": {"$lt": today00}}).sort("tglsort",1):
    i = i + 1
    time = record['properties']['time'].encode("utf-8").decode("utf-8")
    tglsort1[i] = record['tglsort']
    date = record['properties']['tgl'].encode("utf-8").decode("utf-8")
    tgl1[i] = datetime.datetime.strptime(date, '%Y/%m/%d').strftime('%Y-%m-%d')
    tgl1write = datetime.datetime.strptime(date, '%Y/%m/%d').strftime('%Y-%m-%d')
    #try:
    ch[i] = float(record['properties']['ch'].encode("utf-8").decode("utf-8"))
    if tgl1[i] == tgl1[i-1]:
        ch[i] = ch[i-1] + float(record['properties']['ch'].encode("utf-8").decode("utf-8"))
    else:    
        chjadi = str(ch[i-1])
        chjadi2 = eval(chjadi)
        tgljadi = str(tgl1[i-1])
        #print(i-1, tgljadi, chjadi2)
        if tgljadi != str(0):
            #f.write("\n"+ tgljadi + " " + "00:00" + "," + str(float(chjadi2))+ "," + str(float(chjadi2)))
            no_temp = no_temp + 1                
            y = {"date":tgljadi, 
                 "time": "00:00", 
                 "ch": chjadi2
                }               
            temp.append(y) 
    #except:
    #    pass
jmlhdata = no_temp -1 
print('jumlah :', jmlhdata)   
date1 = '2020-01-01'
today0 = datetime.date.today()
date2 = (today0 + datetime.timedelta(days=1)).isoformat()
start = datetime.datetime.strptime(date1, '%Y-%m-%d')
end = datetime.datetime.strptime(date2, '%Y-%m-%d')
step = datetime.timedelta(days=1)

while start <= end:
    temp2 = 0
    for x in range(jmlhdata):  
        ks = list(temp[x].items())
        if str(start.date()) == ks[0][1]:
            with open(HOME_PATH + 'trmm.csv', 'a') as f:
                f.write("\n"+ str(start.date()) + " " + "00:00" + "," + str(float(ks[2][1]))+ "," + str(float(ks[2][1])))
                temp2 = 1
            break
    if temp2 == 0 :
        with open(HOME_PATH + 'trmm.csv', 'a') as f:
            f.write("\n"+ str(start.date()) + " " + "00:00" + "," + "0" + "," + "0")
            #print (str(start.date()) + " " + "00:00" + "," + "0")
        
    start += step

#Cipanunjang_average gage

if os.path.exists(HOME_PATH + 'cipanunjang_discharge_gage.csv'):
  os.remove(HOME_PATH + 'cipanunjang_discharge_gage.csv')
else:
    print ("File does not exist.")

with open(HOME_PATH + 'cipanunjang_discharge_gage.csv', 'a') as f:
   f.write("Location Names, CIPANUNJANG AVERAGE, West Java")
   f.write("\n"+ "Location Ids, CIPANUNJANG AVERAGE, West Java")
   f.write("\n"+ "Time, Flow, Flow")
for record in discharge.find({"stasiun": "cipanunjang"}).sort("sort",1):
    date = record['date'].encode("utf-8").decode("utf-8")
    try: 
        tgl1 = datetime.datetime.strptime(date, '%Y/%m/%d').strftime('%Y-%m-%d')
        val = record['val']
        #pprint.pprint(str(tgl1) + " " + str(tgl1) + " " + str(val)) 
        with open(HOME_PATH + 'cipanunjang_discharge_gage.csv', 'a') as f:
           f.write("\n"+ tgl1 + " " + "00:00:00" + "," + str(float(val))+ "," + str(float(val)))
           #print(tgl1) 
    except:
        pass

    #print(tgl1) #2019-08-05

now = datetime.datetime.now()
#print now.year, now.month, now.day, now.hour, now.minute, now.second
dt = now.year - 2019    
for m in range(1, dt+1):
    year = 2019 + m
    print(year, m)
    for record in discharge.find({"stasiun": "cipanunjang"}).sort("sort",1):
        date = record['date'].encode("utf-8").decode("utf-8")
        try: 
            date1 = datetime.datetime.strptime(date, '%Y/%m/%d').strftime('%m/%d')
            #print(date1)
            date2 = str(year) + "/"+ date1
            tgl1 = datetime.datetime.strptime(date2, '%Y/%m/%d').strftime('%Y-%m-%d')
            tglskrg = datetime.date.today()
            tglbesok = (tglskrg + datetime.timedelta(days=5)).isoformat()
            print(tglbesok)
            if (tgl1 == tglbesok):
                break
            #print(tgl1)
            val = record['val']
            pprint.pprint(str(tgl1) + " " + str(tgl1) + " " + str(val)) 
            with open(HOME_PATH + 'cipanunjang_discharge_gage.csv', 'a') as f:
               f.write("\n"+ tgl1 + " " + "00:00:00" + "," + str(float(val))+ "," + str(float(val)))
        except:
            pass
        
#Cileunca_average gage    
    
if os.path.exists(HOME_PATH + 'cileunca_discharge_gage.csv'):
  os.remove(HOME_PATH + 'cileunca_discharge_gage.csv')
else:
    print ("File does not exist.")

with open(HOME_PATH + 'cileunca_discharge_gage.csv', 'a') as f:
   f.write("Location Names, CILEUNCA AVERAGE, West Java")
   f.write("\n"+ "Location Ids, CILEUNCA AVERAGE, West Java")
   f.write("\n"+ "Time, Flow, Flow")
for record in discharge.find({"stasiun": "cileunca"}).sort("sort",1):
    date = record['date'].encode("utf-8").decode("utf-8")
    try: 
        tgl1 = datetime.datetime.strptime(date, '%Y/%m/%d').strftime('%Y-%m-%d')
        val = record['val']
        #pprint.pprint(str(tgl1) + " " + str(tgl1) + " " + str(val)) 
        with open(HOME_PATH + 'cileunca_discharge_gage.csv', 'a') as f:
           f.write("\n"+ tgl1 + " " + "00:00:00" + "," + str(float(val))+ "," + str(float(val)))
           #print(tgl1) 
    except:
        pass

    #print(tgl1) #2019-08-05

now = datetime.datetime.now()
#print now.year, now.month, now.day, now.hour, now.minute, now.second
dt = now.year - 2019    
for m in range(1, dt+1):
    year = 2019 + m
    print(year, m)
    for record in discharge.find({"stasiun": "cileunca"}).sort("sort",1):
        date = record['date'].encode("utf-8").decode("utf-8")
        try: 
            date1 = datetime.datetime.strptime(date, '%Y/%m/%d').strftime('%m/%d')
            #print(date1)
            date2 = str(year) + "/"+ date1
            tgl1 = datetime.datetime.strptime(date2, '%Y/%m/%d').strftime('%Y-%m-%d')
            tglskrg = datetime.date.today()
            tglbesok = (tglskrg + datetime.timedelta(days=5)).isoformat()
            print(tglbesok)
            if (tgl1 == tglbesok):
                break
            #print(tgl1)
            val = record['val']
            pprint.pprint(str(tgl1) + " " + str(tgl1) + " " + str(val)) 
            with open(HOME_PATH + 'cileunca_discharge_gage.csv', 'a') as f:
               f.write("\n"+ tgl1 + " " + "00:00:00" + "," + str(float(val))+ "," + str(float(val)))
        except:
            pass

# Running ‘HEC-DSSVue.cmd’ from the directory of HEC-DSSVue installation

pathcommandvue ="C:/Program Files (x86)/HEC/HEC-DSSVue/"

#os.system(pathcommand+'HEC-HMS.cmd -s '+ pathscript+'run_trmm.script')
os.chdir(pathcommandvue)
print("Current Working Directory " , os.getcwd())
os.system('HEC-DSSVue.cmd ' + HOME_PATH + 'csvtodss_trmm.py')

#Step 2---------UPDATING CONTROL FILE----------------------------------------

filename = HOME_PATH + "trmm.control"
today1 = arrow.now().format('YYYY-MM-DD')
endate1 = datetime.datetime.strptime(today1, '%Y-%m-%d').strftime('%d %B %Y') #05 August 2019 
startdate1 = datetime.datetime.strptime(tgl1, '%Y-%m-%d').strftime('%Y')
enddate1 = datetime.datetime.strptime(today1, '%Y-%m-%d').strftime('%d %B %Y')
old1 = "     Start Date:"   # if any line contains this text, I want to modify the whole line.
new1 = "     Start Date: 01 Jan "+ startdate1 
old2 = "     End Date:"   # if any line contains this text, I want to modify the whole line.
new2 = "     End Date: "+ enddate1 

#Step 3---------UPDATE GAGE FILE -------------------------------------------------
filename1 = HOME_PATH + "DAS_Cisangkuy_trmm.gage"
#date ="15Aug2019"
date = datetime.datetime.strptime(today1, '%Y-%m-%d').strftime('%d %b %Y') 
endate = enddate1        #harus diubah sesuai tanggal data terakhir TRMM 
path_old1 = "       DSS Pathname: //TRMM/PRECIP-INC/"   # if any line contains this text, I want to modify the whole line.
path_new1 = "       DSS Pathname: //TRMM/PRECIP-INC/31Dec2001 - "+ date +"/1DAY/GAGE/"
path_old2 = "       End Time:"
path_new2 = "       End Time: "+enddate1+", 00:00"

def replacetext(file,old,new):
    x = fileinput.input(files=file, inplace='true')
    for line in x:
        if old in line:
            line = new
        print (line.rstrip())
    x.close()

replacetext(filename,old2,new2)
replacetext(filename1,path_old1,path_new1)
replacetext(filename1,path_old2,path_new2)

#Step 4----------Running HEC-HMS---------------------------------------------

pathcommand ="C:/Program Files (x86)/HEC/HEC-HMS/4.2.1/"

os.chdir(pathcommand)
print("Current Working Directory " , os.getcwd())
os.system('HEC-HMS.cmd '+ '-s ' + HOME_PATH+'run_trmm.script')

#Step 5-----------Reading DSS output-----------------------------------------

os.chdir(HOME_PATH+'PYDSSTools/')
print("Current Working Directory " , os.getcwd())
os.system('python '+ HOME_PATH+'PYDSSTools/'+'outputHMS.py')