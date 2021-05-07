#0. enter to ftp
#1. download format zip
#2. extract to be asc
#3. convert to tif
#4. convert to xyz
#5. convert to csv
#6. convert to json
from datetime import datetime, timedelta
import sys
import ftplib
import os
import time
import zipfile
import gdal, osr
import glob
import shutil
import csv, json
from pymongo import MongoClient
from geojson import Point, Feature, dump, FeatureCollection

yesterday = datetime.strftime(datetime.now() - timedelta(1), '%Y.%m.%d')
year = datetime.strftime(datetime.now() - timedelta(1), '%Y')
yesterday = '2020.02.06'
#year = '2020'

pathScript = os.path.dirname(os.path.abspath("getFORECAST.py"))
HOME = os.path.abspath(os.path.join(os.path.dirname(__file__),'..','public'))
print(HOME)
direktori = HOME + '/FORECAST/GRIB/'
print(direktori)

#make direktori yesterday
try:
    os.makedirs(direktori)    
except :
    print("Directory already exists")
    pass  

try:
    os.makedirs(direktori+yesterday)    
except :
    print("Directory already exists")
    pass  

os.chdir(direktori+yesterday+'/')

def getFTP():
    ftpadd = "............." #fill this with the ftp address
    username = "username" #fill this with the username
    password = "password" #fill this with the password
    ftp = ftplib.FTP(ftpadd)
    ftp.login(username, password)
    ftp.cwd("/"+yesterday)

    filenames = ftp.nlst() # get filenames within the directory
    print (filenames)

    for filename in filenames:
        local_filename = os.path.join(direktori+yesterday+'/', 'FORECAST_'+ year + filename[11:-5])
        file = open(local_filename, 'wb')
        ftp.retrbinary('RETR '+ filename, file.write)

        file.close()

    ftp.quit()  

def processFilesForecast(): 
    #convert to tiff
    direktoritif = HOME + '/FORECAST/TIF/'
    try:
        os.makedirs(direktoritif)    
    except :
        print("Directory already exists")
        pass  

    os.chdir(direktori+yesterday+'/')

    for file in os.listdir(direktori+yesterday+'/'):
        if file.endswith(""):
            try:
                ds = gdal.Open(file)
                ds = gdal.Translate(direktori+yesterday+'/'+file+'.tif', ds)
                ds = None
                print(file)
            except:
                pass

    for file in os.listdir(direktori+yesterday+'/'):
        if file.endswith(".tif"):
            try:
                os.remove(os.path.join(direktoritif, file))
            except:
                pass
            shutil.move(os.path.join(direktori+yesterday+'/', file), os.path.join(direktoritif, file)) 

    #convert to xyz
    for file in os.listdir(direktori+yesterday+'/'):
        if file.endswith(""):
            ds = gdal.Open(direktori+yesterday+'/'+file)
            ds = gdal.Translate(direktori+yesterday+'/'+file+'.xyz', ds)
            ds = None
            print(file)
        
    #convert to csv
    for file in os.listdir(direktori+yesterday+'/'):
        if file.endswith(".xyz"):
            # try: 
            with open(direktori+yesterday+'/'+file, 'r') as in_file:
                stripped = (line.strip() for line in in_file)
                lines = (line.split(" ") for line in stripped if line)
                with open(direktori+yesterday+'/'+file.replace('xyz','csv'), 'w') as out_file:
                    writer = csv.writer(out_file)
                    writer.writerow(('lon', 'lat', 'ch'))
                    writer.writerows(lines)
            #except:
            #    pass
     
    #Add date and time to csv
    for file in os.listdir(direktori+yesterday+'/'):
        if file.endswith(".csv"):
            with open(direktori+yesterday+'/'+file,'r') as csvinput:
                tgl = file[9:-8]+'/'+file[13:-6]+'/'+file[15:-4]
                with open(direktori+yesterday+'/'+file.replace('.csv','_f.csv'), 'w') as csvoutput:
                    writer = csv.writer(csvoutput)
                    for row in csv.reader(csvinput, delimiter=','):
                        if row:
                            if row[0] == "lon":
                                writer.writerow(row+["tgl"]+["time"])
                            elif (float(row[0]) > 107.2840315 and float(row[0]) < 108 and float(row[1]) > -7.3 and float(row[1]) < -6.2) :
                                writer.writerow([row[0]]+[row[1]]+[row[2]]+[tgl]+['00:00'])
                                writer.writerow(row+[tgl]+['00:00'])
            
    # removing space line in csv
    for file in os.listdir(direktori+yesterday+'/'):
        if file.endswith("_f.csv"):            
            with open(direktori+yesterday+'/'+file) as input:
                with open(direktori+yesterday+'/'+file.replace('_f.csv','_f1.csv'), 'w', newline='') as output:
                    writer = csv.writer(output)
                    for row in csv.reader(input):
                        if any(field.strip() for field in row):
                            writer.writerow(row)

    #convert to geojson
 
    def convertGEOJSON():
    #    try:
            os.chdir(direktori+yesterday+'/')
            for file in os.listdir(direktori+yesterday+'/'):
                if file.endswith("_f1.csv"):
                    tgl0 = file.replace('_f1.csv','')
                    tgl1 = tgl0.replace('FORECAST_','')
                    features = []
                    with open(direktori+yesterday+'/'+file) as csvfile:
                        reader = csv.reader(csvfile, delimiter=',')
                        next(reader)
                        for lon, lat, ch, tgl, time in reader:
                            if lon == 'lon':
                                continue
                            else:
                                latitude, longitude = map(float, (lat, lon))
                                features.append(
                                    Feature(
                                        geometry = Point((float(longitude), float(latitude))),
                                        properties = {
                                            'ch': ch,
                                            'tgl': tgl,
                                            'time': time
                                        }, tglsort = int(tgl1)
                                    )
                                )

                    collection = FeatureCollection(features)
                    with open(direktori+yesterday+'/'+file.replace('_f1.csv','.json'), 'w') as f:    
                        f.write('%s' % collection)
     #   except:
     #       pass        

    convertGEOJSON()

    #insert to Mongodb

    client = MongoClient('localhost', 27017)
    db = client['smartwater']
    collection = db['forecast']

    def insertMONGO():
        #try:
        for file in os.listdir(direktori+yesterday+'/'):
            if file.endswith(".json"):
                page = open(direktori+yesterday+'/'+file)
                parsed = json.loads(page.read())
                for item in parsed["features"]:
                    key = {'geometry.coordinates':item['geometry']['coordinates'], 'properties.tgl':item['properties']['tgl'], 'properties.time':item['properties']['time'], 'tglsort':item['tglsort']}
                    collection.update(key, item, upsert=True)
                    print("Sukses input ke Database : " + file)
        #except:
        #    pass        

    insertMONGO()

    client.close()

    #delete all xyz files
    for file in os.listdir(direktori+yesterday):
        if file.endswith(".xyz"):
            try:
                os.remove(os.path.join(direktori+yesterday, file))
            except:
                pass
               
    #delete all json files
    for file in os.listdir(direktori+yesterday):
        if file.endswith(".json"):
            try:
                os.remove(os.path.join(direktori+yesterday, file))
            except:
                pass

    #delete all csv files
    for file in os.listdir(direktori+yesterday):
        if file.endswith(".csv"):
            try:
                os.remove(os.path.join(direktori+yesterday, file))
            except:
                pass

getFTP()
processFilesForecast()