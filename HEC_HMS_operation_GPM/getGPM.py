import datetime
from datetime import datetime, timedelta
import sys
import ftplib
import os
import time
import zipfile
import gdal, osr
import csv, json
from pymongo import MongoClient
from geojson import Point, Feature, dump, FeatureCollection

pathScript = os.path.dirname(os.path.abspath("getGPML.py"))
HOME = os.path.abspath(os.path.join(os.path.dirname(__file__),'..','public'))
direktorizip = HOME + '/GPML/ZIP/'
direktoriasc = HOME + '/GPML/ASC/'
delta = 1
yesterday = datetime.strftime(datetime.now() - timedelta(delta), '%Y-%m-%d')
yesterday1 = datetime.strftime(datetime.now() - timedelta(delta+1), '%Y-%m-%d')
GPML_down = 'GPML_'+yesterday  

def getFTP(dataGPML):
    try:
        ftp = ftplib.FTP("ftpaddress") #remote
        ftp.login("username", "password")
        ftp.cwd("/")
        ftp.retrbinary("RETR " + dataGPML+'.zip', open(direktorizip+dataGPML+'.zip', 'wb').write)
        ftp.quit()
    except:
        pass

getFTP(GPML_down)

try:
    zip_ref = zipfile.ZipFile(direktorizip+GPML_down+'.zip', 'r')
    zip_ref.extractall(direktorizip)
    zip_ref.close()
except:
    pass
            
a =[]
for ndate in range (1,21):
    if (ndate < 10):
        a.append('2020/01/0'+ str(ndate)) 
    else:
        a.append('2020/01/'+ str(ndate))

a =[yesterday1]
    
print(a)

for day in a:
    yesterday = day.replace('-','')
    yesterday0 = day.replace('-','/')

    #yesterday0 = datetime.datetime.strptime(yesterday, '%Y%m%d').strftime('%Y/%m/%d')
    print(yesterday0)
    
    #File utk download
      
    
    #File2 zip
    GPML00 = 'GPML_'+yesterday+'000000'
    GPML03 = 'GPML_'+yesterday+'030000'
    GPML06 = 'GPML_'+yesterday+'060000'
    GPML09 = 'GPML_'+yesterday+'090000'
    GPML12 = 'GPML_'+yesterday+'120000'
    GPML15 = 'GPML_'+yesterday+'150000'
    GPML18 = 'GPML_'+yesterday+'180000'
    GPML21 = 'GPML_'+yesterday+'210000'

    #delete all xyz files

    for file in os.listdir(direktoriasc):
        if file.endswith(".xyz"):
            try:
                os.remove(os.path.join(direktoriasc, file))
            except:
              pass


    #delete all csv files

    for file in os.listdir(direktoriasc):
        if file.endswith(".csv"):
            try:
                os.remove(os.path.join(direktoriasc, file))
            except:
              pass

    #delete all json files

    for file in os.listdir(direktoriasc):
        if file.endswith(".json"):
            try:
                os.remove(os.path.join(direktoriasc, file))
            except:
              pass
              

    #EXTRACT ZIP FILE to ASC

    def unzip(dataGPML):
        try:
            zip_ref = zipfile.ZipFile(direktorizip+dataGPML+'.zip', 'r')
            zip_ref.extractall(direktoriasc)
            zip_ref.close()
        except:
            pass

    
    unzip(GPML00)
    unzip(GPML03)
    unzip(GPML06)
    unzip(GPML09)
    unzip(GPML12)
    unzip(GPML15)
    unzip(GPML18)
    unzip(GPML21)
    

    GPML00 = 'GPML_'+yesterday+'000000'
    GPML03 = 'GPML_'+yesterday+'030000'
    GPML06 = 'GPML_'+yesterday+'060000'
    GPML09 = 'GPML_'+yesterday+'090000'
    GPML12 = 'GPML_'+yesterday+'120000'
    GPML15 = 'GPML_'+yesterday+'150000'
    GPML18 = 'GPML_'+yesterday+'180000'
    GPML21 = 'GPML_'+yesterday+'210000'

    #3. CONVERT TO TIF 
    direktoritif = HOME + 'GPML/TIF/'

    def convertTIF(dataGPML):
        try:
            ds = gdal.Open(direktoriasc+dataGPML+'.asc')
            ds = gdal.Translate(direktoritif+dataGPML+'.tif', ds)
            ds = None
        except:
            pass
                
    convertTIF(GPML00)
    convertTIF(GPML03)
    convertTIF(GPML06)
    convertTIF(GPML09)
    convertTIF(GPML12)
    convertTIF(GPML15)
    convertTIF(GPML18)
    convertTIF(GPML21)


    #4. convert to xyz
    direktorixyz = HOME + 'GPML/XYZ/'

    def convertXYZ(dataGPML):
        try:
            ds = gdal.Open(direktoriasc+dataGPML+'.asc')
            ds = gdal.Translate(direktoriasc+dataGPML+'.xyz', ds)
            ds = None
        except:
            print('convertXYZ '+dataGPML+' gagal')
            pass

    convertXYZ(GPML00)
    convertXYZ(GPML03)
    convertXYZ(GPML06)
    convertXYZ(GPML09)
    convertXYZ(GPML12)
    convertXYZ(GPML15)
    convertXYZ(GPML18)
    convertXYZ(GPML21)

    #5. convert to csv

    direktoricsv = HOME + 'GPML/CSV/'

    def convertCSV(dataGPML, time):
        try:
            with open(direktoriasc+dataGPML+'.xyz', 'r') as in_file:
                stripped = (line.strip() for line in in_file)
                lines = (line.split(" ") for line in stripped if line)
                with open(direktoriasc+dataGPML+'.csv', 'w') as out_file:
                    writer = csv.writer(out_file)
                    writer.writerow(('lon', 'lat', 'ch'))
                    writer.writerows(lines)

            with open(direktoriasc+dataGPML+'.csv','r') as csvinput:
                with open(direktoriasc+dataGPML+'_f.csv', 'w') as csvoutput:
                    writer = csv.writer(csvoutput)

                    for row in csv.reader(csvinput):
                        if row:
                            if row[0] == "lon":
                                writer.writerow(row+["tgl"]+["time"])
                            elif (float(row[0]) > 107.2840315 and float(row[0]) < 108 and float(row[1]) > -7.3) :
                                writer.writerow(row+[yesterday0]+[time])
                                print(row+[yesterday0]+[time])
            
            with open(direktoriasc+dataGPML+'_f.csv') as input:
                with open(direktoriasc+dataGPML+'_f1.csv', 'w', newline='') as output:
                    writer = csv.writer(output)
                    for row in csv.reader(input):
                        if any(field.strip() for field in row):
                            writer.writerow(row)
            
        except:
            print('convertCSV '+dataGPML+' gagal')
            pass				

    convertCSV(GPML00,'00:00')
    convertCSV(GPML03,'03:00')
    convertCSV(GPML06,'06:00')
    convertCSV(GPML09,'09:00')
    convertCSV(GPML12,'12:00')
    convertCSV(GPML15,'15:00')
    convertCSV(GPML18,'18:00')
    convertCSV(GPML21,'21:00')
                    
    #convert to geojson

    def convertGEOJSON(dataGPML):
        try:
            tgl0 = dataGPML.replace('_f1.csv','')
            tgl1 = tgl0.replace('GPML_','')
            features = []
            with open(direktoriasc+dataGPML+'_f1.csv') as csvfile:
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
            with open(direktoriasc+dataGPML+".json", "w") as f:
                f.write('%s' % collection)
        except:
            print('convertJSON '+dataGPML+' gagal')
            pass        

    convertGEOJSON(GPML00)
    convertGEOJSON(GPML03)
    convertGEOJSON(GPML06)
    convertGEOJSON(GPML09)
    convertGEOJSON(GPML12)
    convertGEOJSON(GPML15)
    convertGEOJSON(GPML18)
    convertGEOJSON(GPML21)

    #insert to Mongodb

    client = MongoClient('localhost', 27017)
    db = client['smartwater']
    collection = db['GPML']

    def insertMONGO(dataGPML):
        #try:
        for file in os.listdir(direktoriasc):
            if file.endswith(".json"):
                #jsonfile = open(file, 'r')	
                page = open(direktoriasc+file)
                parsed = json.loads(page.read())
                for item in parsed["features"]:
                    key = {'geometry.coordinates':item['geometry']['coordinates'], 'tglsort':item['tglsort']}
                    #key = {'geometry.coordinates':item['geometry']['coordinates'], 'properties.tgl':item['properties']['tgl'], 'properties.time':item['properties']['time'], 'tglsort':item['tglsort']}
                    collection.update(key, item, upsert=True);
                    print("Sukses input ke Database : " + file) 
        #except:
        #    pass          

    insertMONGO(GPML00)
    insertMONGO(GPML03)
    insertMONGO(GPML06)
    insertMONGO(GPML09)
    insertMONGO(GPML12)
    insertMONGO(GPML15)
    insertMONGO(GPML18)
    insertMONGO(GPML21)

    client.close()
