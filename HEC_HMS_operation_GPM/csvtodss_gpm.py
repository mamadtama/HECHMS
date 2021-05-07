from hec.script import MessageBox
from hec.heclib.dss import HecDss
from hec.heclib.util import HecTime
from hec.io import TimeSeriesContainer
import java
import csv
import os

pathScript = os.path.dirname(os.path.abspath("csvtodss_trmm.py"))
#HOME_PATH = pathScript + '/'
HOME_PATH = 'C:/Users/Mamad/Documents/OneDrive/WORK/SMARTWATER/smartwater/HEC-HMS/DAS_Cisangkuy_trmm/'

def process(filecsv, var):
    try :
        try :
            NUM_METADATA_LINES = 3;
            DSS_FILE_PATH = HOME_PATH + 'DAS_Cisangkuy_trmm.dss'
            CSV_FILE_PATH = HOME_PATH + filecsv

            myDss = HecDss.open(DSS_FILE_PATH)
            csvReader = csv.reader(open(CSV_FILE_PATH, 'r'), delimiter=',', quotechar='|')
            csvList = list(csvReader)
            
            numLocations = len(csvList[0]) - 1
            numValues = len(csvList) - NUM_METADATA_LINES # Ignore Metadata
            locationIds = csvList[1][1:]
            print ('Start reading', numLocations, csvList[0][0], ':', ', '.join(csvList[0][1:]))
            print ('Period of ', numValues, 'values')
            print ('Location Ids :', locationIds)

            for i in range(0, numLocations):
                print ('/n>>>>>>> Start processing ', locationIds[i], '<<<<<<<<<<<<')
                values = []
                for j in range(NUM_METADATA_LINES, numValues + NUM_METADATA_LINES):
                    p = float(csvList[j][i+1])
                    values.append(p)

                print ('Value of ', locationIds[i], values[:10])
                tsc = TimeSeriesContainer()
                # tsc.fullName = "/BASIN/LOC/FLOW//1HOUR/OBS/"
                tsc.fullName = '//' + locationIds[i].upper() + '/' + var + '//1DAY/GAGE/'

                print ('Start time : ', csvList[NUM_METADATA_LINES][0])
                start = HecTime(csvList[NUM_METADATA_LINES][0])
                tsc.interval = 24 * 60
                times = []
                for value in values :
                  times.append(start.value())
                  start.add(tsc.interval)
                tsc.times = times
                tsc.values = values
                tsc.numberValues = len(values)
                tsc.units = "MM"
                tsc.type = "PER-CUM"
                myDss.put(tsc)

        except Exception, e :
            MessageBox.showError(' '.join(e.args), "Python Error")
        except java.lang.Exception, e :
            MessageBox.showError(e.getMessage(), "Error")
    finally :
        myDss.done()
        print ('/nCompleted converting ', CSV_FILE_PATH, ' to ', DSS_FILE_PATH)
        
process('trmm.csv','PRECIP-INC')
process('cileunca_discharge_gage.csv','FLOW')
process('cipanunjang_discharge_gage.csv','FLOW')        