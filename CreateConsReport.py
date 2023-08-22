import json
from datetime import datetime

SUBSTRING_CONS_REPORT = "CONSUMPTION_REPORT"

def filenameconsreportAll():
    return (datetime.today()).strftime("%Y%m%d_%H%M%S") + "_" + SUBSTRING_CONS_REPORT + ".json"

def getPathConsReport():
    return "\\\\energycorp.com\\common\\divsede\\Operations\\Schedules\\Germany\\ConsumptionReportsPython\\"

def GetConsumptionReport(myPath, substringToFind,grid,timedelta1):
    from os import listdir
    from os.path import isfile, join
    from datetime import datetime, timedelta
    consReports = []

    filesToDelete = [f for f in listdir(myPath)
                 if isfile(join(myPath, f))
                 if substringToFind in f
                 if grid in f
                 if (datetime.now() + timedelta(days=timedelta1)).strftime("%Y%m%d")  in f
                 ]

    for file in filesToDelete:
        consReports.append(join(myPath, file))

    return consReports

def GetListGrids():
    myDictGrids = []

    myDictGrids.append('DE_AMPRION_N')
    myDictGrids.append('DE_TENNET_N')
    myDictGrids.append('DE_50HERTZ_N')
    myDictGrids.append('DE_TRANSNETBW_N')

    return myDictGrids

class TsPoint:
  def __init__(self, volume, period):
    self.Volume = volume
    self.Period = period

timedelta=0
#timedelta=-1

myDictGrids = GetListGrids()
for grid in myDictGrids:

    myConsReports = GetConsumptionReport(getPathConsReport(), SUBSTRING_CONS_REPORT, grid, timedelta)

    reportsMax=0
    reportsMin=0
    sumReports=0
    for consReport in myConsReports:

        # Opening JSON file
        f = open(consReport)

        cons = json.load(f)

        aggrTimeseriesConsReport = [0] * 96
        for contract in cons:
            timeseriesFrameContract = contract['VOLUME_TS']
            sumReports = sumReports + float(contract['AGG_VOLUME'])
            for x in enumerate(timeseriesFrameContract):
                aggrTimeseriesConsReport[x[0]] = aggrTimeseriesConsReport[x[0]] + float(x[1])

        localmax = max(aggrTimeseriesConsReport)
        if localmax > reportsMax:
            reportsMax = localmax

        localmin = min(aggrTimeseriesConsReport)
        if localmin < reportsMin:
            reportsMin = localmin

    avg = sumReports/(len(myConsReports))

    print(grid + ': Average cons volume over ' + str(len(myConsReports)) + ' reports: ' + str(round(avg,3)) + ' MWh')
    print(grid + ': Max cons volume over ' + str(len(myConsReports)) + ' reports: ' + str(round(reportsMax, 3)) + ' MWh')
    print(grid + ': Min cons volume over ' + str(len(myConsReports)) + ' reports: ' + str(round(reportsMin, 3)) + ' MWh')

    test = ""