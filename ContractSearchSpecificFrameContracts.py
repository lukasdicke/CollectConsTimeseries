# title: "CollectConsTimeseriesDeltaXE"
# description: "This script collects specific consumption timeseries, queried from DeltaXE and stores them in a JSON-file."
# output: ".json"
# parameters: {}
# owner: "MCSO, Lukas Dicke"

import pandas as pd
from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta
from pytz import timezone
import json
import operator

from DeltaXE import Deltaxe_client

#from eva.ops.deltaXE import Deltaxe_client

SUBSTRING_CONS_REPORT = "CONSMASTER_NEVER_USED"
SUBSTRING_PROD_REPORT = "PRODMASTER_NEVER_USED"

DAYS_BACK = -1900

def gettimedifference():
    # time difference between UTC and Europe/Berlin
    # 1h or 2h
    utcnow = timezone('utc').localize(date_from)
    here = utcnow.astimezone(timezone('UTC')).replace(tzinfo=None)
    there = utcnow.astimezone(timezone('Europe/Berlin')).replace(tzinfo=None)
    offset = relativedelta(here, there)

    return offset.hours


def getcontractsearchstring(contractdict):
    ret = ""
    for contract in contractdict:
        ret = ret + "EXTERNAL_ID eq '" + contract + "' or "

    ret = ret.removesuffix(' or ')

    return ret

def filenameconsreportGridSpecific(grid):
    return (datetime.today()).strftime("%Y%m%d_%H%M%S") + "_" + SUBSTRING_CONS_REPORT + "_" + grid + ".json"

def filenameprodreportGridSpecific(grid,year):
    return (datetime.today()).strftime("%Y%m%d_%H%M%S") + "_" + SUBSTRING_PROD_REPORT + "_" + grid + "_" + str(year) + ".json"

def getPathConsReport():
    return "\\\\energycorp.com\\common\\divsede\\Operations\\Schedules\\Germany\\ConsumptionReportsPython\\"

def GetListGrids():
    myDictGrids = []

    myDictGrids.append('DE_AMPRION_N')
    myDictGrids.append('DE_TENNET_N')
    myDictGrids.append('DE_50HERTZ_N')
    myDictGrids.append('DE_TRANSNETBW_N')

    return myDictGrids

def GetUrlContractSearchConsmaster(grid):
   return "Contract?$" \
                        + "filter=DATE_FROM lt " + (date_from.replace(tzinfo=None) + timedelta(days=DAYS_BACK)).isoformat() + \
                        "Z and DATE_TO ge " + (date_from.replace(tzinfo=None) + timedelta(days=1)).isoformat() + \
                        "Z and BALANCE eq 'INTRASED' and IN_PARTY eq 'CONSMASTER[" + grid + "]' and OUT_AREA eq '" + grid + "'" + \
                        " and STATUS ne 'DELETED'"

def GetUrlContractSearchProdmaster(grid):
   return "Contract?$" \
                        + "filter=DATE_FROM lt " + (date_from.replace(tzinfo=None) + timedelta(days=DAYS_BACK)).isoformat() + \
                        "Z and DATE_TO ge " + (date_from.replace(tzinfo=None) + timedelta(days=1)).isoformat() + \
                        "Z and BALANCE eq 'INTRASED' and OUT_PARTY eq 'PRODMASTER[" + grid + "]' and OUT_AREA eq '" + grid + "'" + \
                        " and STATUS ne 'DELETED'"

def GetUrlContractSearchProdmasterSpecificYear(grid, year):
   return "Contract?$" \
                        + "filter=DATE_FROM lt " + (datetime.strptime(str(year) + '-01-01', '%Y-%m-%d') + timedelta(days=0)).isoformat() + \
                        "Z and DATE_TO ge " + (datetime.strptime(str(year) + '-12-31', '%Y-%m-%d') + timedelta(days=0)).isoformat() + \
                        "Z and BALANCE eq 'INTRASED' and OUT_PARTY eq 'PRODMASTER[" + grid + "]' and OUT_AREA eq '" + grid + "'" + \
                        " and STATUS ne 'DELETED'"

def GiveOutput(url, filename):
        final_list = []

        responseContractSearchConsmaster = deltaXE.get(url).json()

        for contracts in responseContractSearchConsmaster['value']:

            timeseriesVolumes = []

            internalId = contracts["CONTRACT_ID"]

            name = contracts["NAME"]

            cp = contracts["COUNTERPARTY"]

                # if internalId < 100000 and cp == 'GARATH':
            if name != '':

                    date_From = datetime.strptime(str(year) + '-01-01', '%Y-%m-%d')
                    date_To = datetime.strptime(str(year)+'-12-31', '%Y-%m-%d')

                    urlContratLineSearch = "ContractLine?$filter=CONTRACT_ID eq " + str(internalId)
                    responseContractLineSearch = deltaXE.get(urlContratLineSearch).json()

                    for contractLines in responseContractLineSearch['value']:

                        contractLineID = str(contractLines['CONTRACT_LINE_ID'])
                        urlContractPositionSearch = "ContractPosition?$filter=CONTRACT_LINE_ID eq " + contractLineID

                        responseContractPositionSearch = deltaXE.get(urlContractPositionSearch).json()

                        for contractPosition in responseContractPositionSearch['value']:

                            if contractPosition['POSITION_TYPE'] == 'QUANTITY':

                                dateTo = (date_To.replace(tzinfo=None) + timedelta(0) + timedelta(
                                    hours=time_difference)).isoformat()
                                dateFrom = (date_From.replace(tzinfo=None) + timedelta(0) + timedelta(
                                    hours=time_difference)).isoformat()

                                positionID = str(contractPosition['POSITION_ID'])

                                # Known issue/feature: DateFrom & DateTo are switched
                                urlContractLines = "TimeSeriesHour?$filter=POSITION_ID eq " + positionID + \
                                                   " and DATE_FROM lt " + dateTo + "Z and DATE_TO gt " + dateFrom + "Z"
                                responseContractLines = deltaXE.get(urlContractLines).json()

                                for contractLine in responseContractLines['value']:
                                    if contractLine['VALUE'] is None:
                                        pass
                                    else:
                                        # All values will be positive
                                        timeseriesVolumes.append(contractLine["VALUE"])
                    if sum(timeseriesVolumes) == 0:
                        # myDict = {  "COUNTERPARTY": contracts["COUNTERPARTY"],
                        #                 "NAME": contracts["NAME"],
                        #                 "EXTERNAL_CONTRACT_ID": contracts["EXTERNAL_ID"],
                        #                 "CONTROL_AREA": contracts["OUT_AREA"],
                        #                 "BALANCE": contracts["BALANCE"],
                        #                 "AGG_VOLUME": (sum(timeseriesVolumes) / 4),
                        #                 "VOLUME_TS": timeseriesVolumes}

                        myDict = {  "COUNTERPARTY": contracts["COUNTERPARTY"],
                                        "NAME": contracts["NAME"],
                                        "EXTERNAL_CONTRACT_ID": contracts["EXTERNAL_ID"],
                                        "CONTROL_AREA": contracts["OUT_AREA"],
                                        "BALANCE": contracts["BALANCE"]}

                        final_list.append(myDict)

        path = pathConsReport + filename

        df = pd.DataFrame.from_dict(final_list)
        df.to_json(path, orient='records', indent=True)

begin = datetime.today() + timedelta(days=0)
date_from = pd.to_datetime(begin).normalize()

# time difference between UTC and Europe/Berlin
# 1h or 2h
time_difference = 1

# initializations
deltaXE = Deltaxe_client()

pathConsReport = getPathConsReport()

myDictGrids = GetListGrids()

for grid in myDictGrids:

    years = []
    years.append(2013)
    years.append(2014)
    years.append(2015)
    years.append(2016)
    years.append(2017)
    years.append(2018)
    years.append(2019)
    years.append(2020)
    years.append(2021)
    years.append(2022)
    years.append(2023)

    for year in years:

        urlContractSearch = GetUrlContractSearchProdmasterSpecificYear(grid, year)

        #urlConsmaster = GetUrlContractSearchConsmaster(grid)

        #filenameConsmaster = filenameconsreportGridSpecific(grid)
        filenameProdmaster = filenameprodreportGridSpecific(grid, year)

        #GiveOutput(urlConsmaster, filenameConsmaster, datetimeStart2013, datetimeEnd2013)
        GiveOutput(urlContractSearch,filenameProdmaster)

print("Done")
