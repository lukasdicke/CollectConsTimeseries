# title: "CollectConsTimeseriesDeltaXE"
# description: "This script collects specific consumption timeseries, queried from DeltaXE and stores them in a JSON-file."
# output: ".json"
# parameters: {}
# owner: "MCSO, Lukas Dicke"

"""

Usage:
    example /PIXOS/EVA_Jobs/ops/CollectConsTimeseriesDeltaXE --daysAhead=<int>

"""


import argparse
from datetime import datetime
from datetime import timedelta

import pandas as pd
from dateutil.relativedelta import relativedelta
from pytz import timezone
from pyxos.task import args

from DeltaXE import Deltaxe_client

#from eva.ops.deltaXE import Deltaxe_client

CONST_SUBSTRING_CONS_REPORT = "CONSUMPTION_REPORT"


CONST_JSON_COUNTERPARTY = "COUNTERPARTY"
CONST_JSON_DELIVERY_DATE = "DELIVERY_DATE"
CONST_JSON_REPORT_TIMESTAMP = "REPORT_TIMESTAMP"
CONST_JSON_NAME = "NAME"
CONST_JSON_EXTERNAL_CONTRACT_ID = "EXTERNAL_CONTRACT_ID"
CONST_JSON_CONTROL_AREA = "CONTROL_AREA"
CONST_JSON_BALANCE = "BALANCE"
CONST_JSON_AGG_VOLUME = "AGG_VOLUME (MWh)"
CONST_JSON_MIN_VOLUME = "MIN_VOLUME (MW)"
CONST_JSON_MAX_VOLUME = "MAX_VOLUME (MW)"
CONST_JSON_VOLUME_TS = "VOLUME_TS"

date_now = datetime.today()


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


def filenameconsreportGridSpecific(grid, daysAhead):
    deliverySpecificString = ""
    if daysAhead == 0:
        deliverySpecificString = "INTRADAY"
    elif daysAhead == 1:
        deliverySpecificString = "DAYAHEAD"
    elif daysAhead == -1:
        deliverySpecificString = "DAYAFTER"

    return (date_now + timedelta(days=daysAhead)).strftime(
        "%Y%m%d_%H%M%S") + "_" + CONST_SUBSTRING_CONS_REPORT + "_" + grid + "_" + deliverySpecificString + ".json"


def getPathConsReport():
    return "\\\\energycorp.com\\common\\divsede\\Operations\\Schedules\\Germany\\ConsumptionReportsPython\\"


def HousekeepingConsumptionReports(myPath, substringToFind):
    from os import listdir
    from os import remove as FileDelete
    from os.path import isfile, join
    from datetime import datetime, timedelta

    filesToDelete = [f for f in listdir(myPath)
                     if isfile(join(myPath, f))
                     if substringToFind in f
                     if (datetime.now() + timedelta(days=2)).strftime("%Y%m%d") not in f
                     if (datetime.now() + timedelta(days=1)).strftime("%Y%m%d") not in f
                     if (datetime.now() + timedelta(days=0)).strftime("%Y%m%d") not in f
                     if (datetime.now() + timedelta(days=-1)).strftime("%Y%m%d") not in f
                     if (datetime.now() + timedelta(days=-2)).strftime("%Y%m%d") not in f
                     if (datetime.now() + timedelta(days=-3)).strftime("%Y%m%d") not in f
                     if (datetime.now() + timedelta(days=-4)).strftime("%Y%m%d") not in f
                     if (datetime.now() + timedelta(days=-5)).strftime("%Y%m%d") not in f
                     if (datetime.now() + timedelta(days=-6)).strftime("%Y%m%d") not in f
                     if (datetime.now() + timedelta(days=-7)).strftime("%Y%m%d") not in f
                     ]

    for file in filesToDelete:
        FileDelete(join(myPath, file))


def GetListGrids():
    myDictGrids = []

    myDictGrids.append('DE_AMPRION_N')
    myDictGrids.append('DE_TENNET_N')
    myDictGrids.append('DE_50HERTZ_N')
    myDictGrids.append('DE_TRANSNETBW_N')

    return myDictGrids


def GetListContracts():
    myDictContracts = []

    myDictContracts.append('IMBAs50HuI')
    myDictContracts.append('IMBAsAMPuI')
    myDictContracts.append('IMBAsTENuI')
    myDictContracts.append('IMBAsTRNuI')
    # myDictContracts.append('')
    # myDictContracts.append('')
    # myDictContracts.append('')
    # myDictContracts.append('')
    # myDictContracts.append('')
    # myDictContracts.append('')

    return myDictContracts


def GetUrlContractSearchSpecificContracts(grid, myDictContracts, daysAhead):
    return "Contract?$" \
        + "filter=DATE_FROM lt " + (date_from.replace(tzinfo=None) + timedelta(days=daysAhead)).isoformat() + \
        "Z and DATE_TO ge " + (date_from.replace(tzinfo=None) + timedelta(days=daysAhead + 1)).isoformat() + \
        "Z and (" + getcontractsearchstring(myDictContracts) + ") and OUT_AREA eq '" + grid + "'" + \
        " and STATUS ne 'DELETED'"


begin = datetime.today() + timedelta(days=0)
date_from = pd.to_datetime(begin).normalize()

# time difference between UTC and Europe/Berlin
# 1h or 2h
time_difference = gettimedifference()

# initializations
deltaXE = Deltaxe_client()

pathConsReport = getPathConsReport()

HousekeepingConsumptionReports(pathConsReport, CONST_SUBSTRING_CONS_REPORT)

myDictGrids = GetListGrids()


def GiveOutput(daysAhead):
    for grid in myDictGrids:

        timeseriesVolumes = []
        final_list = []

        urlContractSearch = GetUrlContractSearchSpecificContracts(grid, GetListContracts(), daysAhead)

        responseContractSearch = deltaXE.get(urlContractSearch).json()

        for contracts in responseContractSearch['value']:

            contractID = str(contracts['CONTRACT_ID'])
            urlContratLineSearch = "ContractLine?$filter=CONTRACT_ID eq " + contractID
            responseContractLineSearch = deltaXE.get(urlContratLineSearch).json()

            for contractLines in responseContractLineSearch['value']:

                contractLineID = str(contractLines['CONTRACT_LINE_ID'])
                urlContractPositionSearch = "ContractPosition?$filter=CONTRACT_LINE_ID eq " + contractLineID

                responseContractPositionSearch = deltaXE.get(urlContractPositionSearch).json()

                for contractPosition in responseContractPositionSearch['value']:

                    if contractPosition['POSITION_TYPE'] == 'QUANTITY':

                        dateTo = (date_from.replace(tzinfo=None) + timedelta(1 + daysAhead) + timedelta(
                            hours=time_difference)).isoformat()
                        dateFrom = (date_from.replace(tzinfo=None) + timedelta(0 + daysAhead) + timedelta(
                            hours=time_difference)).isoformat()

                        positionID = str(contractPosition['POSITION_ID'])

                        # Known issue/feature: DateFrom & DateTo are switched
                        urlContractLines = "TimeSeriesQuarterHour?$filter=POSITION_ID eq " + positionID + \
                                           " and DATE_FROM lt " + dateTo + "Z and DATE_TO gt " + dateFrom + "Z"
                        responseContractLines = deltaXE.get(urlContractLines).json()

                        timeseriesVolumes = []
                        for contractLine in responseContractLines['value']:
                            if contractLine['VALUE'] is None:
                                pass
                            else:
                                # All values will be positive
                                timeseriesVolumes.append(contractLine["VALUE"])
                        if timeseriesVolumes:
                            myDict = {CONST_JSON_COUNTERPARTY: contracts["COUNTERPARTY"],
                                      CONST_JSON_DELIVERY_DATE: (datetime.today() + timedelta(daysAhead)).strftime("%d.%m.%Y"),
                                      CONST_JSON_REPORT_TIMESTAMP: date_now.strftime("%H:%M:%S"),
                                      CONST_JSON_NAME: contracts["NAME"],
                                      CONST_JSON_EXTERNAL_CONTRACT_ID: contracts["EXTERNAL_ID"],
                                      CONST_JSON_CONTROL_AREA: contracts["OUT_AREA"],
                                      CONST_JSON_BALANCE: contracts["BALANCE"],
                                      CONST_JSON_AGG_VOLUME: round((sum(timeseriesVolumes) / 4), 3),
                                      CONST_JSON_MIN_VOLUME: round(min(timeseriesVolumes), 3),
                                      CONST_JSON_MAX_VOLUME: round(max(timeseriesVolumes), 3),
                                      CONST_JSON_VOLUME_TS: timeseriesVolumes}


                            final_list.append(myDict)

        df = pd.DataFrame.from_dict(final_list)

        path = pathConsReport + filenameconsreportGridSpecific(grid, daysAhead)

        # df.to_csv(path, sep=";")
        df.to_json(path, orient='records', indent=True)


print(args[0] + ':' + args[1])

if args == None:
    daysAhead = 0
else:
    daysAhead = int(args[1])

print("Delivery day: " + (datetime.today() + timedelta(daysAhead)).strftime("%d.%m.%Y") + " chosen. Script started.")

GiveOutput(daysAhead)

print("Done")
