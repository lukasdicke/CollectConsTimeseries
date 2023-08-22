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

SUBSTRING_CONS_REPORT = "CONSMASTER_CONTRACTS"
SUBSTRING_PROD_REPORT = "PRODMASTER_CONTRACTS"

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

def filenameprodreportGridSpecific(grid):
    return (datetime.today()).strftime("%Y%m%d_%H%M%S") + "_" + SUBSTRING_PROD_REPORT + "_" + grid + ".json"

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
                        + "filter=DATE_FROM lt " + (date_from.replace(tzinfo=None) + timedelta(days=0)).isoformat() + \
                        "Z and DATE_TO ge " + (date_from.replace(tzinfo=None) + timedelta(days=1)).isoformat() + \
                        "Z and BALANCE eq 'INTRASED' and IN_PARTY eq 'CONSMASTER[" + grid + "]' and OUT_AREA eq '" + grid + "'" + \
                        " and STATUS ne 'DELETED'"

def GetUrlContractSearchProdmaster(grid):
   return "Contract?$" \
                        + "filter=DATE_FROM lt " + (date_from.replace(tzinfo=None) + timedelta(days=0)).isoformat() + \
                        "Z and DATE_TO ge " + (date_from.replace(tzinfo=None) + timedelta(days=1)).isoformat() + \
                        "Z and BALANCE eq 'INTRASED' and OUT_PARTY eq 'PRODMASTER[" + grid + "]' and OUT_AREA eq '" + grid + "'" + \
                        " and STATUS ne 'DELETED'"

def GiveOutput(url,filename):
    final_list = []
    urlContractSearch = url

    responseContractSearchConsmaster = deltaXE.get(urlContractSearch).json()

    for contracts in responseContractSearchConsmaster['value']:

        internalId = contracts["CONTRACT_ID"]

        if internalId < 100000:

            myDict = {"COUNTERPARTY": contracts["COUNTERPARTY"],
                      "NAME": contracts["NAME"],
                      "EXTERNAL_CONTRACT_ID": contracts["EXTERNAL_ID"],
                      "INTERNAL_CONTRACT_ID": internalId,
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
time_difference = gettimedifference()

# initializations
deltaXE = Deltaxe_client()

pathConsReport = getPathConsReport()

myDictGrids = GetListGrids()

for grid in myDictGrids:

    urlConsmaster = GetUrlContractSearchConsmaster(grid)
    urlProdmaster = GetUrlContractSearchProdmaster(grid)

    filenameConsmaster = filenameconsreportGridSpecific(grid)
    filenameProdmaster = filenameprodreportGridSpecific(grid)

    GiveOutput(urlConsmaster, filenameConsmaster)
    GiveOutput(urlProdmaster, filenameProdmaster)

print("Done")
