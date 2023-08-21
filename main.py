def GetContractSearchString(contractDict):

    ret=""
    for contract in contractDict:
        ret = ret + "EXTERNAL_ID eq '" + contract + "' or "

    ret = ret.removesuffix(' or ')

    return ret

def FilenameConsReport():
    return (datetime.today()).strftime("%Y%m%d_%H%M%S") +"_" + "CONSUMPTION_REPORT" + ".json"

def PathConsReport():
    return "\\\\energycorp.com\\common\\divsede\\Operations\\Personal_OPS\\Lukas\\"

import pandas as pd
from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta
from pytz import timezone
from DeltaXE import Deltaxe_client

begin = datetime.today() + timedelta(days=0)
date_from = pd.to_datetime(begin).normalize()

#time difference between UTC and Europe/Berlin
# 1h or 2h
utcnow = timezone('utc').localize(date_from)
here = utcnow.astimezone(timezone('UTC')).replace(tzinfo=None)
there = utcnow.astimezone(timezone('Europe/Berlin')).replace(tzinfo=None)
offset = relativedelta(here, there)
time_difference = offset.hours

#initializations


deltaXE = Deltaxe_client()

myDictContracts=[]
myDictGrids=[]
myDictContracts.append('IMBAs50HuI')
myDictContracts.append('IMBAsAMPuI')
myDictContracts.append('IMBAsTENuI')
myDictContracts.append('IMBAsTRNuI')

myDictGrids.append('DE_AMPRION_N')
myDictGrids.append('DE_TENNET_N')
myDictGrids.append('DE_50HERTZ_N')
myDictGrids.append('DE_TRANSNETBW_N')

final_list = []

for grid in myDictGrids:

    timeseriesVolumes = []

    urlContractSearch = "Contract?$" \
                        +"filter=DATE_FROM lt " + (date_from.replace(tzinfo=None) + timedelta(days=0)).isoformat() +\
                        "Z and DATE_TO ge " + (date_from.replace(tzinfo=None) + timedelta(days=0)).isoformat() +\
                        "Z and (" + GetContractSearchString(myDictContracts) + ") and OUT_AREA eq '" + grid + "'" +\
                        " and STATUS ne 'DELETED'"

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

                    dateTo = (date_from.replace(tzinfo=None) + timedelta(0) + timedelta(hours=time_difference)).isoformat()
                    dateFrom = (date_from.replace(tzinfo=None) + timedelta(-1) + timedelta(hours=time_difference)).isoformat()

                    positionID = str(contractPosition['POSITION_ID'])

                    #Known issue/feature: DateFrom & DateTo are switched
                    urlContractLines = "TimeSeriesQuarterHour?$filter=POSITION_ID eq " + positionID + " and DATE_FROM lt " + dateTo + "Z and DATE_TO gt " + dateFrom + "Z"
                    responseContractLines = deltaXE.get(urlContractLines).json()

                    timeseriesVolumes = []
                    for contractLine in responseContractLines['value']:
                        if contractLine['VALUE'] == None:
                            pass
                        else:
                            # All values will be positive
                            timeseriesVolumes.append(contractLine["VALUE"])
                    if timeseriesVolumes:

                        myDict = {"COUNTERPARTY": contracts["COUNTERPARTY"],
                                  # "DATE_FROM": (date_from + timedelta(-1) + timedelta(hours=0)).isoformat(),
                                  "DELIVERY_DATE": (datetime.today()).strftime("%d.%m.%Y"),
                                  "NAME": contracts["NAME"],
                                  "EXTERNAL_CONTRACT_ID": contracts["EXTERNAL_ID"],
                                  "CONTROL_AREA": contracts["OUT_AREA"],
                                  "BALANCE": contracts["BALANCE"],
                                  "AGG_VOLUME": (sum(timeseriesVolumes) / 4),
                                  "VOLUME_TS": ";".join(str(i) for i in timeseriesVolumes)}

                        final_list.append(myDict)

df = pd.DataFrame.from_dict(final_list)

path = PathConsReport() + FilenameConsReport

#df.to_csv(path, sep=";")
df.to_json(path, orient='records', indent=  True)
df



print("Done")