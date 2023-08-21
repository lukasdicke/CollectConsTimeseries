def GetContractSearchString(contractDict):

    ret=""
    for contract in contractDict:
        myDictContracts.append("EXTERNAL_ID eq '" + contract + "' or ")
        ret = ret + "EXTERNAL_ID eq '" + contract + "' or "

    ret = ret.removesuffix('or ')

    return ret

import pandas as pd
from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta
from pytz import timezone
from DeltaXE import Deltaxe_client

begin = datetime.today() + timedelta(days=1)
date_from = pd.to_datetime(begin).normalize()

#time difference between UTC and Europe/Berlin
# 1h or 2h
utcnow = timezone('utc').localize(date_from)
here = utcnow.astimezone(timezone('UTC')).replace(tzinfo=None)
there = utcnow.astimezone(timezone('Europe/Berlin')).replace(tzinfo=None)
offset = relativedelta(here, there)
time_difference = offset.hours

#initializations
final_list = []
timeseriesVolumes = []

deltaXE = Deltaxe_client()

myDictContracts=[]
myDictContracts.append('IMBAs50HuI')
myDictContracts.append('IMBAsAMPuI')
myDictContracts.append('IMBAsTENuI')
myDictContracts.append('IMBAsTRNuI')


urlContractSearch = "Contract?$" \
                    +"filter=DATE_FROM lt " + (date_from.replace(tzinfo=None) + timedelta(days=0)).isoformat() +\
                    "Z and DATE_TO ge " + (date_from.replace(tzinfo=None) + timedelta(days=0)).isoformat() +\
                    "Z and " + GetContractSearchString(myDictContracts) +\
                    " and STATUS ne 'DELETED'"

responseContractSearch = deltaXE.get(urlContractSearch).json()

#cnt=len(responseContractSearch['value'])

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
                              "DATE_FROM": (date_from + timedelta(-1) + timedelta(hours=0)).isoformat(),
                              "DATE_TO": (date_from + timedelta(days=0) + timedelta(hours=0)).isoformat(),
                              "NAME": contracts["NAME"],
                              "EXTERNAL_ID": contracts["EXTERNAL_ID"],
                              "AGG_VOLUME": (sum(timeseriesVolumes) / 4),
                              "VOLUME_TS": ";".join(str(i) for i in timeseriesVolumes)}

                    final_list.append(myDict)

df = pd.DataFrame.from_dict(final_list)
#df.to_csv("\\\\energycorp.com\\common\\divsede\\Operations\\Personal_OPS\\Lukas\\"+date_from.strftime("%d.%m.%Y")+"_Amprion_prod.csv", sep=";")
df.to_json("\\\\energycorp.com\\common\\divsede\\Operations\\Personal_OPS\\Lukas\\"+date_from.strftime("%d.%m.%Y")+"_Amprion_prod.json", orient='split')
df

print("Done")