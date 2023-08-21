# description: "DeltaXE API class"
# owner: "Ozan Sahin"

import requests
import datetime
from pytz import timezone
import pandas as pd
from pyxos import q
from dateutil.relativedelta import relativedelta


class Deltaxe_client:

    def __init__(self):
        self.client_id = q.credentials.deltaxe_api.client_id
        self.client_secret = q.credentials.deltaxe_api.client_secret
        self.tokenURL = "https://apigw.statkraft.com/account/Authenticate"
        self.baseURL = "https://apigw.statkraft.com/odata/v1/odata/"
        self.Ocp_Apim_Subscription_Key = q.credentials.deltaxe_api.subscription_key
        self.token_req_payload = {'grant_type': 'client_credentials'}
        self.token_response = requests.post(self.tokenURL, data=self.token_req_payload,
                                            auth=(self.client_id, self.client_secret)).json()
        self.headers = {'Authorization': self.token_response["access_token"],
                        'Ocp-Apim-Subscription-Key': self.Ocp_Apim_Subscription_Key}

    def get(self, url):
        if self.token_expired():
            self.refresh_token()
        return requests.get(self.baseURL + url, headers=self.headers)

    def token_expired(self):
        return int(datetime.datetime.now().timestamp()) >= int(self.token_response["expires_on"])

    def refresh_token(self):
        self.token_response = requests.post(self.tokenURL, data=self.token_req_payload,
                                            auth=(self.client_id, self.client_secret)).json()
        self.headers = {'Authorization': self.token_response["access_token"],
                        'Ocp-Apim-Subscription-Key': self.Ocp_Apim_Subscription_Key}
        return

    def get_volumes(self, query, from_date: datetime, to_date: datetime, columns, tz) -> pd.DataFrame:

        '''This function returns the time series values of DeltaXE contracts for given date range.
        '''

        # time difference between UTC and Europe/Berlin
        # 1h or 2h
        utcnow = timezone('utc').localize(from_date)
        here = utcnow.astimezone(timezone('UTC')).replace(tzinfo=None)
        there = utcnow.astimezone(timezone(tz)).replace(tzinfo=None)
        offset = relativedelta(here, there)
        time_difference_from_date = offset.hours

        utcnow = timezone('utc').localize(to_date)
        here = utcnow.astimezone(timezone('UTC')).replace(tzinfo=None)
        there = utcnow.astimezone(timezone(tz)).replace(tzinfo=None)
        offset = relativedelta(here, there)
        time_difference_to_date = offset.hours

        d1 = (to_date.replace(tzinfo=None) + datetime.timedelta(hours=time_difference_to_date)).isoformat()
        d2 = (from_date.replace(tzinfo=None) + datetime.timedelta(hours=time_difference_from_date)).isoformat()

        # initializations
        final_list = []
        values = []

        response0 = self.get(query).json()

        if response0:

            for elements in response0['value']:

                contractID = str(elements['CONTRACT_ID'])
                url1 = "ContractLine?$filter=CONTRACT_ID eq " + contractID \
                       + " and DATE_FROM lt " + d1 + "Z and DATE_TO gt " + d2 + "Z"

                response1 = self.get(url1).json()

                for elements2 in response1['value']:

                    contractLineID = str(elements2['CONTRACT_LINE_ID'])
                    url2 = "ContractPosition?$filter=CONTRACT_LINE_ID eq " \
                           + contractLineID + " and DATE_FROM lt " + d1 + "Z and DATE_TO gt " + d2 + "Z"

                    response2 = self.get(url2).json()

                    for elements3 in response2['value']:

                        if elements3['POSITION_TYPE'] == 'QUANTITY':

                            positionID = str(elements3['POSITION_ID'])
                            url3 = "TimeSeriesHour?$filter=POSITION_ID eq " \
                                   + positionID + " and DATE_FROM lt " + d1 + \
                                   "Z and DATE_TO gt " + d2 + "Z"

                            response3 = self.get(url3).json()

                            values = []
                            for elements4 in response3['value']:
                                if elements4['VALUE'] == None:
                                    values.append("")
                                else:
                                    if elements['DIRECTION'] == 'SELL':
                                        values.append(elements4["VALUE"] * -1)
                                    else:
                                        values.append(elements4["VALUE"])

                            if values:
                                myDict = {i: elements[i] for i in columns}
                                myDict.update({"DATE_FROM": (from_date + datetime.timedelta(0) + datetime.timedelta(
                                    hours=0)).isoformat(),
                                               "DATE_TO": (to_date + datetime.timedelta(0) + datetime.timedelta(
                                                   hours=0)).isoformat(),
                                               "VALUE": ";".join(str(i) for i in values)})

                                final_list.append(myDict)

            df = pd.DataFrame.from_dict(final_list)
            return df

        else:
            print(response0)
            print("Check your query!")
            return

    def get_nomination_status(self, query, from_date: datetime, to_date: datetime, tz) -> pd.DataFrame:

        # initialize Data Frame
        df2 = pd.DataFrame()

        response1 = self.get(query).json()

        columns = []

        if response1['value']:

            # initialize Data Frame
            df = pd.DataFrame()
            version_dict = {}

            # Find the latest version
            for elements in response1['value']:

                if elements["MESSAGE_TYPE"] != "ScheduleMessage": continue

                if elements["MESSAGE_ID"] in version_dict.keys():
                    old_version = version_dict[elements["MESSAGE_ID"]]
                    new_version = elements["VERSION"]
                    if new_version > old_version:
                        version_dict[elements["MESSAGE_ID"]] = elements["VERSION"]
                else:
                    version_dict[elements["MESSAGE_ID"]] = elements["VERSION"]

            for elements in response1['value']:

                if elements["MESSAGE_TYPE"] != "ScheduleMessage": continue

                if version_dict[elements["MESSAGE_ID"]] == elements["VERSION"]:
                    df = df.append(elements, ignore_index=True)

            df = df.dropna(how='all')
            url1 = "ScheduleMessagePos?$" \
                   + "filter=SCHEDULE_MESSAGE_ID eq " + str(response1['value'][0]["SCHEDULE_MESSAGE_ID"]) \
                   + " and COMMUNICATION_STATUS ne 'Message created'"

            for ix, val in df.iterrows():
                mesID = str(val["SCHEDULE_MESSAGE_ID"])
                message_date = val["MESSAGE_DATE"]
                display_as = val["SCHEDULING_DISPLAY_AS"]
                url1 = "ScheduleMessagePos?$" \
                       + "filter=SCHEDULE_MESSAGE_ID eq " + mesID

                response2 = self.get(url1).json()
                if response2["value"]:
                    for each in response2['value']:
                        each.update({"MESSAGE_DATE": message_date, "NAME": display_as, \
                                     "DATE_FROM": val["SCHEDULING_DATE_FROM"], "DATE_TO": val["SCHEDULING_DATE_TO"]})
                        df2 = df2.append(each, ignore_index=True)

            return df2

        else:
            print(f"Check your query! This is the response: {response1}")
            return pd.DataFrame()

    def get_nomination_object(self, query, from_date: datetime, to_date: datetime, tz) -> pd.DataFrame:

        # initialize Data Frame
        df2 = pd.DataFrame()

        response1 = self.get(query).json()

        columns = []

        if response1['value']:

            # initialize Data Frame
            df = pd.DataFrame()
            version_dict = {}

            # Find the latest version
            for elements in response1['value']:

                if elements["MESSAGE_TYPE"] != "ScheduleMessage": continue

                if elements["MESSAGE_ID"] in version_dict.keys():
                    old_version = version_dict[elements["MESSAGE_ID"]]
                    new_version = elements["VERSION"]
                    if new_version > old_version:
                        version_dict[elements["MESSAGE_ID"]] = elements["VERSION"]
                else:
                    version_dict[elements["MESSAGE_ID"]] = elements["VERSION"]

            for elements in response1['value']:

                if elements["MESSAGE_TYPE"] != "ScheduleMessage": continue

                if version_dict[elements["MESSAGE_ID"]] == elements["VERSION"]:
                    df = pd.concat([df, pd.DataFrame([elements])], ignore_index=True)
                    # df = df.append(elements, ignore_index=True)

            df = df.dropna(how='all')

            return df

        else:
            print(f"Check your query! This is the response: {response1}")
            return pd.DataFrame()
