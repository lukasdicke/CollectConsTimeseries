import json
from datetime import datetime

SUBSTRING_CONS_REPORT = "CONSUMPTION_REPORT"


def SendMailPythonServer(send_to, send_cc, send_bcc, subject, message, files=[]):
    msgBody = """<html><head></head>
        <style type = "text/css">
            table, td {height: 3px; font-size: 14px; padding: 5px; border: 1px solid black;}
            td {text-align: left;}
            body {font-size: 12px;font-family:Calibri}
            h2,h3 {font-family:Calibri}
            p {font-size: 14px;font-family:Calibri}
         </style>"""

    msgBody += "<h2>" + subject + "</h2>"
    # msgBody += "<h3>" + message + "</h3>"
    msgBody += message

    strFrom = "no-reply-duswvpyt002p@statkraft.de"

    msgRoot = MIMEMultipart('related')
    msgRoot['Subject'] = subject
    msgRoot['From'] = strFrom
    if len(send_to) == 1:
        msgRoot['To'] = send_to[0]
    else:
        msgRoot['To'] = ",".join(send_to)

    if len(send_cc) == 1:
        msgRoot['Cc'] = send_cc[0]
    else:
        msgRoot['Cc'] = ",".join(send_cc)

    if len(send_cc) == 1:
        msgRoot['Bcc'] = send_bcc[0]
    else:
        msgRoot['Bcc'] = ",".join(send_bcc)
    msgRoot.preamble = 'This is a multi-part message in MIME format.'

    msgAlternative = MIMEMultipart('alternative')
    msgRoot.attach(msgAlternative)

    for path in files:
        part = MIMEBase('application', "octet-stream")
        with open(path, 'rb') as file:
            part.set_payload(file.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition',
                        'attachment; filename={}'.format(Path(path).name))
        msgRoot.attach(part)

    msgText = MIMEText('Sorry this mail requires your mail client to allow for HTML e-mails.')
    msgAlternative.attach(msgText)

    msgText = MIMEText(msgBody, 'html')
    msgAlternative.attach(msgText)

    smtp = smtplib.SMTP('smtpdus.energycorp.com')
    smtp.sendmail(strFrom, recipientsTo, msgRoot.as_string())
    smtp.quit()

    print("Mail sent successfully from " + strFrom)

def GetTimestamp(Period):

    startHour =int ((Period-1) / 4);

    startMinute = ((startHour+1) * 4 - Period );

    if startMinute == 0:
        return str(startHour).zfill(2) + ":45" + " - " + str(startHour+1).zfill(2) + ":00"
    elif startMinute == 1:
        return str(startHour).zfill(2) + ":30" + " - " + str(startHour).zfill(2) + ":45"
    elif startMinute == 2:
        return str(startHour).zfill(2) + ":15" + " - " + str(startHour).zfill(2) + ":30"
    else:
        return str(startHour).zfill(2) + ":00" + " - " + str(startHour).zfill(2) + ":15"

def filenameconsreportAll():
    return (datetime.today()).strftime("%Y%m%d_%H%M%S") + "_" + SUBSTRING_CONS_REPORT + ".json"

def getPathConsReport():
    #return "\\\\energycorp.com\\common\\divsede\\Operations\\Schedules\\Germany\\ConsumptionReportsPython\\"
    return "\\\\energycorp.com\\common\\divsede\\Operations\\Schedules\\Germany\\ConsumptionReportsPython\\Test\\"

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
  def __init__(self, volume, period, timestamp):
    self.Volume = volume
    self.Period = period
    self.Timestamp = timestamp

timedelta=0
#timedelta=0

myDictGrids = GetListGrids()
for grid in myDictGrids:

    myConsReports = GetConsumptionReport(getPathConsReport(), SUBSTRING_CONS_REPORT, grid, timedelta)

    maxConsVolumeSingleQtrHr = 0
    maxAggrConsVolume = 0
    timestampMaxAggrConsVolume = 0
    reportsMin = 0
    sumReports = 0
    tsPointMaxSingleQtrHr = TsPoint(0, 0, "")
    tsPointMaxReportAggrVolume = TsPoint(0, 0, "")
    txPointMin = TsPoint(0, 0, "")
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

        localmax = 0
        for x in enumerate( aggrTimeseriesConsReport):
            if float(x[1]) > localmax and float(x[1]) > maxConsVolumeSingleQtrHr:
                localmax = float(x[1])
                tsPointMaxSingleQtrHr.Period = GetTimestamp(x[0] + 1)
                # tsPointMaxSingleQtrHr.Volume = localmax
                tsPointMaxSingleQtrHr.Timestamp = contract['REPORT_TIMESTAMP']

        if localmax > maxConsVolumeSingleQtrHr:
            maxConsVolumeSingleQtrHr = localmax
            tsPointMaxSingleQtrHr.Volume = maxConsVolumeSingleQtrHr

        sumAggrConsReport = sum(aggrTimeseriesConsReport)
        if sumAggrConsReport > maxAggrConsVolume:
            maxAggrConsVolume = sumAggrConsReport
            tsPointMaxReportAggrVolume.Volume = maxAggrConsVolume
            tsPointMaxReportAggrVolume.Timestamp = contract['REPORT_TIMESTAMP']

        localmin = min(aggrTimeseriesConsReport)
        if localmin < reportsMin:
            reportsMin = localmin

    lenReports = len(myConsReports)
    if lenReports > 0:
        avg = sumReports/(len(myConsReports))

        print(grid + ': Average cons volume over ' + str(len(myConsReports)) + ' reports: ' + str(round(avg,3)) + ' MWh')

        maxSingleQtrHrDetails = ""
        if tsPointMaxSingleQtrHr.Volume > 0:
            maxSingleQtrHrDetails = " (Period: " + str(tsPointMaxSingleQtrHr.Period) + ", Report time: " + str(tsPointMaxSingleQtrHr.Timestamp) + ")"
        print(grid + ': Max single quarter-hourly cons volume: ' + str(tsPointMaxSingleQtrHr.Volume) + ' MWh' + maxSingleQtrHrDetails)

        maxReportAggrVolumeDetails = ""
        if tsPointMaxReportAggrVolume.Volume > 0:
            maxReportAggrVolumeDetails = " (Report time: " + str(tsPointMaxReportAggrVolume.Timestamp) +")"
        print(grid + ': Max aggregated cons volume reported: ' + str(round(tsPointMaxReportAggrVolume.Volume, 3)) + ' MWh' + maxReportAggrVolumeDetails)

        print(grid + ': Min cons volume of ' + str(len(myConsReports)) + ' reports: ' + str(round(reportsMin, 3)) + ' MWh')
