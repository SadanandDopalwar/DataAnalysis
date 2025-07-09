import sys
import os


# Add the Clientdata folder to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'services'))

import logger_file
import db_connector
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import uvicorn
import asyncio
from threading import Thread
import json
import asyncio
import DiskSpace
import DataRetension
import Reports
from datetime import datetime, time, timedelta
# Extarcting Setting file

file_path = 'settings.json'

# Write the settings to the specified JSON file
with open(file_path, 'r') as f:
    settings = json.load(f)

notificationport = settings.get("notificationport")
log_file_path = settings.get("log_file_path")

IsDataRetension = settings['DataRetensionSettings']['IsDataRetension']
DataRetensionInterval = settings['DataRetensionSettings']['Interval']
ImageDays = settings['DataRetensionSettings']['ImageData']
ImageFolder = settings['DataRetensionSettings']['ImageFolder']

IsCheckDiskSpace = settings['DiskSpaceSettings']['IsCheckDiskSpace']
DiskPath = settings['DiskSpaceSettings']['DiskPath']
DiskCheckCutoff = settings['DiskSpaceSettings']['DiskCheckCutoff']
DiskSpaceInterval = settings['DiskSpaceSettings']['Interval']
IsSendMail = settings['DiskSpaceSettings']['IsSendMail']
MailRecipients = settings['DiskSpaceSettings']['MailRecipients']
connectionstring = settings['DatabaseSettings']['ConnectionString']
IsSendReports = settings['ReportsSettings']['IsSendReports']
IsCalibration = settings['ReportsSettings']['IsCalibration']
LastHours = settings['ReportsSettings']['LastHours']
TargetMinute = settings['ReportsSettings']['TargetMinute']
TargetHour = settings['ReportsSettings']['TargetHour']
IsPrimary = settings['ReportsSettings']['IsPrimary']
IsSecondary = settings['ReportsSettings']['IsSecondary']
IsTimeRange = settings['ReportsSettings']['IsTimeRange']
start_time_ist = settings['ReportsSettings']['start_time_ist']
end_time_ist = settings['ReportsSettings']['end_time_ist']
IsAlarm = settings['ReportsSettings']['IsAlarm']
IsUploader = settings['ReportsSettings']['IsUploader']
# Extracting the logger file
logger = logger_file.logging_handler(log_file_path)


client = db_connector.dbconnector(connectionstring, logger)

app = FastAPI()


async def DeleterService(logger):
    while True:
        logger.info("Checking Deleter Database")
        Thread(target=lambda: asyncio.run(DataRetension.DataDeleter(logger, ImageDays, ImageFolder))).start()    
        await asyncio.sleep(DataRetensionInterval)


async def DiskSpaceChecker(logger, DiskSpaceInterval):
    while True:
        logger.info("Checking Disk Space")
        Thread(target=lambda: asyncio.run(DiskSpace.check_disk_usage(DiskPath, DiskCheckCutoff, IsSendMail, MailRecipients, logger))).start()
        await asyncio.sleep(DiskSpaceInterval)

async def GenReports(logger):
    while True:
        now = datetime.now()
        target_time = datetime.combine(now.date(), time(TargetHour, TargetMinute))

        if now > target_time:
            # If current time has already passed 23:40 today, schedule for tomorrow
            target_time += timedelta(days=1)

        sleep_seconds = (target_time - now).total_seconds()
        logger.info(f"Next Reports scheduled at {target_time}. Sleeping for {sleep_seconds:.2f} seconds.")
        await asyncio.sleep(sleep_seconds)

        logger.info("Generating Reports")
        await DataRetension.DeleteReports(logger)
        Thread(target=lambda: asyncio.run(Reports.GetReportsData(client, logger, LastHours, IsPrimary, IsSecondary, IsCalibration, IsSendReports, IsTimeRange, start_time_ist, end_time_ist, IsAlarm, IsUploader))).start()
        

async def main(logger):
    if IsDataRetension == True:
        Thread(target=lambda: asyncio.run(DeleterService(logger))).start()
    
    if IsCheckDiskSpace == True:
        Thread(target=lambda: asyncio.run(DiskSpaceChecker(logger, DiskSpaceInterval))).start()

    if IsSendReports == True:
        Thread(target=lambda: asyncio.run(GenReports(logger))).start()



def start_uvicorn():
    uvicorn.run(app, host='0.0.0.0', port=notificationport)


if __name__ == "__main__":
    # Start FastAPI in a separate thread
    Thread(target=start_uvicorn).start()
    Thread(target=lambda: asyncio.run(main(logger))).start() 
