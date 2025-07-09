from datetime import datetime, timedelta, timezone
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import pytz
import SendReports
import asyncio
from threading import Thread
import Reports

def datetime_to_ticks(dt):
    """
    Convert datetime (UTC) to .NET ticks (number of 100-ns intervals since 0001-01-01T00:00:00Z)
    """
    epoch = datetime(1, 1, 1, tzinfo=timezone.utc)
    ticks = int((dt - epoch).total_seconds() * 10**7)
    return ticks

def ticks_to_datetime(ticks):
    # .NET ticks start from 0001-01-01
    # Each tick = 100 nanoseconds
    epoch_start = datetime(1, 1, 1, tzinfo=timezone.utc)
    return epoch_start + timedelta(microseconds=ticks / 10)


async def GenerateCalibrationReport(client, LastHours, logger, IsTimeRange, start_time_ist, end_time_ist):
    db = client["CalibrationService"]

    # Access collection
    collection = db["CalibrationLogs"]
    UTC = pytz.utc
    IST = pytz.timezone("Asia/Kolkata")
    if IsTimeRange:
        # Assume start_time_ist and end_time_ist are datetime objects in IST
        # Convert them to UTC
        start_time_utc = IST.localize(start_time_ist).astimezone(UTC)
        end_time_utc = IST.localize(end_time_ist).astimezone(UTC)

        now_utc = start_time_utc
        utc_24hrs_ago = end_time_utc
    else:
        now_utc = datetime.now(timezone.utc)
        # Calculate 24 hours ago
        utc_24hrs_ago = now_utc - timedelta(hours=LastHours)

    # Build query to filter documents between now and 24 hours ago
    # query = {
    #     "DateTime.DateTime": {
    #         "$gte": utc_24hrs_ago,
    #         "$lte": now_utc
    #     }
    # }
    start_ticks = datetime_to_ticks(utc_24hrs_ago)
    end_ticks = datetime_to_ticks(now_utc)
    start_ist = ticks_to_datetime(start_ticks).astimezone(pytz.timezone("Asia/Kolkata"))
    logger.info(f"Start IST: {start_ist.strftime('%d-%m-%Y %H:%M:%S')}")

    # Mongo query
    query = {
        "DateTime.0": {
            "$gte": start_ticks,
            "$lte": end_ticks
        }
    }
    logger.info("Fetching data from database")
    results = collection.find(query)
    def safe_get(doc, keys):
        for key in keys:
            if isinstance(doc, dict):
                doc = doc.get(key)
            else:
                return None
        return doc

    # Extract desired fields
    data = []
    for doc in results:
        #print(doc)
        utc_time = safe_get(doc, ['DateTime', 'DateTime'])  # naive UTC datetime
        if utc_time:
            # Localize as UTC and then convert to IST
            utc_time = UTC.localize(utc_time)
            ist_time = utc_time.astimezone(IST).strftime('%d-%m-%Y %H:%M:%S')
            utc_str = utc_time.strftime('%d-%m-%Y %H:%M:%S')
        else:
            ist_time = 'UNKNOWN'
            utc_str = 'UNKNOWN'
        row = {
            '_id': str(doc.get('_id')),
            'Barcode': doc.get('Code') or 'UNKNOWN',
            'Scan Timestamp': ist_time,
            'UTC Timestamp': utc_time.strftime('%Y-%m-%d %H:%M:%S') if utc_time else 'UNKNOWN',
            'Length Status': safe_get(doc, ['LengthStatus', 'Status']),
            'Width Status': safe_get(doc, ['WidthStatus', 'Status']),
            'Height Status': safe_get(doc, ['HeightStatus', 'Status']) or 'N/A',
            'Weight Status': safe_get(doc, ['WeightStatus', 'Status']),
            'Final Status': safe_get(doc, ['Status']),
            'MachineId': safe_get(doc, ['MachineDetails', 'UId']),
        
        }
        data.append(row)

    # Create DataFrame
    logger.info("Creating DataFrame from fetched data for calibration data")
    df = pd.DataFrame(data)

    # Export to CSV
    logger.info("Exporting Data from fetched calibration data")
    df.to_csv('CalibData.csv', index=False)

    logger.info(f"Exported {len(df)} documents to CalibData.csv")
    if len(df) == 0:
        logger.warning(f"No data found for calibration machines in the last {LastHours} hours.")
        return

async def GetInfeedReportsData(client, logger, LastHours, IsTimeRange, start_time_ist, end_time_ist):
    # Replace with your actual MongoDB connection string
    # client = MongoClient("mongodb://nido:Nido%40123@
    db = client["ProfilerService"]

    # Access collection
    collection = db["Sortings"]
    UTC = pytz.utc
    IST = pytz.timezone("Asia/Kolkata")
    if IsTimeRange:
        # Assume start_time_ist and end_time_ist are datetime objects in IST
        # Convert them to UTC
        start_time_utc = IST.localize(start_time_ist).astimezone(UTC)
        end_time_utc = IST.localize(end_time_ist).astimezone(UTC)

        now_utc = start_time_utc
        utc_24hrs_ago = end_time_utc
    else:
        now_utc = datetime.now(timezone.utc)
        # Calculate 24 hours ago
        utc_24hrs_ago = now_utc - timedelta(hours=LastHours)
    
    # Build query to filter documents between now and 24 hours ago
    # query = {
    #     "DateTime.DateTime": {
    #         "$gte": utc_24hrs_ago,
    #         "$lte": now_utc
    #     }
    # }
    query = {
        "$or": [
            {
                "DateTime.DateTime": {
                    "$gte": utc_24hrs_ago,
                    "$lte": now_utc
                }
            },
            {
                "PrimarySorting.Timestamp.DateTime": {
                    "$gte": utc_24hrs_ago,
                    "$lte": now_utc
                }
            }
        ]
    }

    logger.info("Fetching data from database")
    results = collection.find(query)
    def safe_get(doc, keys):
        for key in keys:
            if isinstance(doc, dict):
                doc = doc.get(key)
            else:
                return None
        return doc

    # Extract desired fields
    data = []
    for doc in results:
        #print(doc)
        utc_time = safe_get(doc, ['DateTime', 'DateTime'])  # naive UTC datetime
        if utc_time:
            utc_time = UTC.localize(utc_time)
            ist_time = utc_time.astimezone(IST).strftime('%d-%m-%Y %H:%M:%S')
            utc_str = utc_time.strftime('%d-%m-%Y %H:%M:%S')
        else:
            ist_time = 'UNKNOWN'
            utc_str = 'UNKNOWN'

        utc_time2 = safe_get(doc, ['PrimarySorting', 'Timestamp', 'DateTime'])  # naive UTC datetime
        if utc_time2:
            utc_time2 = UTC.localize(utc_time2)
            ist_time2 = utc_time2.astimezone(IST).strftime('%d-%m-%Y %H:%M:%S')
            utc_str2 = utc_time2.strftime('%d-%m-%Y %H:%M:%S')
        else:
            ist_time2 = 'UNKNOWN'
            utc_str2 = 'UNKNOWN'
        row = {
            '_id': str(doc.get('_id')),
            'Barcode': doc.get('Code') or 'UNKNOWN',
            'Scan Timestamp': ist_time,
            'UTC Timestamp': utc_time.strftime('%Y-%m-%d %H:%M:%S') if utc_time else 'UNKNOWN',
            'Primary Sorted Bay': safe_get(doc, ['PrimarySorting', 'ChuteDetails', 'Code']),
            'Primary Scan Timestamp': ist_time2,
            'Primary Status': safe_get(doc, ['PrimarySorting', 'IsRejected']),
            'Primary Rejection Detail': safe_get(doc, ['PrimarySorting', 'RejectionDetail', 'Code']) or 'N/A',
            'Section': safe_get(doc, ['FreightDetails', 'DocketType']) or 'N/A',
            'Secondary Sorted Bin': safe_get(doc, ['SecondarySorting', 'ChuteDetails', 'Code']),
            'Secondary Status': safe_get(doc, ['SecondarySorting', 'IsRejected']),
            'Secondary Rejection Detail': safe_get(doc, ['SecondarySorting', 'RejectionDetail', 'Code']) or 'N/A',
            'Machine Type': safe_get(doc, ['MachineDetails', 'SubType']) or 'UNKNOWN',
            'MachineId': safe_get(doc, ['MachineDetails', 'UId'])
        
        }
        data.append(row)

    # Create DataFrame
    logger.info("Creating DataFrame from fetched data")
    df = pd.DataFrame(data)

    # Export to CSV
    logger.info("Exporting Data from fetched data")
    df.to_csv('Data.csv', index=False)

    logger.info(f"Exported {len(df)} documents to Data.csv")
    if len(df) == 0:
        logger.warning(f"No data found for sorting in the last {LastHours} hours")
        return
    

async def GetAlarmReportsData(client, logger, LastHours, IsTimeRange, start_time_ist, end_time_ist):
    # Replace with your actual MongoDB connection string
    # client = MongoClient("mongodb://nido:Nido%40123@
    db = client["PersistAlarmService"]

    # Access collection
    collection = db["AlarmLogs"]
    UTC = pytz.utc
    IST = pytz.timezone("Asia/Kolkata")
    if IsTimeRange:
        # Assume start_time_ist and end_time_ist are datetime objects in IST
        # Convert them to UTC
        start_time_utc = IST.localize(start_time_ist).astimezone(UTC)
        end_time_utc = IST.localize(end_time_ist).astimezone(UTC)

        now_utc = start_time_utc
        utc_24hrs_ago = end_time_utc
    else:
        now_utc = datetime.now(timezone.utc)
        # Calculate 24 hours ago
        utc_24hrs_ago = now_utc - timedelta(hours=LastHours)

    # Build query to filter documents between now and 24 hours ago
    query = {
        "DateTime.DateTime": {
            "$gte": utc_24hrs_ago,
            "$lte": now_utc
        }
    }
    

    logger.info("Fetching alarm data from database")
    results = collection.find(query)
    def safe_get(doc, keys):
        for key in keys:
            if isinstance(doc, dict):
                doc = doc.get(key)
            else:
                return None
        return doc

    # Extract desired fields
    data = []
    for doc in results:
        #print(doc)
        utc_time = safe_get(doc, ['DateTime', 'DateTime'])  # naive UTC datetime
        if utc_time:
            utc_time = UTC.localize(utc_time)
            ist_time = utc_time.astimezone(IST).strftime('%d-%m-%Y %H:%M:%S')
            utc_str = utc_time.strftime('%d-%m-%Y %H:%M:%S')
        else:
            ist_time = 'UNKNOWN'
            utc_str = 'UNKNOWN'

        
        row = {
            '_id': str(doc.get('_id')),
            'Alarm Name': doc.get('Code') or 'UNKNOWN',
            'Raised Timestamp': ist_time,
            'UTC Timestamp': utc_time.strftime('%Y-%m-%d %H:%M:%S') if utc_time else 'UNKNOWN',
            'Source': doc.get('Source') or 'UNKNOWN',
            'ShortDescription': doc.get('ShortDescription') or 'UNKNOWN',
            'Total Alarm Time(hh:mm:ss.fff)': doc.get('RaisedTime') or 'UNKNOWN',
            'MachineId': safe_get(doc, ['MachineDetails', 'UId'])
        
        }
        data.append(row)

    # Create DataFrame
    logger.info("Creating DataFrame from fetched alarm data")
    df = pd.DataFrame(data)

    # Export to CSV
    logger.info("Exporting Data from fetched alarm data")
    df.to_csv('Alarm.csv', index=False)

    logger.info(f"Exported {len(df)} documents to Alarm.csv")
    if len(df) == 0:
        logger.warning(f"No data found for alarm logs in the last {LastHours} hours")
        return
    

async def GetapiUploaderReportsData(client, logger, LastHours, IsTimeRange, start_time_ist, end_time_ist):
    db = client["ApiCallService"]

    # Access collection
    collection = db["ApiCalls"]
    UTC = pytz.utc
    IST = pytz.timezone("Asia/Kolkata")
    if IsTimeRange:
        # Assume start_time_ist and end_time_ist are datetime objects in IST
        # Convert them to UTC
        start_time_utc = IST.localize(start_time_ist).astimezone(UTC)
        end_time_utc = IST.localize(end_time_ist).astimezone(UTC)

        now_utc = start_time_utc
        utc_24hrs_ago = end_time_utc
    else:
        now_utc = datetime.now(timezone.utc)
        # Calculate 24 hours ago
        utc_24hrs_ago = now_utc - timedelta(hours=LastHours)

    # Build query to filter documents between now and 24 hours ago
    query = {
        "DateTime.DateTime": {
            "$gte": utc_24hrs_ago,
            "$lte": now_utc
        }
    }
    

    logger.info("Fetching api data from database")
    results = collection.find(query)
    def safe_get(doc, keys):
        for key in keys:
            if isinstance(doc, dict):
                doc = doc.get(key)
            else:
                return None
        return doc

    # Extract desired fields
    data = []
    for doc in results:
        #print(doc)
        utc_time = safe_get(doc, ['DateTime', 'DateTime'])  # naive UTC datetime
        if utc_time:
            utc_time = UTC.localize(utc_time)
            ist_time = utc_time.astimezone(IST).strftime('%d-%m-%Y %H:%M:%S')
            utc_str = utc_time.strftime('%d-%m-%Y %H:%M:%S')
        else:
            ist_time = 'UNKNOWN'
            utc_str = 'UNKNOWN'

        
        row = {
            '_id': str(doc.get('_id')),
            'Time': ist_time,
            'UTC Timestamp': utc_time.strftime('%Y-%m-%d %H:%M:%S') if utc_time else 'UNKNOWN',
            'ResponseCode': doc.get('ResponseCode') or 'UNKNOWN',
            'Type2': doc.get('Type') or 'UNKNOWN',
            'MachineId': safe_get(doc, ['MachineDetails', 'UId'])
        
        }
        data.append(row)

    # Create DataFrame
    logger.info("Creating DataFrame from fetched api data")
    df = pd.DataFrame(data)

    # Export to CSV
    logger.info("Exporting Data from fetched api data")
    df.to_csv('ApiCall.csv', index=False)

    logger.info(f"Exported {len(df)} documents to ApiCall.csv")
    if len(df) == 0:
        logger.warning(f"No data found for api in the last {LastHours} hours")
        return
    
async def GetawsUploaderReportsData(client, LastHours, logger, IsTimeRange, start_time_ist, end_time_ist):
    db = client["AWS3CallService"]

    # Access collection
    collection = db["AWS3Calls"]
    UTC = pytz.utc
    IST = pytz.timezone("Asia/Kolkata")
    if IsTimeRange:
        # Assume start_time_ist and end_time_ist are datetime objects in IST
        # Convert them to UTC
        start_time_utc = IST.localize(start_time_ist).astimezone(UTC)
        end_time_utc = IST.localize(end_time_ist).astimezone(UTC)

        now_utc = start_time_utc
        utc_24hrs_ago = end_time_utc
    else:
        now_utc = datetime.now(timezone.utc)
        # Calculate 24 hours ago
        utc_24hrs_ago = now_utc - timedelta(hours=LastHours)

    # Build query to filter documents between now and 24 hours ago
    # query = {
    #     "DateTime.DateTime": {
    #         "$gte": utc_24hrs_ago,
    #         "$lte": now_utc
    #     }
    # }
    start_ticks = datetime_to_ticks(utc_24hrs_ago)
    end_ticks = datetime_to_ticks(now_utc)
    start_ist = ticks_to_datetime(start_ticks).astimezone(pytz.timezone("Asia/Kolkata"))
    logger.info(f"Start IST: {start_ist.strftime('%d-%m-%Y %H:%M:%S')}")

    # Mongo query
    query = {
        "DateTime.0": {
            "$gte": start_ticks,
            "$lte": end_ticks
        }
    }
    logger.info("Fetching data from aws3 database")
    results = collection.find(query)
    def safe_get(doc, keys):
        for key in keys:
            if isinstance(doc, dict):
                doc = doc.get(key)
            else:
                return None
        return doc

    # Extract desired fields
    data = []
    for doc in results:
        #print(doc)
        utc_time = safe_get(doc, ['DateTime', 'DateTime'])  # naive UTC datetime
        if utc_time:
            # Localize as UTC and then convert to IST
            utc_time = UTC.localize(utc_time)
            ist_time = utc_time.astimezone(IST).strftime('%d-%m-%Y %H:%M:%S')
            utc_str = utc_time.strftime('%d-%m-%Y %H:%M:%S')
        else:
            ist_time = 'UNKNOWN'
            utc_str = 'UNKNOWN'
        row = {
            '_id': str(doc.get('_id')),
            'Time': ist_time,
            'UTC Timestamp': utc_time.strftime('%Y-%m-%d %H:%M:%S') if utc_time else 'UNKNOWN',
            'ResponseCode': doc.get('ResponseCode') or 'UNKNOWN',
            'Type2': doc.get('Type') or 'UNKNOWN',
            'MachineId': safe_get(doc, ['MachineDetails', 'UId'])
        }
        data.append(row)

    # Create DataFrame
    logger.info("Creating DataFrame from fetched data for AWS3 data")
    df = pd.DataFrame(data)

    # Export to CSV
    logger.info("Exporting Data from fetched AWS3 data")
    df.to_csv('AWSCall.csv', index=False)

    logger.info(f"Exported {len(df)} documents to AWSCall.csv")
    if len(df) == 0:
        logger.warning(f"No data found for AWS3 in the last {LastHours} hours.")
        return
