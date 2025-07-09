from datetime import datetime, timedelta, timezone
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import pytz
import SendReports
import asyncio
from threading import Thread
import ReportQueries
import PrimaryReport
import CalibReport
import SecondaryReport
import AlarmReport
import UploaderReport


async def GetReportsData(client, logger, LastHours, IsPrimary, IsSecondary, IsCalibration, IsSendReports, IsTimeRange, start_time_ist, end_time_ist, IsAlarm, IsUploader):
    try:
        logger.info("Generating report from Data.csv")
        #export data
        try:
            if IsPrimary or IsSecondary:
                await ReportQueries.GetInfeedReportsData(client, logger, LastHours, IsTimeRange, start_time_ist, end_time_ist)
                df1 = pd.read_csv('Data.csv')
                df1.head()
            
            if IsPrimary:
                await PrimaryReport.GeneratePrimaryReport(df1, logger, LastHours, IsTimeRange, start_time_ist, end_time_ist)
                await PrimaryReport.GeneratePrimaryReport1(df1, logger, LastHours, IsTimeRange, start_time_ist, end_time_ist)
                await PrimaryReport.GeneratePrimaryReport2(df1, logger, LastHours, IsTimeRange, start_time_ist, end_time_ist)
            
            if IsSecondary:
                await SecondaryReport.GenerateSecondaryReport(df1, logger, LastHours, IsTimeRange, start_time_ist, end_time_ist)
        except Exception as e:
            logger.error(f"Error in InfeedReportsData: {e}")
        
        try:
            if IsCalibration:
                await ReportQueries.GenerateCalibrationReport(client, LastHours, logger, IsTimeRange, start_time_ist, end_time_ist)
                df2 = pd.read_csv('CalibData.csv')
                df2.head()
                await CalibReport.GenerateCalibrationReportImage(df2, logger, LastHours, IsTimeRange, start_time_ist, end_time_ist)
                await CalibReport.GenerateCalibrationReportImage2(df2, logger, LastHours, IsTimeRange, start_time_ist, end_time_ist)
        
        except Exception as e:
            logger.error(f"Error in CalibrationReportsData: {e}")
        
        try:
            if IsAlarm:
                await ReportQueries.GetAlarmReportsData(client, logger, LastHours, IsTimeRange, start_time_ist, end_time_ist)
                df3 = pd.read_csv('Alarm.csv')
                df3.head()
                await AlarmReport.GenerateAlarmReport(df3, logger, LastHours, IsTimeRange, start_time_ist, end_time_ist)
        
        except Exception as e:
            logger.error(f"Error in AlarmReportsData: {e}")

        try:
            if IsUploader:
                await ReportQueries.GetapiUploaderReportsData(client, logger, LastHours, IsTimeRange, start_time_ist, end_time_ist)
                await ReportQueries.GetawsUploaderReportsData(client, LastHours, logger, IsTimeRange, start_time_ist, end_time_ist)
                await UploaderReport.GenerateUploaderReport(logger, LastHours, IsTimeRange, start_time_ist, end_time_ist)
        except Exception as e:
            logger.error(f"Error in UploaderReportsData: {e}")
        
        if IsSendReports:
            print("Sending Reports")
            #Thread(target=lambda: asyncio.run(SendReports.Send_Reports(logger))).start()
    
    except Exception as e:
        logger.error(f"Error in GetReportsData: {e}")


    



