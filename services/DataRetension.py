# Delete data as per days
import DataRetensionQueries
import os

async def DataDeleter(logger, ImageDays, ImageFolder):
    try:
        # await DataRetensionQueries.IncomingDeleter(cursor, conn, logger, machineid, IncomingDataDays)
        # await DataRetensionQueries.UploaderDeleter(cursor, conn, logger, machineid, UploaderDataDays)
        # await DataRetensionQueries.SortingDeleter(cursor, conn, logger, machineid, SortingDays)
        # await DataRetensionQueries.BaggingDeleter(cursor, conn, logger, machineid, SortingDays)
        # await DataRetensionQueries.AlarmLogDeleter(cursor, conn, logger, machineid, SortingDays)
        # await DataRetensionQueries.ConfigDeleter(cursor, conn, logger, machineid, SortingDays)
        # await DataRetensionQueries.CalibrationLogsDeleter(cursor, conn, logger, machineid, SortingDays)
        # await DataRetensionQueries.DeviceLogsDeleter(cursor, conn, logger, machineid, SortingDays)
        await DataRetensionQueries.ImageDeleter(logger, ImageDays, ImageFolder)
    except Exception as e:

        logger.error("Error while deleting data: %s", e)
         

async def DeleteReports(logger):
    for filename in os.listdir('.'):
        if filename.lower().endswith(('.png', '.csv')):
            try:
                os.remove(filename)
                print(f"Deleted: {filename}")
            except Exception as e:
                print(f"Error deleting {filename}: {e}")