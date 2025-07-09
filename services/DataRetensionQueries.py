import os
import time


async def ImageDeleter(logger, ImageDays, ImageFolder):
    try:
        logger.info(f"Deleting Image Data Older Than {ImageDays} days")
        now = time.time()
        time_threshold = ImageDays * 86400

        for root, dirs, files in os.walk(ImageFolder):
            for filename in files:
                file_path = os.path.join(root, filename)
                try:
                    file_age = now - os.path.getmtime(file_path)
                    if file_age > time_threshold:
                        os.remove(file_path)
                        logger.info(f"Deleted file: {repr(file_path)}")
                        #print(f"Deleted file: {repr(file_path)}")
                except Exception as file_error:
                    logger.warning(f"Error deleting file {repr(file_path)}: {file_error}")

    except Exception as e:
        logger.error("Error while deleting image data: %s", e)


    





