from pymongo import MongoClient

def dbconnector(connectionstring, logger):
    logger.info("Connecting to database...")
    try:
        client = MongoClient(f"{connectionstring}")
        logger.info("Database Connected Successfully...")
        return client
    except Exception as e:
        logger.error("Error connecting to database:", e)
        return None