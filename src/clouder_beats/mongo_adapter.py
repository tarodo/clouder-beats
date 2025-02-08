import logging

from pymongo import MongoClient, UpdateOne, errors
from pymongo.synchronous.database import Database

from src.clouder_beats.config import settings

logger = logging.getLogger("mongo")


def get_mongo_conn() -> Database:
    """Connects to MongoDB and returns the database object."""
    mongo_url = settings.mongo_url
    mongo_db = settings.mongo_db
    try:
        client = MongoClient(mongo_url, serverSelectionTimeoutMS=5000)
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB :: {e}")
        raise

    try:
        client.admin.command("ping")
        return client[mongo_db]
    except Exception as e:
        logger.error(f"Failed to check the MongoDB database. :: {e}")
        raise


def save_data_mongo_by_id(
    data, collection_name: str, key_fields: list = None, db: MongoClient = None
) -> tuple[int, int]:
    """Save data to MongoDB by id in collection"""
    logger.info(f"Save data : {collection_name} : count = {len(data)} :: Start")
    close_connection = False
    if db is None:
        db = get_mongo_conn()
        close_connection = True
    if not key_fields:
        key_fields = [
            "id",
        ]

    operations = []
    for item in data:
        item_keys = {field: item[field] for field in key_fields}
        operations.append(UpdateOne(item_keys, {"$set": item}, upsert=True))

    if not operations:
        logger.info(f"Save data : {collection_name} : count = 0 :: Done")
        return 0, 0
    try:
        result = db[collection_name].bulk_write(operations)
        inserted = result.upserted_count
        updated = result.matched_count
        logger.info(f"Save data : {collection_name} : {inserted=} : {updated=} :: Done")
        return inserted, updated
    except errors.PyMongoError as e:
        logger.error(f"MongoDB error while saving to {collection_name}: {e}")
        return 0, 0
    finally:
        if close_connection:
            db.client.close()


def get_data(
    collection: str,
    query_filters: dict = None,
    query_fields: list = None,
    db: MongoClient = None,
) -> list:
    """Get data from MongoDB"""
    logger.info(f"Get data : {collection} with filters : {query_filters} :: Start")
    close_connection = False
    if db is None:
        db = get_mongo_conn()
        close_connection = True
    filters = {}
    filters.update(query_filters) if query_filters else filters
    fields = {"_id": 0}
    fields.update({field: 1 for field in query_fields}) if query_fields else fields
    try:
        result = list(
            db[collection].find(
                filters,
                fields,
            )
        )
        return result
    finally:
        if close_connection:
            db.client.close()
