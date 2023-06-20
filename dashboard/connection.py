import pymongo

def get_mongodb_connection():
    client = pymongo.MongoClient(
        "mongodb://root:admin@mongodb:27017",
        replicaSet="replicaset",
        compressors="zstd",
    )
    return client