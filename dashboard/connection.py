import pymongo

def get_mongodb_connection():
    client = pymongo.MongoClient(
        "mongodb://root:admin@172.17.0.1:27017",
        replicaSet="replicaset",
        compressors="zstd",
        serverSelectionTimeoutMS=1000,
    )
    return client
