import pymongo

def createDB(name,client):
    try:
        if name not in client.list_database_names():
            return client[name]
        else:
            print("Database Already exits")
            return client[name]
    except Exception as e:
        print(e)

def createCollection(name,db):
    try:
        if name not in db.list_collections():
            print("Creating the Colletion")
            return db[name]
        else:
            print("Collection Already exits")
            return db[name]
    except Exception as e:
        print(e)

def mongoConnection():
    try:
        client = pymongo.MongoClient("localhost", 27017, maxPoolSize=50)
        return client
    except Exception as e:
        print(e)