import time
from pymongo import MongoClient

# Connect to MongoDB Atlas
uri = "mongodb+srv://carlosalonso:admin1234@basicinitialtests.0asff.mongodb.net/?retryWrites=true&w=majority&appName=BasicInitialTests"
client = MongoClient(uri)

# Access the database and collection
db = client["CatSalutCDR"]
collection = db["finalSearch"]

# Build the query
query = {
    "ehr_id": "616a330c-9b0b-4a2e-c60f-8a56d871cc5a~r10",
    "cn": {
        "$all": [
            {
                "$elemMatch": {
                    "d.ani": 240000,
                    "a": {"$all": [20, 10000, 20000, 30000, 21]},
                    "d.v.df.cs": {
                        "$in": [
                            "62461000122102", "33695-8", "16676-9", "10851-4",
                            "LL1937-3", "88186-2", "77955-3", "41216-3",
                            "7894-9", "16925-0", "38392-7", "31870-9", "74408-6",
                            "17327-8", "98080-5", "82301-3", "31946-7", "31947-5",
                            "21834-2", "21835-9", "31966-5", "21503-8", "60270-6",
                            "33689-1", "82731-1"
                        ]
                    }
                }
            },
            {
                "$elemMatch": {
                    "d.ani": 10000,
                    "a": {"$all": [20, 10000, 20000, 30000, 21]},
                    "d.v.v": "Indetectable"
                }
            },
            {
                "$elemMatch": {
                    "d.cx.st.v": {
                        "$gte": {
                            "$date": "2018-04-14T19:04:47+11:00"
                        }
                    }
                }
            },
            {
                "$elemMatch": {
                    "d.ani": 110000,
                    "a": {"$all": [10000, 3, 50000]},
                    "d.v.df.cs": "H43001903"
                }
            }
        ]
    }
}

# Execute and time the query
start_time = time.time()
results = list(collection.find(query))
end_time = time.time()

# Output result and timing
print(f"Found {len(results)} documents")
print(f"Query took {end_time - start_time:.2f} seconds")
