import time
from pymongo import MongoClient

# MongoDB Atlas connection URI
uri = "mongodb+srv://carlosalonso:admin1234@basicinitialtests.0asff.mongodb.net/?retryWrites=true&w=majority&appName=BasicInitialTests"
client = MongoClient(uri)

# Database and collection
db = client["CatSalutCDR"]
collection = db["finalSearch"]

# Aggregation pipeline
pipeline = [
    {
        "$search": {
            "index": "commonIndex-4p",
            "returnStoredSource": True,
            "compound": {
                "filter": [
                    {
                        "embeddedDocument": {
                            "path": "sn",
                            "operator": {
                                "compound": {
                                    "filter": [
                                        {"equals": {"path": "sn.d.ani", "value": 10000}},
                                        {"in": {"path": "sn.a", "value": [18, 10000, 20000, 30000, 19]}},
                                        {"equals": {"path": "sn.d.v.v", "value": "Negatiu"}}
                                    ]
                                }
                            }
                        }
                    },
                    {
                        "embeddedDocument": {
                            "path": "sn",
                            "operator": {
                                "compound": {
                                    "filter": [
                                        {"equals": {"path": "sn.d.T", "value": "C"}},
                                        {"range": {"path": "sn.d.cx.st.v", "gte": "2022-01-19T00:00:00.000Z"}}
                                    ]
                                }
                            }
                        }
                    },
                    {
                        "embeddedDocument": {
                            "path": "sn",
                            "operator": {
                                "compound": {
                                    "filter": [
                                        {"equals": {"path": "sn.d.ani", "value": 10000}},
                                        {"in": {"path": "sn.a", "value": [10000, 3]}},
                                        {"range": {"path": "sn.d.v.v", "lte": "2022-01-01T08:04:47.000Z"}}
                                    ]
                                }
                            }
                        }
                    },
                    {
                        "embeddedDocument": {
                            "path": "sn",
                            "operator": {
                                "compound": {
                                    "filter": [
                                        {"equals": {"path": "sn.d.ani", "value": 110000}},
                                        {"in": {"path": "sn.a", "value": [10000, 3, 50000]}},
                                        {"equals": {"path": "sn.d.v.df.cs", "value": "H43001903"}}
                                    ]
                                }
                            }
                        }
                    },
                    {
                        "embeddedDocument": {
                            "path": "sn",
                            "operator": {
                                "compound": {
                                    "filter": [
                                        {"equals": {"path": "sn.d.ani", "value": 240000}},
                                        {"in": {"path": "sn.a", "value": [18, 10000, 20000, 30000, 19]}},
                                        {"in": {
                                            "path": "sn.d.v.df.cs",
                                            "value": [
                                                "718-7", "62461000122102", "16676-9", "10851-4", "LL1937-3",
                                                "88186-2", "77955-3", "41216-3", "7894-9", "16925-0", "38392-7",
                                                "31870-9", "74408-6", "17327-8", "98080-5", "82301-3", "31946-7",
                                                "31947-5", "21834-2", "21835-9", "31966-5", "21503-8", "60270-6",
                                                "33689-1", "82731-1"
                                            ]
                                        }}
                                    ]
                                }
                            }
                        }
                    }
                ]
            }
        }
    },
    {"$limit": 100}
]

# Run and time the query
start_time = time.time()
results = list(collection.aggregate(pipeline))
elapsed_time = time.time() - start_time

# Output result count and query time
print(f"Returned {len(results)} documents")
print(f"$search query took {elapsed_time:.2f} seconds")
