from helper import get_connection, get_logger

logger = get_logger()
conn = get_connection()

finalSearch = conn["CatSalutCDR"]["finalSearch"]
finalSearchFlat = conn["CatSalutCDR"]["finalSearchFlat"]

bulk = []
for doc in finalSearch.find():
    for s in doc["sn"]:
        if "a" not in s:
            continue
        s["as"] = ".".join(str(a) for a in s["a"])

    bulk.append(doc)
    if len(bulk) >= 1000:
        finalSearchFlat.insert_many(bulk)
        bulk = []
        break

   
if len(bulk) > 0:
    finalSearchFlat.insert_many(bulk)
    bulk = []
