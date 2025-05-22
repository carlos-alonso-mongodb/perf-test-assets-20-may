from datetime import datetime
from helper import execute_query

# Construir la consulta
query = {
    "ehr_id": "3504dd8f-996c-450a-a899-66134ead3846~r3",
    "cn": {
        "$all": [
            {
                "$elemMatch": {
                    "d.T": "A",
                    "d.ani": 13,
                    "a": {"$all": [11, 12]},
                    "d.time.v": {
                        "$gte": datetime.fromisoformat("2000-04-13T07:54:16.345"),
                        "$lte": datetime.fromisoformat("2025-04-13T07:54:16.345")
                    },
                    "d.op.pf.ids.id": "P..A"
                }
            },
            {
                "$elemMatch": {
                    "d.ani": 140000,
                    "a": {"$all": [70000, 3, 11]},
                    "d.v.df.cs": "E08001460"
                }
            }
        ]
    }
}

execute_query("CatSalutCDR.finalCompositions", query)
