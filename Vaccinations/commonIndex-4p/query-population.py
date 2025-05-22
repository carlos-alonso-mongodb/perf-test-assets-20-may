import time
from pymongo import MongoClient
from datetime import datetime

# Conexión a MongoDB Atlas
uri = "mongodb+srv://carlosalonso:admin1234@basicinitialtests.0asff.mongodb.net/?retryWrites=true&w=majority&appName=BasicInitialTests"
client = MongoClient(uri)

# Base de datos y colección
db = client["CatSalutCDR"]
collection = db["finalSearch"]

# Pipeline con $search
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
                                        {"equals": {"path": "sn.d.T", "value": "A"}},
                                        {"equals": {"path": "sn.d.ani", "value": 13}},
                                        # ⚠️ usar 'in' en lugar de múltiples 'equals' para sn.a
                                        {"in": {"path": "sn.a", "value": [11, 12]}},
                                        {"range": {
                                            "path": "sn.d.time.v",
                                            "gte": datetime.fromisoformat("2000-04-13T07:54:16.345"),
                                            "lte": datetime.fromisoformat("2025-04-13T07:54:16.345")
                                        }},
                                        {"equals": {"path": "sn.d.op.pf.ids.id", "value": "P..A"}}
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
                                        {"equals": {"path": "sn.d.ani", "value": 140000}},
                                        # ⚠️ combinar todos los valores de sn.a en una sola expresión
                                        {"in": {"path": "sn.a", "value": [70000, 3, 11]}},
                                        {"equals": {"path": "sn.d.v.df.cs", "value": "E08001460"}}
                                    ]
                                }
                            }
                        }
                    }
                ]
            }
        }
    }
]

# Ejecutar y medir tiempo
start_time = time.time()
results = list(collection.aggregate(pipeline))
elapsed_time = time.time() - start_time

# Mostrar resultados
print(f"Se obtuvieron {len(results)} documentos")
print(f"La consulta tomó {elapsed_time:.2f} segundos")
