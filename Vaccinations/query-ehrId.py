import time
from pymongo import MongoClient
from datetime import datetime

# Conexión a MongoDB Atlas
uri = "mongodb+srv://carlosalonso:admin1234@basicinitialtests.0asff.mongodb.net/?retryWrites=true&w=majority&appName=BasicInitialTests"
client = MongoClient(uri)

# Seleccionar la base de datos y colección
db = client["CatSalutCDR"]
collection = db["finalCompositions"]

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

# Ejecutar la consulta y medir el tiempo
start_time = time.time()
results = list(collection.find(query))
elapsed_time = time.time() - start_time

# Mostrar resultados
print(f"Se obtuvieron {len(results)} documentos")
print(f"La consulta tomó {elapsed_time:.2f} segundos")
