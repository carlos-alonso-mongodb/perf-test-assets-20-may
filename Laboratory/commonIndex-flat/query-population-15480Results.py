import time
from helper import execute_aggregation
from datetime import datetime

# Definir pipeline de agregación
#pipeline = [
#    {
#        "$search": {
#            "index": "commonIndex-4p",
#            "returnStoredSource": True,
#            "compound": {
#                "filter": [
#                    {
#                        "embeddedDocument": {
#                            "path": "sn",
#                            "operator": {
#                                "compound": {
#                                    "filter": [
#                                        {"equals": {"path": "sn.d.ani", "value": 240000}},
#                                        {"equals": {"path": "sn.as", "value": "18.10000.20000.30000.19"}},
#                                        {"in": {"path": "sn.d.v.df.cs", "value": [
#                                            "718-7", "62461000122102", "16676-9", "10851-4", "LL1937-3",
#                                            "88186-2", "77955-3", "41216-3", "7894-9", "16925-0", "38392-7",
#                                            "31870-9", "74408-6", "17327-8", "98080-5", "82301-3", "31946-7",
#                                            "31947-5", "21834-2", "21835-9", "31966-5", "21503-8", "60270-6",
#                                            "33689-1", "82731-1"
#                                        ]}}
#                                    ]
#                                }
#                            }
#                        }
#                    },
#                    {
#                        "embeddedDocument": {
#                            "path": "sn",
#                            "operator": {
#                                "compound": {
#                                    "filter": [
#                                        {"equals": {"path": "sn.d.ani", "value": 10000}},
#                                        {"equals": {"path": "sn.as", "value": "18.10000.20000.30000.19"}},
#                                        {"equals": {"path": "sn.d.v.v", "value": "Negatiu"}}
#                                    ]
#                                }
#                            }
#                        }
#                    },
#                    {
#                        "embeddedDocument": {
#                            "path": "sn",
#                            "operator": {
#                                "compound": {
#                                    "filter": [
#                                        {"equals": {"path": "sn.d.T", "value": "C"}},
#                                        {"range": {
#                                            "path": "sn.d.cx.st.v",
#                                            "gte": datetime.fromisoformat("2024-06-19T00:00:00")
#                                        }}
#                                    ]
#                                }
#                            }
#                        }
#                    },
#                    {
#                        "embeddedDocument": {
#                            "path": "sn",
#                            "operator": {
#                                "compound": {
#                                    "filter": [
#                                        {"equals": {"path": "sn.d.ani", "value": 10000}},
#                                        {"equals": {"path": "sn.as", "value": "10000.3"}},
#                                        {"range": {
#                                            "path": "sn.d.v.v",
#                                            "lte": datetime.fromisoformat("2021-01-01T08:04:47")
#                                        }}
#                                    ]
#                                }
#                            }
#                        }
#                    },
#                    {
#                        "embeddedDocument": {
#                            "path": "sn",
#                            "operator": {
#                                "compound": {
#                                    "filter": [
#                                        {"equals": {"path": "sn.d.ani", "value": 110000}},
#                                        {"equals": {"path": "sn.as", "value": "10000.3.50000"}},
#                                        {"equals": {"path": "sn.d.v.df.cs", "value": "H43001903"}}
#                                    ]
#                                }
#                            }
#                        }
#                    }
#                ]
#            }
#        }
#    },
#    {"$limit": 100}
#]

filters = [
                    {
                        "embeddedDocument": {
                            "path": "sn",
                            "operator": {
                                "compound": {
                                    "filter": [
                                        {"equals": {"path": "sn.d.ani", "value": 240000}},
                                        {"equals": {"path": "sn.as", "value": "16.17.18.10000.20000.30000.19"}},
                                        {"in": {"path": "sn.d.v.df.cs", "value": [
                                            "718-7", "62461000122102", "16676-9", "10851-4", "LL1937-3",
                                            "88186-2", "77955-3", "41216-3", "7894-9", "16925-0", "38392-7",
                                            "31870-9", "74408-6", "17327-8", "98080-5", "82301-3", "31946-7",
                                            "31947-5", "21834-2", "21835-9", "31966-5", "21503-8", "60270-6",
                                            "33689-1", "82731-1"
                                        ]}}
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
                                        {"equals": {"path": "sn.as", "value": "16.17.18.10000.20000.30000.19"}},
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
                                        {"range": {
                                            "path": "sn.d.cx.st.v",
                                            "gte": datetime.fromisoformat("2024-05-01")
                                        }}
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
                                        {"equals": {"path": "sn.as", "value": "16.10000.3"}},
                                        {"range": {
                                            "path": "sn.d.v.v",
                                            "lte": datetime.fromisoformat("2024-01-01T08:04:47")
                                        }}
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
                                        {"equals": {"path": "sn.as", "value": "16.10000.3.50000"}},
                                        {"equals": {"path": "sn.d.v.df.cs", "value": "H43001903"}}
                                    ]
                                }
                            }
                        }
                    }
                ]

pipeline = [
    {
        "$search": {
            "index": "commonIndex-4p",
            "returnStoredSource": True,
            "concurrent": True,
            "compound": {
                "filter": filters
            }
        }
    },
    {"$limit": 100}
]

results = execute_aggregation("CatSalutCDR.finalSearchFlat", pipeline)



