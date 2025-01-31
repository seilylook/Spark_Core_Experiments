MONGODB_CONFIG = {
    "CONN_URI": "mongodb://testuser:testpass@mongodb:27017/test.students?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+2.3.4",
    "DATABASE": "test",
    "COLLECTION": "students",
}

SPARK_PACKAGES = ["org.mongodb.spark:mongo-spark-connector_2.12:10.4.0"]
