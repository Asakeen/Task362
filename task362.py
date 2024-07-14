from pyspark.sql import SparkSession
from pyspark.sql.functions import rand
from datetime import datetime, timedelta
import random
from pyspark.sql import Row

# Создаем сессию Spark
spark = SparkSession.builder.appName("SyntheticDataGeneration").getOrCreate()

# Создание списка товаров
possible_products = ["Товар1", "Товар2", "Товар3", "Товар4", "Товар5"]

# Генерация случайных значений для Количества и Цены
num_values = 1000
quantity_values = [random.randint(1, 100) for _ in range(num_values)]
price_values = [round(random.uniform(1, 200), 2) for _ in range(num_values)]

# Генерация случайных дат в пределах последнего года
end_date = datetime.now()
start_date = end_date - timedelta(days=365)
date_values = [start_date + timedelta(days=random.randint(0, 365)) for _ in range(num_values)]

# Генерация случайных идентификаторов пользователей
user_id_values = [random.randint(1, 9999) for _ in range(num_values)]

# Создание RDD из значений

rows = [Row(Дата=date, UserID=user_id, Продукт=product, Количество=quantity, Цена=price)
        for date, user_id, product, quantity, price in zip(date_values, user_id_values, possible_products, quantity_values, price_values)]

# Создание DataFrame
df = spark.createDataFrame(rows)
df.show()

# Сохранение DataFrame в формате CSV
df.write.csv("GeneratedDF_362", header=True, mode="overwrite")

spark.stop()
