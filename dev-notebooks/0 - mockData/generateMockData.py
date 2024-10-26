# Databricks notebook source
import random
import json
from faker import Faker
from datetime import datetime

# Inicializa o gerador de dados fictícios
fake = Faker()

# Definindo intervalo de tempo
start_date = datetime(2024, 8, 1, 0,)
end_date = datetime(2024, 8, 1, 23)

def generate_random_data():
    """Gera um dicionário com dados aleatórios no formato JSON especificado."""
    return {
        "imei": str(fake.random_number(digits=15, fix_len=True)),
        "mac": ':'.join([f"{random.randint(0, 255):02X}" for _ in range(6)]),
        "rede": random.choice(["VIVO", "TIM", "CLARO", "OI"]),
        "client_ip": fake.ipv4(),
        "latitude": str(fake.latitude()),
        "logintude": str(fake.longitude()),  # Corrigido para "longitude"
        "cpf": str(fake.random_number(digits=11, fix_len=True)),
        "senha": fake.password(length=6, special_chars=False, digits=True, upper_case=True, lower_case=True),
        "transaction": random.choice(["true", "false"]),
        "api": "login-authentication",
        "endpoint": "v1/login",
        "os": random.choice(["ANDROID", "iOS"]),
        "os_version": str(random.randint(1, 30)),
        "app_version": random.choice(["2.0.1", "2.0.2", "2.0.3"]),
        "erro": random.choice(["0", "INCORRECT_PASS", "USER_NOT_FOUND"]),
        "timestamp": str(fake.unix_time(start_datetime=start_date, end_datetime=end_date)*1000)
    }

# Gera um array com 100 dados
data_array = [generate_random_data() for _ in range(100)]

# Salva o array em um arquivo JSON
with open('data.json', 'w') as f:
    json.dump(data_array, f, indent=4)

# COMMAND ----------

