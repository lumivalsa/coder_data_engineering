import requests
import psycopg2
from datetime import datetime, timedelta

# Conexión a Redshift
try:
    conn = psycopg2.connect(
        dbname='data-engineer-database',
        user='lumivalsa_coderhouse',
        password='5gAuNt90Pn',
        host='data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com',
        port='5439'
    )
    cursor = conn.cursor()
    print("Conexión exitosa a Redshift!")

    # Lista de ciudades europeas
    cities = ['Madrid', 'Paris', 'Berlin', 'Rome', 'London', 'Budapest']  

    # Fechas desde enero hasta noviembre de 2023
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 11, 30)
    date_range = [start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)]

    # Obtener datos de OpenWeatherMap para cada ciudad y fecha
    for city in cities:
        for date in date_range:
            key = "3be43396ae24ed9b1a5fb3c88350ceff"
            url = f'http://api.openweathermap.org/data/2.5/weather?q={city}&appid={key}&units=metric'
            res = requests.get(url)
            data = res.json()

            # Verificar si los datos ya existen en Redshift
            cursor.execute(
                "SELECT COUNT(*) FROM weather_data WHERE city = %s AND date = %s",
                (city, date.date())
            )
            existing_data = cursor.fetchone()[0]

            # Insertar datos solo si no existen ya en Redshift
            if existing_data == 0:
                cursor.execute(
                    "INSERT INTO weather_data (city, temperature, humidity, date) VALUES (%s, %s, %s, %s)",
                    (city, data['main']['temp'], data['main']['humidity'], date.date())
                )
                conn.commit()
                print(f"Dato de {city} para {date.date()} insertado en Redshift!")
            else:
                print(f"Dato de {city} para {date.date()} ya existe en Redshift. No se insertó.")

    print("Datos insertados en Redshift!")

except psycopg2.Error as e:
    print("Error al conectar a Redshift:", e)

finally:
    if conn:
        cursor.close()
        conn.close()
        print("Conexión cerrada.")
