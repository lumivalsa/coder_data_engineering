import requests
import psycopg2

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

    # Obtener datos de OpenWeatherMap
    city = 'Madrid'
    key = "3be43396ae24ed9b1a5fb3c88350ceff"
    url = f'http://api.openweathermap.org/data/2.5/weather?q={city}&appid={key}&units=metric'
    res = requests.get(url)
    data = res.json()

    # Insertar datos en Redshift
    cursor.execute("CREATE TABLE IF NOT EXISTS weather_data (city VARCHAR(50), temperature FLOAT, humidity FLOAT)")
    cursor.execute("INSERT INTO weather_data (city, temperature, humidity) VALUES (%s, %s, %s)", (city, data['main']['temp'], data['main']['humidity']))
    conn.commit()
    print("Datos insertados en Redshift!")

except psycopg2.Error as e:
    print("Error al conectar a Redshift:", e)

finally:
    if conn:
        cursor.close()
        conn.close()
        print("Conexión cerrada.")
