'''
    Proyecto ETL S&P 500
        
    @author Edwart Llanos
    Agosto de 2024
'''

from sqlalchemy import create_engine
from credentials import access
import logging
import os
import pandas as pd
import yfinance as yf
import pyodbc

# Definición de variables
url = "https://es.wikipedia.org/wiki/Anexo:Compa%C3%B1%C3%ADas_del_S%26P_500"
start_date = '2024-01-01'
end_date = '2024-06-30'
table1 = 'CompanyProfiles'
table2 = 'Companies'
server = ''
database = ''
username = ''
password = ''
driver = 'ODBC Driver 17 for SQL Server'
# Configuración de la conexión a SQL Server
engine = create_engine(f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver}')

# Creación de directorios para guardar logs y datos
logs_dir = './logs'
data_dir = './data'

if not os.path.exists(logs_dir):
    os.makedirs(logs_dir)

if not os.path.exists(data_dir):
    os.makedirs(data_dir)

# Configuración de logging
log_filename = os.path.join(logs_dir, 'etl_process.log')

logging.basicConfig(
    encoding = 'utf-8',
    format = '%(asctime)s - %(levelname)s - %(message)s',
    level = logging.INFO,
    handlers = [
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)

# Función para el proceso de extracción de datos
def extract_data(url, start_date, end_date):
    try:
        logging.info('Extrayendo datos de compañías del S&P 500')
        SandP500_df = pd.read_html(url)[0]
        ticker_list = SandP500_df['Símbolo'].values.tolist()
        logging.info('Datos de compañías del S&P 500 extraídos')
        logging.info(f'Extrayendo datos precios de cierre para S&P 500 desde {start_date} hasta {end_date}')
        closing_prices = yf.download(ticker_list, start=start_date, end=end_date)['Close']
        logging.info('Datos precios de cierre extraídos para compañías del S&P 500')
        extracted_data = [SandP500_df, closing_prices]
        return extracted_data
    except Exception as e:
        logging.error(f'Error extrayendo datos: {e}')
        return None

# Función para el proceso de transformación de datos
def transform_data(extracted_data):
    try:
        logging.info('Transformando datos S&P 500')
        SandP500 = extracted_data[0]
        SandP500.drop(columns=['Presentación ante la SEC'], inplace=True)
        SandP500.rename({'Símbolo': 'Symbol', 'Seguridad': 'Company', 'Sector GICS': 'Sector', 'Sub-industria GICS': 'SubSector', 'Ubicación de la sede': 'HeadQuarter', 
            'Fecha de incorporación': 'IncorporationDate', 'Clave de índice central': 'CentralIndexKey', 'Fundada': 'FundationYear'}, axis="columns", inplace=True)
        SandP500['IncorporationDate'] = SandP500['IncorporationDate'].fillna('1900-01-01')
        SandP500['IncorporationDate'] = SandP500['IncorporationDate'].replace('1983-11-30 (1957-03-04)','1983-11-30')
        SandP500['IncorporationDate'] = SandP500['IncorporationDate'].replace('2009','2009-01-01')
        SandP500['IncorporationDate'] = pd.to_datetime(SandP500['IncorporationDate'])
        logging.info('Datos S&P 500 transformados exitosamente')
        logging.info('Transformando datos precios de cierre S&P 500')
        closing_prices = extracted_data[1]
        closing_prices = closing_prices.stack().reset_index(level=["Ticker"]).sort_values(["Ticker", "Date"])
        closing_prices.reset_index(level = ["Date"], inplace=True)
        closing_prices.rename({'Ticker': 'Symbol', 0: "ClosePrice"}, axis="columns", inplace=True)
        logging.info('Datos precios de cierre transformados exitosamente')
        transformed_data = [SandP500, closing_prices]
        return transformed_data
    except Exception as e:
        logging.error(f'Error transformando datos: {e}')
        return None

# Función para el proceso de carga de datos
def load_data_to_csv(transformed_data):
    try:
        filename1 = os.path.join(data_dir, 'companies-sp-500.csv')
        logging.info(f'Guardando datos transformados S&P 500 en {filename1}')
        SandP500 = pd.DataFrame(transformed_data[0])
        SandP500.to_csv(filename1, encoding="utf-16", index=False)
        logging.info('Datos S&P 500 guardados exitosamente')
        filename2 = os.path.join(data_dir, 'closing-price-sp-500.csv')
        logging.info(f'Guardando datos transformados closing prices S&P 500 en {filename2}')
        closing_price = pd.DataFrame(transformed_data[1])
        closing_price.to_csv(filename2, index=False)
        logging.info('Datos closing price S&P 500 guardados exitosamente')
        loaded_data_to_csv = [SandP500, closing_price]
        return loaded_data_to_csv
    except Exception as e:
        logging.error(f'Error guardando datos: {e}')
        return None

# Función para la carga de datos en SQL Server
def load_data_to_sql(engine, df, table):
    try:
        logging.info(f'Insertando datos en la tabla {table}')
        df.to_sql(table, con=engine, index=False, if_exists='append')
        logging.info(f'Datos insertados con éxito en la tabla {table}')
    except Exception as e:
        logging.error(f'Error insertando datos en la tabla {table}: {e}')
        return None

# Función para integración de las funciones de ETL
def etl_process(url, start_date, end_date, engine, table1, table2):
    extracted_data = extract_data(url, start_date, end_date)
    if extracted_data is not None:
        transformed_data = transform_data(extracted_data)
        if transformed_data is not None:
            loaded_data_to_csv = load_data_to_csv(transformed_data)
            if loaded_data_to_csv is not None:
                # Insertar datos en SQL Server
                load_data_to_sql(engine, loaded_data_to_csv[0], table1)
                load_data_to_sql(engine, loaded_data_to_csv[1], table2)
                return transformed_data
    return None

if __name__ == "__main__":
    etl_process(url, start_date, end_date, engine, table1, table2)