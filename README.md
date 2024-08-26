<h1>Análisis de las compañías del S&P 500</h1>

![Badge en Desarollo](https://img.shields.io/badge/status-terminado-green)
![Badge forks](https://img.shields.io/badge/forks-1-blue)

Proyecto para el análisis del comportamiento accionario de las compañías del S&P 500 en el mercado de inversiones durante los dos primeros trimestres de 2024. El proyecto se desarrolló en varias fases, incluyendo la extracción y transformación de la información, análisis estadístico, almacenamiento en una base de datos en SQL Server, generación de dashboards en Power BI y la clusterización de las compañías según indicadores de volatilidad de los precios en el mercado bursátil.

> [!IMPORTANT]
> ## Requisitos:
> Python 3.11.0, librerías: altair, logging, matplotlib, numpy, pandas, pyodbc, scikit-learn, sqlalchemy, yfinance.
> Power BI Desktop.
> SQL Server.

## Estructura del Proyecto
- `data/`: contiene los archivos .csv con los datos y perfiles de las compañías y el dashboard de Power BI.
- `images/`: contiene imágenes utilizadas en el dashboard.
- `scripts/`: contiene los scripts Python para las diferentes fases del proyecto y el script sql para la creación de las tablas en la base de datos.
- `README.md`: Documento explicativo del proyecto.
  
## Utilizando el proyecto
1. Para utilizar la aplicación de análisis clone el código fuente:
```
git clone https://github.com/edwartlc/sp-500-analysis.git
```
2. Instale las librerías:
```
pip install -r requirements.txt
```
3. Configure la conexión a SQL Server en el script de la fase correspondiente.
4. Ejecute los scripts en el orden correspondiente.
    
> [!NOTE]
> Visite mi [Sitio GitHub](https://edwartlc.github.io/sp-500-analysis/)

## Etapas del proyecto
- `Etapa 1:` extracción de perfiles de las compañías de Wikipedia y de los precios de cotización en el mercado bursátil desde Yahoo Finance.
- `Etapa 2:` análisis estadístico de los precios de cierre de las acciones.
- `Etapa 3:` almacenamiento de la información de las compañías en una base de datos SQL Server.
- `Etapa 4:` creación dashboard en Power BI con KPI's, tooltips y bookmarks.
- `Etapa 5:` clusterización de las compañías de acuerdo con los indicadores de volatilidad de los retornos porcentuales diarios.
- `Etapa 6:` publicación en GitHub.
