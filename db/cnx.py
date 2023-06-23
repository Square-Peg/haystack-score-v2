import os
import sqlalchemy
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_PORT = os.getenv('POSTGRES_PORT')
POSTGRES_DBNAME = os.getenv('POSTGRES_DBNAME')

# Connection engine
postgres_str = 'postgresql://{username}:{password}@{host}:{port}/{dbname}'.format(
    username=POSTGRES_USER,
    password=POSTGRES_PASSWORD,
    host=POSTGRES_HOST,
    port=POSTGRES_PORT,
    dbname=POSTGRES_DBNAME,
)
# Create the connection
Cnx = sqlalchemy.create_engine(postgres_str)
