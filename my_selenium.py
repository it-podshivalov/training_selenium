from selenium import webdriver
from bs4 import BeautifulSoup
from datetime import datetime
import databases
from sqlalchemy import Table, MetaData, Column, Integer, DateTime, VARCHAR, create_engine
import os
import time
import asyncio
from fake_useragent import UserAgent
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

DB_USER = "postgres"
DB_NAME = "laptops"
DB_PASSWORD = "postgrespw"
DB_HOST = "127.0.0.1"

# создание Docker контейнера с postgres 
os.system(f"docker run --name My_Postgres -p 5432:5432 -e POSTGRES_USER={DB_USER} -e POSTGRES_PASSWORD={DB_PASSWORD} -e POSTGRES_DB={DB_NAME} -d postgres:latest")
time.sleep(5)

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:5432/{DB_NAME}"

database = databases.Database(DATABASE_URL)
metadata = MetaData()

laptops = Table(
    "laptops",
    metadata,
    Column("ID", Integer, primary_key=True, unique=True, autoincrement=True),
    Column("URL", VARCHAR),
    Column("Visited_at", DateTime),
    Column("Name", VARCHAR),
    Column("Proccessor", VARCHAR),
    Column("RAM_size", Integer),
    Column("SSD_size", Integer),
    Column("Price", Integer),
    Column("Rank", Integer)
)

# engine - пул соединений к БД
engine = create_engine(DATABASE_URL)
time.sleep(2)

# создание таблиц
metadata.create_all(engine)

ua = UserAgent()


async def get_data_e2e4():
    url = 'https://novosibirsk.e2e4online.ru/catalog/noutbuki-42/'
    user_agent = ua.random
    print(user_agent)
    options = Options()
    options.add_argument("--headless")
    #options.add_argument("--no-sandbox")
    #options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-extensions")
    options.add_argument(f'user-agent={user_agent}')
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    await database.connect()
    for page in range(1, 16, 1):
        page_url = url + '?page=' + str(page)
        driver.get(page_url)
        soup = BeautifulSoup(driver.page_source, "html.parser")
        cards = soup.find_all(class_='block-offer-item subcategory-new-offers__item-block')
        for obj in cards:
            obj_arrib = obj.find_all(class_='block-offer-item__description lg-and-up')[0].text
            if 'HDD' in obj_arrib:
                continue
            price = obj.find_all(class_='price-block block-offer-item__price _default')[0].text
            Name = obj_arrib.split(', ')[0]
            Obj_url = (url.split('/catalog/noutbuki-42/'))[0] + obj.find_all(class_='block-offer-item__head-info')[0].next.attrs['href']
            Visited_at = datetime.now()
            Proccessor = obj_arrib.split(', ')[1]
            RAM_size = int(((obj_arrib.split(', ')[2]).split('Gb'))[0])
            try:
                Size_SDD = int(((obj_arrib.split(', ')[3]).split('Gb'))[0])
            except:
                Size_SDD = int(((obj_arrib.split(', ')[3]).split('Tb'))[0]) * 1024    
            try:
                Price = int((price.split('\xa0'))[0] + (price.split('\xa0'))[1])
            except:
                continue    
            Rank = int((RAM_size * 30 + Size_SDD * 0.5) * 1 / (Price / 700))

            query = laptops.insert().values(URL=Obj_url, Visited_at=Visited_at, Name=Name, Proccessor=Proccessor, RAM_size=RAM_size, SSD_size=Size_SDD, Price=Price, Rank=Rank)
            await database.fetch_all(query)
    await database.disconnect()                                


async def get_data_citilink():
    url = 'https://www.citilink.ru/catalog/noutbuki/'
    await database.connect()
    for page in range(1, 14, 1):
        user_agent = ua.random
        print(user_agent)
        options = Options()
        options.add_argument("--headless")
        # options.add_argument("--no-sandbox")
        # options.add_argument("--disable-dev-shm-usage")
        options.add_argument(f'user-agent={user_agent}')
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        page_url = url + '?p=' + str(page)
        try:
            driver.get(page_url)
        except:
            try:
                driver.get(page_url)
            except:
                continue                                                    
        soup = BeautifulSoup(driver.page_source, "html.parser")
        cards = soup.find_all(class_='e1ex4k9s0 app-catalog-1bogmvw e1loosed0')
        for obj in cards:
            try:
                obj_arrib = obj.find_all(class_='e1lqnfu30 app-catalog-1x5tdac ejdpak00')[0].next.attrs['title']
            except:
                continue  
            if '1000' in obj_arrib:
                continue
            try:
                Price = int(obj.find_all(class_='app-catalog-0 eb8dq160')[0].attrs['data-meta-price'])
            except:
                continue                
            Obj_url = url.split('/catalog/noutbuki/')[0] + obj.find_all(class_='e1lqnfu30 app-catalog-1x5tdac ejdpak00')[0].next.attrs['href']
            Visited_at = datetime.now()
            Name = obj_arrib.split(', ')[0]
            Proccessor = obj_arrib.split(', ')[3]
            if 'ГБ' in Proccessor:
                Proccessor = obj_arrib.split(', ')[2]
                try:
                    RAM_size = int(obj_arrib.split(', ')[3].split('ГБ')[0])
                except:
                    continue
                try:
                    Size_SDD = int(obj_arrib.split(', ')[4].split('ГБ')[0])
                except:
                    try:
                        Size_SDD = int(obj_arrib.split(', ')[4].split('ТБ')[0]) * 1024
                    except:
                        continue
            else:                            
                try:
                    RAM_size = int(obj_arrib.split(', ')[4].split('ГБ')[0])
                except:
                    continue
                try:
                    Size_SDD = int(obj_arrib.split(', ')[5].split('ГБ')[0])
                except:
                    try:
                        Size_SDD = int(obj_arrib.split(', ')[5].split('ТБ')[0]) * 1024
                    except:
                        continue                    

            Rank = int((RAM_size * 30 + Size_SDD * 0.5) * 1 / (Price / 700))
            query = laptops.insert().values(URL=Obj_url, Visited_at=Visited_at, Name=Name, Proccessor=Proccessor, RAM_size=RAM_size, SSD_size=Size_SDD, Price=Price, Rank=Rank)
            await database.fetch_all(query)
    await database.disconnect()


loop = asyncio.get_event_loop()
loop.run_until_complete(get_data_e2e4())
loop.run_until_complete(get_data_citilink())
loop.close()