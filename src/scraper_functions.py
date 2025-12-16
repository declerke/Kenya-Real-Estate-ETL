import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import re
import time
from typing import List, Dict, Optional
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, Float, String, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import inspect
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

Base = declarative_base()

class HouseProperty(Base):
    __tablename__ = 'properties'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    title = Column(Text, nullable=False)
    price_text = Column(String(50))
    location = Column(String(200))
    bedrooms_text = Column(String(50))
    bathrooms_text = Column(String(50))
    size_text = Column(String(50))
    
    price_numeric = Column(Float)
    bedrooms_numeric = Column(Integer)
    bathrooms_numeric = Column(Integer)
    size_sqm = Column(Float)
    
    location_clean = Column(String(200))
    source = Column(String(100))
    scraped_date = Column(DateTime)
    inserted_date = Column(DateTime, default=datetime.utcnow)
    
    def __repr__(self):
        return f"<Property(id={self.id}, title='{self.title[:30]}...', price={self.price_numeric})>"

def get_db_engine(config: dict):
    db_url = f"postgresql://{config['username']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}"
    engine = create_engine(db_url, echo=False)
    Base.metadata.create_all(engine)
    return engine

def scrape_pages(start_page: int, end_page: int) -> pd.DataFrame:
    base_url = 'https://www.buyrentkenya.com/houses-for-sale'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }
    properties = []
    for page_num in range(start_page, end_page + 1):
        url = f'{base_url}?page={page_num}'
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            logger.warning(f"Failed to retrieve page {page_num}. Status code: {response.status_code}")
            continue
        soup = BeautifulSoup(response.content, 'html.parser')
        listings = soup.find_all('div', class_='listing-card')
        for listing in listings:
            title_tag = listing.find('h2')
            title = title_tag.get_text(strip=True) if title_tag else 'No title'
            price_tag = listing.find('p', class_='text-xl font-bold leading-7 text-grey-900')
            price = price_tag.get_text(strip=True) if price_tag else 'No price'
            location_tag = listing.find('p', class_='ml-1 truncate text-sm font-normal capitalize text-grey-650')
            location = location_tag.get_text(strip=True) if location_tag else 'No location'
            bedrooms = bathrooms = size = 'N/A'
            swiper_div = listing.find('div', class_='scrollable-list')
            if swiper_div:
                slides = swiper_div.find_all('div', class_='swiper-slide')
                for slide in slides:
                    text = slide.get_text(strip=True)
                    if 'Bedroom' in text:
                        bedrooms = text
                    elif 'Bathroom' in text:
                        bathrooms = text
                    elif 'm²' in text:
                        size = text
            properties.append({
                'Title': title,
                'Price': price,
                'Location': location,
                'Bedrooms': bedrooms,
                'Bathrooms': bathrooms,
                'Size': size
            })
        if page_num < end_page:
            time.sleep(1)
    df = pd.DataFrame(properties)
    logger.info(f"Scraping complete. Total properties extracted: {len(df)}")
    return df

def clean_price(price_str: str) -> float:
    if not price_str or price_str in ['No price', 'N/A', '']:
        return np.nan
    try:
        clean_str = price_str.replace('KSh', '').strip()
        clean_str = clean_str.replace(',', '')
        return float(clean_str)
    except (ValueError, AttributeError):
        return np.nan

def extract_number_from_text(text: str) -> float:
    if not text or text in ['N/A', 'No data', '']:
        return np.nan
    if 'studio' in text.lower():
        return 0.0
    try:
        match = re.search(r'\d+', text)
        if match:
            return float(match.group())
        else:
            return np.nan
    except (ValueError, AttributeError):
        return np.nan

def clean_size(size_str: str) -> float:
    if not size_str or size_str in ['N/A', 'No size', '']:
        return np.nan
    try:
        clean_str = size_str.replace('m²', '').replace('m2', '').strip()
        clean_str = clean_str.replace(',', '')
        return float(clean_str)
    except (ValueError, AttributeError):
        return np.nan

def clean_location(location_str: str) -> str:
    if not location_str or location_str in ['No location', 'N/A', '']:
        return 'Unknown'
    return location_str.strip().title()

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df_clean = df.copy()
    initial_rows = len(df_clean)
    
    df_clean['Price_Numeric'] = df_clean['Price'].apply(clean_price)
    df_clean['Bedrooms_Numeric'] = df_clean['Bedrooms'].apply(extract_number_from_text)
    df_clean['Bathrooms_Numeric'] = df_clean['Bathrooms'].apply(extract_number_from_text)
    df_clean['Size_SqM'] = df_clean['Size'].apply(clean_size)
    df_clean['Location_Clean'] = df_clean['Location'].apply(clean_location)
    
    df_clean = df_clean.dropna(subset=['Price_Numeric', 'Bedrooms_Numeric'])
    
    df_clean['Scraped_Date'] = pd.Timestamp.now()
    df_clean['Source'] = 'buyrentkenya.com'
    
    logger.info(f"Data cleaned. Final records: {len(df_clean)}. Removed {initial_rows - len(df_clean)} incomplete records.")
    return df_clean

def prepare_data_for_db(df: pd.DataFrame) -> list:
    property_objects = []
    for index, row in df.iterrows():
        property_obj = HouseProperty(
            title=row['Title'],
            price_text=row['Price'],
            location=row['Location'],
            bedrooms_text=row['Bedrooms'],
            bathrooms_text=row['Bathrooms'],
            size_text=row['Size'],
            price_numeric=row['Price_Numeric'] if pd.notna(row['Price_Numeric']) else None,
            bedrooms_numeric=int(row['Bedrooms_Numeric']) if pd.notna(row['Bedrooms_Numeric']) else None,
            bathrooms_numeric=int(row['Bathrooms_Numeric']) if pd.notna(row['Bathrooms_Numeric']) else None,
            size_sqm=row['Size_SqM'] if pd.notna(row['Size_SqM']) else None,
            location_clean=row['Location_Clean'],
            source=row['Source'],
            scraped_date=row['Scraped_Date']
        )
        property_objects.append(property_obj)
    return property_objects

def insert_data_to_db(df_clean: pd.DataFrame, engine):
    property_objects = prepare_data_for_db(df_clean)
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        session.add_all(property_objects)
        session.commit()
        logger.info(f"Successfully inserted {len(property_objects)} records.")
        return len(property_objects)
    except Exception as e:
        session.rollback()
        logger.error(f"Error inserting data: {e}")
        return 0
    finally:
        session.close()

def scrape_and_store_data(db_config: dict, scraping_config: dict):
    start_time = datetime.now()
    
    try:
        df_raw = scrape_pages(
            start_page=scraping_config['start_page'],
            end_page=scraping_config['end_page']
        )
        
        df_clean = clean_dataframe(df_raw)
        
        engine = get_db_engine(db_config)
        records_inserted = insert_data_to_db(df_clean, engine)
        
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        
        summary = {
            'status': 'success',
            'records_scraped': len(df_raw),
            'records_cleaned': len(df_clean),
            'records_inserted': records_inserted,
            'execution_time_seconds': execution_time,
            'timestamp': datetime.now().isoformat()
        }
        
        logger.info("=" * 60)
        logger.info("ETL PIPELINE COMPLETED SUCCESSFULLY")
        logger.info(f"Records inserted: {records_inserted}")
        logger.info(f"Execution time: {execution_time:.2f} seconds")
        logger.info("=" * 60)
        
        return summary
    
    except Exception as e:
        logger.error(f"ETL pipeline failed: {str(e)}")
        return {
            'status': 'failed',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }