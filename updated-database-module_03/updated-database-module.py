import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import logging
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import random

# Nastavení logování
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("fred_database.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Konfigurace
CONFIG = {
    "db_file": "fred_data.db",
    "min_delay": 1.0,
    "max_delay": 3.0,
    "page_load_timeout": 30,
    "retry_limit": 3,
    "use_proxy": None
}

def create_database(db_file=None):
    """Vytvoří strukturu databáze optimalizovanou pro časové řady"""
    if db_file:
        CONFIG["db_file"] = db_file
        
    conn = sqlite3.connect(CONFIG["db_file"])
    cursor = conn.cursor()
    
    # Tabulka pro metadata sérií
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS series_metadata (
        series_id TEXT PRIMARY KEY,
        title TEXT,
        frequency TEXT,
        units TEXT,
        seasonal_adjustment TEXT,
        last_updated TEXT,
        last_checked TEXT,
        source TEXT,
        data_source TEXT,
        notes TEXT
    )
    ''')
    
    # Tabulka pro hodnoty sérií - hlavní tabulka dat
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS series_values (
        series_id TEXT,
        date TEXT,
        value REAL,
        PRIMARY KEY (series_id, date)
    )
    ''')
    
    # Tabulka pro sledování aktualizací
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS update_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        series_id TEXT,
        action TEXT,
        status TEXT,
        message TEXT
    )
    ''')
    
    # Indexy pro rychlejší vyhledávání
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_series_values_date ON series_values (date)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_series_values_series_id ON series_values (series_id)')
    
    conn.commit()
    conn.close()
    logger.info(f"Databáze úspěšně inicializována v souboru {CONFIG['db_file']}")
    return True

def store_series(series_data):
    """Uloží data série do databáze"""
    if not series_data or 'series_id' not in series_data:
        return False
    
    try:
        conn = sqlite3.connect(CONFIG["db_file"])
        cursor = conn.cursor()
        
        # Uložit metadata
        cursor.execute('''
        INSERT OR REPLACE INTO series_metadata
        (series_id, title, frequency, units, seasonal_adjustment, last_updated, 
         last_checked, source, data_source)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            series_data['series_id'],
            series_data.get('title', ''),
            series_data.get('frequency', ''),
            series_data.get('units', ''),
            series_data.get('seasonal_adjustment', ''),
            series_data.get('last_updated', ''),
            datetime.now().isoformat(),
            series_data.get('source', 'FRED'),
            series_data.get('data_source', '')
        ))
        
        # Uložit hodnoty
        if 'data' in series_data and series_data['data']:
            for point in series_data['data']:
                try:
                    cursor.execute('''
                    INSERT OR REPLACE INTO series_values (series_id, date, value)
                    VALUES (?, ?, ?)
                    ''', (
                        series_data['series_id'],
                        point['date'],
                        point['value']
                    ))
                except Exception as e:
                    logger.error(f"Chyba při ukládání hodnoty {point} pro {series_data['series_id']}: {str(e)}")
        
        # Zaznamenat aktualizaci
        data_count = len(series_data.get('data', []))
        cursor.execute('''
        INSERT INTO update_log (timestamp, series_id, action, status, message)
        VALUES (?, ?, ?, ?, ?)
        ''', (
            datetime.now().isoformat(),
            series_data['series_id'],
            'UPDATE',
            'SUCCESS',
            f"Aktualizováno s {data_count} datovými body"
        ))
        
        conn.commit()
        conn.close()
        
        logger.info(f"Série {series_data['series_id']} úspěšně uložena s {data_count} hodnotami")
        return True
    except Exception as e:
        logger.error(f"Chyba při ukládání série {series_data['series_id']}: {str(e)}")
        
        try:
            # Zaznamenat chybu
            conn = sqlite3.connect(CONFIG["db_file"])
            cursor = conn.cursor()
            cursor.execute('''
            INSERT INTO update_log (timestamp, series_id, action, status, message)
            VALUES (?, ?, ?, ?, ?)
            ''', (
                datetime.now().isoformat(),
                series_data['series_id'],
                'UPDATE',
                'ERROR',
                str(e)
            ))
            conn.commit()
            conn.close()
        except:
            pass
        
        return False

def get_latest_date_in_db(series_id):
    """Získá poslední datum v databázi pro danou sérii"""
    conn = sqlite3.connect(CONFIG["db_file"])
    cursor = conn.cursor()
    
    cursor.execute(
        "SELECT MAX(date) FROM series_values WHERE series_id = ?", 
        (series_id,)
    )
    result = cursor.fetchone()
    conn.close()
    
    if result and result[0]:
        return result[0]
    return None

def create_driver():
    """Vytvoří WebDriver s vlastnostmi pro obcházení detekce"""
    options = webdriver.ChromeOptions()
    
    # Nastavení pro nižší detekci automatizace
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--disable-infobars")
    
    # Realistický user agent
    options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15")
    
    # Přidat proxy, pokud je specifikována
    if CONFIG["use_proxy"]:
        logger.info(f"Používám proxy: {CONFIG['use_proxy']}")
        options.add_argument(f'--proxy-server={CONFIG["use_proxy"]}')
    
    # Vytvořit driver
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(CONFIG["page_load_timeout"])
    
    # Obejít detekci
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    return driver

def random_delay():
    """Náhodné zpoždění pro simulaci lidského chování"""
    delay = CONFIG["min_delay"] + random.random() * (CONFIG["max_delay"] - CONFIG["min_delay"])
    time.sleep(delay)

def safe_get_url(driver, url, max_retries=3):
    """Bezpečně načte URL s ošetřením chyb a detekcí 'Access Denied'"""
    for attempt in range(max_retries):
        try:
            random_delay()
            driver.get(url)
            
            # Zkontrolovat Access Denied
            if "Access Denied" in driver.page_source or "You don't have permission to access" in driver.page_source:
                logger.warning(f"Access Denied detekován při pokusu {attempt+1} pro {url}")
                
                if attempt < max_retries - 1:
                    wait_time = 10 + random.random() * 5
                    logger.info(f"Čekám {wait_time:.2f}s před dalším pokusem...")
                    time.sleep(wait_time)
                    
                    # Zkusit vrátit se na úvodní stránku
                    try:
                        driver.get("https://fred.stlouisfed.org/")
                        time.sleep(3)
                    except:
                        pass
                else:
                    logger.error(f"Nepodařilo se obejít Access Denied po {max_retries} pokusech")
                    return False
            else:
                # Úspěšně načteno
                return True
        except Exception as e:
            logger.error(f"Chyba při načítání {url}: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(5)
            else:
                return False
    
    return False

def check_if_needs_update_selenium(series_id, driver=None):
    """Kontroluje pomocí Selenia, zda série potřebuje aktualizaci"""
    should_close_driver = False
    
    try:
        conn = sqlite3.connect(CONFIG["db_file"])
        cursor = conn.cursor()
        
        # Získat datum poslední kontroly a frekvenci
        cursor.execute(
            "SELECT last_checked, frequency FROM series_metadata WHERE series_id = ?", 
            (series_id,)
        )
        result = cursor.fetchone()
        
        if not result:
            # Nemáme žádné záznamy, potřebujeme stáhnout
            conn.close()
            return True
        
        last_checked, frequency = result
        
        try:
            last_checked_date = datetime.fromisoformat(last_checked)
        except (ValueError, TypeError):
            # Neplatné datum, aktualizujeme
            conn.close()
            return True
        
        now = datetime.now()
        
        # Zkontrolovat podle frekvence, jak často bychom měli kontrolovat
        check_interval_days = {
            'Daily': 1,
            'Weekly': 1,
            'Biweekly': 2,
            'Monthly': 2,
            'Quarterly': 7
        }
        
        # Získat interval
        freq_lower = frequency.lower() if frequency else ''
        interval = 1  # default
        
        for key, value in check_interval_days.items():
            if key.lower() in freq_lower:
                interval = value
                break
        
        # Pokud jsme nedávno kontrolovali, není potřeba aktualizovat
        if (now - last_checked_date).days < interval:
            conn.close()
            return False
        
        conn.close()
        
        # Jinak jdeme na web a kontrolujeme, zda došlo k aktualizaci
        # Vytvořit driver, pokud nebyl předán
        if driver is None:
            driver = create_driver()
            should_close_driver = True
        
        url = f"https://fred.stlouisfed.org/series/{series_id}"
        
        if not safe_get_url(driver, url):
            logger.error(f"Nepodařilo se načíst stránku pro {series_id}")
            return True  # V případě chyby raději aktualizujeme
        
        # Najít datum poslední aktualizace na webu
        try:
            meta_labels = driver.find_elements(By.CSS_SELECTOR, 'span.series-meta-label')
            
            last_updated_web = None
            for label_elem in meta_labels:
                if 'last updated' in label_elem.text.lower():
                    try:
                        value_elem = label_elem.find_element(By.XPATH, "following-sibling::span[@class='series-meta-value']")
                        last_updated_web = value_elem.text.strip() if value_elem else None
                        break
                    except:
                        pass
            
            if not last_updated_web:
                # Zkusit alternativní způsob
                meta_items = driver.find_elements(By.CSS_SELECTOR, '.series-meta-item')
                for item in meta_items:
                    item_text = item.text
                    if "Last Updated:" in item_text:
                        last_updated_web = item_text.split("Last Updated:", 1)[1].strip()
                        break
            
            if last_updated_web:
                # Získat poslední datum aktualizace v databázi
                conn = sqlite3.connect(CONFIG["db_file"])
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT last_updated FROM series_metadata WHERE series_id = ?", 
                    (series_id,)
                )
                result = cursor.fetchone()
                conn.close()
                
                if result and result[0]:
                    last_updated_db = result[0]
                    
                    # Porovnat data - pokud se liší, potřebujeme aktualizaci
                    needs_update = last_updated_web != last_updated_db
                    
                    if needs_update:
                        logger.info(f"Série {series_id} potřebuje aktualizaci: web={last_updated_web}, db={last_updated_db}")
                    else:
                        logger.info(f"Série {series_id} je aktuální")
                    
                    return needs_update
            
            # Pokud nemůžeme určit, raději aktualizujeme
            logger.warning(f"Nelze určit 'last updated' pro {series_id}, raději aktualizuji")
            return True
            
        except Exception as e:
            logger.error(f"Chyba při hledání 'last updated' pro {series_id}: {str(e)}")
            return True
    
    except Exception as e:
        logger.error(f"Chyba při kontrole, zda {series_id} potřebuje aktualizaci: {str(e)}")
        return True
    
    finally:
        # Zavřít driver, pokud jsme ho vytvořili zde
        if should_close_driver and driver:
            driver.quit()

def check_if_needs_update(series_id):
    """Zpětná kompatibilita pro původní funkci"""
    return check_if_needs_update_selenium(series_id)

def get_series_to_update(min_days=None):
    """Získá seznam sérií, které by měly být zkontrolovány pro aktualizaci"""
    try:
        conn = sqlite3.connect(CONFIG["db_file"])
        cursor = conn.cursor()
        
        query = """
        SELECT series_id, frequency, last_checked 
        FROM series_metadata 
        WHERE frequency LIKE '%Daily%' OR 
              frequency LIKE '%Weekly%' OR 
              frequency LIKE '%Biweekly%' OR 
              frequency LIKE '%Monthly%' OR 
              frequency LIKE '%Quarterly%'
        """
        
        # Filtrovat podle počtu dní od poslední kontroly
        if min_days is not None:
            cutoff_date = (datetime.now() - timedelta(days=min_days)).isoformat()
            query += f" AND (last_checked IS NULL OR last_checked < '{cutoff_date}')"
        
        cursor.execute(query)
        results = cursor.fetchall()
        conn.close()
        
        series_list = []
        for series_id, frequency, last_checked in results:
            series_list.append({
                'series_id': series_id,
                'frequency': frequency,
                'last_checked': last_checked
            })
        
        logger.info(f"Nalezeno {len(series_list)} sérií ke kontrole pro aktualizaci")
        return series_list
    
    except Exception as e:
        logger.error(f"Chyba při získávání seznamu sérií k aktualizaci: {str(e)}")
        return []

def get_series_stats():
    """Získá statistiky o uložených sériích"""
    try:
        conn = sqlite3.connect(CONFIG["db_file"])
        cursor = conn.cursor()
        
        # Počet uložených sérií
        cursor.execute("SELECT COUNT(*) FROM series_metadata")
        series_count = cursor.fetchone()[0]
        
        # Počet hodnot
        cursor.execute("SELECT COUNT(*) FROM series_values")
        values_count = cursor.fetchone()[0]
        
        # Frekvence aktualizace
        cursor.execute("""
        SELECT frequency, COUNT(*) as count 
        FROM series_metadata 
        GROUP BY frequency 
        ORDER BY count DESC
        """)
        frequency_counts = cursor.fetchall()
        
        # Poslední aktualizace
        cursor.execute("""
        SELECT datetime(MAX(timestamp)) as last_update
        FROM update_log
        WHERE status = 'SUCCESS'
        """)
        last_update = cursor.fetchone()[0] if cursor.fetchone() else None
        
        conn.close()
        
        stats = {
            'series_count': series_count,
            'values_count': values_count,
            'frequency_distribution': [
                {'frequency': freq, 'count': count}
                for freq, count in frequency_counts
            ],
            'last_update': last_update
        }
        
        return stats
    
    except Exception as e:
        logger.error(f"Chyba při získávání statistik: {str(e)}")
        return {
            'error': str(e),
            'series_count': 0,
            'values_count': 0,
            'frequency_distribution': [],
            'last_update': None
        }

# Inicializace databáze, pokud skript spuštěn přímo
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Správa databáze FRED dat')
    parser.add_argument('--init', action='store_true', help='Inicializovat databázi')
    parser.add_argument('--stats', action='store_true', help='Zobrazit statistiky o databázi')
    parser.add_argument('--db-file', type=str, help='Cesta k databázovému souboru')
    parser.add_argument('--proxy', type=str, help='Proxy server (volitelné)')
    
    args = parser.parse_args()
    
    if args.db_file:
        CONFIG["db_file"] = args.db_file
    
    if args.proxy:
        CONFIG["use_proxy"] = args.proxy
    
    if args.init:
        create_database()
    
    if args.stats:
        stats = get_series_stats()
        print(f"Statistiky databáze {CONFIG['db_file']}:")
        print(f"Počet sérií: {stats['series_count']}")
        print(f"Počet hodnot: {stats['values_count']}")
        print(f"Poslední aktualizace: {stats['last_update']}")
        
        print("\nDistribuce frekvencí:")
        for item in stats['frequency_distribution']:
            print(f"  {item['frequency'] or 'Neznámá'}: {item['count']}")
    
    if not args.init and not args.stats:
        parser.print_help()