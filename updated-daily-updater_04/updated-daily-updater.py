import pandas as pd
import sqlite3
import logging
import time
from datetime import datetime, timedelta
import os
import sys
import json
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse
from selenium import webdriver
import random

# Přidat cestu k modulům
script_dir = os.path.dirname(os.path.abspath(__file__))
if script_dir not in sys.path:
    sys.path.append(script_dir)

# Import nových modulů (předpokládáme, že máme tyto moduly v adresáři)
try:
    # Importovat funkce z modulů, které jsme vytvořili
    from updated_database_module import (
        create_database, store_series, get_latest_date_in_db, 
        check_if_needs_update_selenium, get_series_to_update, create_driver
    )
except ImportError:
    # Záložní import, pokud moduly nejsou dostupné
    print("VAROVÁNÍ: Nemohu najít nové moduly, používám původní funkce")
    from database import store_series, get_latest_date_in_db, check_if_needs_update
    from series_scraper import scrape_series_data, is_quarterly_or_more_frequent

# Nastavení logování
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("fred_daily_update.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Konfigurace
CONFIG = {
    "db_file": "fred_data.db",
    "max_workers": 5,  # Menší počet vláken pro méně agresivní scrapování
    "timeout": 3600,  # 1 hodina maximální délka běhu
    "limit_per_run": None,  # bez limitu
    "min_delay": 1.0,  # minimální zpoždění mezi požadavky
    "max_delay": 3.0,  # maximální zpoždění
    "retry_limit": 3,  # počet pokusů při chybě
    "use_proxy": None  # proxy server (volitelné)
}

def random_delay():
    """Náhodné zpoždění pro simulaci lidského chování"""
    delay = CONFIG["min_delay"] + random.random() * (CONFIG["max_delay"] - CONFIG["min_delay"])
    time.sleep(delay)

def extract_series_data_selenium(driver, series_id):
    """Extrahuje data série pomocí Selenia"""
    try:
        url = f"https://fred.stlouisfed.org/series/{series_id}"
        
        # Náhodné zpoždění
        random_delay()
        
        # Načíst stránku
        driver.get(url)
        
        # Kontrola na Access Denied
        if "Access Denied" in driver.page_source:
            logger.error(f"Přístup odepřen pro {series_id}")
            return None
        
        # Získat metadata
        title_element = None
        try:
            title_element = driver.find_element("css selector", 'h1.series-title')
        except:
            try:
                title_element = driver.find_element("css selector", '.series-title')
            except:
                pass
        
        title = title_element.text.strip() if title_element else ""
        
        metadata = {
            'series_id': series_id,
            'title': title,
            'frequency': None,
            'units': None,
            'seasonal_adjustment': None,
            'last_updated': None,
            'source': 'FRED',
            'data_source': None
        }
        
        # Extrahovat metadata ze stránky
        try:
            meta_labels = driver.find_elements("css selector", 'span.series-meta-label')
            
            for label_elem in meta_labels:
                label = label_elem.text.strip().lower()
                
                try:
                    value_elem = driver.find_element("xpath", f"//span[@class='series-meta-label' and contains(text(),'{label}')]/following-sibling::span[@class='series-meta-value']")
                    value = value_elem.text.strip() if value_elem else ""
                except:
                    value = ""
                
                if 'frequency' in label:
                    metadata['frequency'] = value
                elif 'units' in label:
                    metadata['units'] = value
                elif 'adjustment' in label:
                    metadata['seasonal_adjustment'] = value
                elif 'last updated' in label:
                    metadata['last_updated'] = value
                elif 'source' in label:
                    metadata['data_source'] = value
        except Exception as e:
            logger.warning(f"Chyba při extrakci metadat pro {series_id}: {str(e)}")
        
        # Kontrola frekvence
        if not is_quarterly_or_more_frequent(metadata['frequency']):
            logger.info(f"Přeskakuji {series_id} - frekvence {metadata['frequency']} není dostatečně častá")
            return None
        
        # Stáhnout CSV data
        download_url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"
        
        # Náhodné zpoždění
        random_delay()
        
        try:
            driver.get(download_url)
            
            # Extrahovat CSV
            page_source = driver.page_source
            
            # Zpracovat CSV data
            csv_text = page_source
            
            # Někdy je CSV vloženo do HTML, takže extrahujeme jen CSV část
            if "<html" in csv_text:
                # Může být různě formátováno podle prohlížeče
                if "<pre" in csv_text:
                    # Najít obsah mezi <pre> tagy
                    start = csv_text.find("<pre")
                    start = csv_text.find(">", start) + 1
                    end = csv_text.find("</pre>", start)
                    if start > 0 and end > start:
                        csv_text = csv_text[start:end]
                else:
                    # Zkusit extrahovat text z body
                    start = csv_text.find("<body")
                    start = csv_text.find(">", start) + 1
                    end = csv_text.find("</body>", start)
                    if start > 0 and end > start:
                        csv_text = csv_text[start:end]
            
            # Vyčistit případné HTML entity a značky
            csv_text = csv_text.replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&")
            csv_text = csv_text.replace("&quot;", "\"").replace("&apos;", "'")
            
            # Odstranit HTML tagy
            import re
            csv_text = re.sub(r'<[^>]+>', '', csv_text)
            
            # Uložit do dočasného souboru
            temp_file = f"temp_{series_id}.csv"
            with open(temp_file, 'w', encoding='utf-8') as f:
                f.write(csv_text.strip())
            
            # Načíst data z dočasného souboru
            try:
                df = pd.read_csv(temp_file)
                os.remove(temp_file)
                
                if df.empty:
                    logger.warning(f"Prázdná data pro {series_id}")
                    return None
                
                # Standardizovat názvy sloupců
                df.columns = [col.strip() for col in df.columns]
                
                # Převést na formát pro databázi
                data = []
                for _, row in df.iterrows():
                    date_str = str(row.iloc[0])
                    value = row.iloc[1]
                    
                    # Kontrola platnosti hodnoty
                    if pd.notna(value):
                        try:
                            value_float = float(value)
                            data.append({
                                'date': date_str,
                                'value': value_float
                            })
                        except (ValueError, TypeError):
                            logger.warning(f"Neplatná hodnota {value} pro {series_id} na {date_str}")
                    else:
                        # Zahrnout i NULL hodnoty pro úplnost
                        data.append({
                            'date': date_str,
                            'value': None
                        })
                
                metadata['data'] = data
                
                logger.info(f"Úspěšně extrahováno {len(data)} datových bodů pro {series_id}")
                return metadata
                
            except Exception as e:
                logger.error(f"Chyba při zpracování CSV dat pro {series_id}: {str(e)}")
                if os.path.exists(temp_file):
                    os.remove(temp_file)
                return None
            
        except Exception as e:
            logger.error(f"Chyba při stahování CSV pro {series_id}: {str(e)}")
            return None
        
    except Exception as e:
        logger.error(f"Chyba při extrakci dat pro {series_id}: {str(e)}")
        return None

def is_quarterly_or_more_frequent(frequency):
    """Určí, zda je frekvence čtvrtletní nebo častější"""
    if not frequency:
        return False
    
    high_frequency = ['daily', 'weekly', 'biweekly', 'monthly', 'quarterly']
    frequency_lower = frequency.lower() if frequency else ''
    
    for freq in high_frequency:
        if freq in frequency_lower:
            return True
    
    return False

def update_series_selenium(driver, series_info):
    """Aktualizuje jednu sérii pomocí Selenia"""
    series_id, priority = series_info
    
    try:
        # Zkontrolovat, zda potřebujeme aktualizaci
        if not check_if_needs_update_selenium(series_id, driver):
            logger.info(f"Série {series_id} (priorita {priority}) nepotřebuje aktualizaci")
            
            # Aktualizovat jen datum poslední kontroly
            conn = sqlite3.connect(CONFIG["db_file"])
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE series_metadata SET last_checked = ? WHERE series_id = ?",
                (datetime.now().isoformat(), series_id)
            )
            conn.commit()
            conn.close()
            
            return False
        
        # Stáhnout data
        series_data = extract_series_data_selenium(driver, series_id)
        
        if not series_data:
            logger.warning(f"Nepodařilo se získat data pro {series_id}")
            return False
        
        # Uložit do databáze
        success = store_series(series_data)
        
        if success:
            logger.info(f"Série {series_id} úspěšně aktualizována")
        else:
            logger.warning(f"Problém při ukládání dat pro {series_id}")
        
        return success
    
    except Exception as e:
        logger.error(f"Chyba při aktualizaci série {series_id}: {str(e)}")
        return False

def update_series(series_info):
    """Kompatibilita se starým API - vytvoří dočasný driver a aktualizuje sérii"""
    driver = None
    try:
        driver = create_driver()
        return update_series_selenium(driver, series_info)
    except Exception as e:
        logger.error(f"Chyba při aktualizaci série: {str(e)}")
        return False
    finally:
        if driver:
            driver.quit()

def driver_worker(worker_id, series_list, results):
    """Worker vlákno, které pracuje s jednou instancí driveru"""
    driver = None
    try:
        driver = create_driver()
        logger.info(f"Worker {worker_id} vytvořil driver a začíná zpracování {len(series_list)} sérií")
        
        # Nejprve navštívit hlavní stránku pro získání cookies
        driver.get("https://fred.stlouisfed.org/")
        time.sleep(2)
        
        for i, series_info in enumerate(series_list):
            series_id = series_info[0]
            
            try:
                was_updated = update_series_selenium(driver, series_info)
                
                if was_updated:
                    results['updated'] += 1
                else:
                    results['skipped'] += 1
                
                # Zobrazit průběh
                if (i + 1) % 10 == 0 or i == len(series_list) - 1:
                    processed = results['updated'] + results['skipped'] + results['failed']
                    logger.info(f"Worker {worker_id}: Zpracováno {i+1}/{len(series_list)} sérií "
                                f"({processed}/{results['total']} celkem)")
            
            except Exception as e:
                results['failed'] += 1
                logger.error(f"Worker {worker_id}: Chyba během aktualizace {series_id}: {str(e)}")
                
                # Zkusit se zotavit
                try:
                    # Vrátit se na hlavní stránku pro reset session
                    driver.get("https://fred.stlouisfed.org/")
                    time.sleep(3)
                except:
                    pass
        
        logger.info(f"Worker {worker_id} dokončil zpracování {len(series_list)} sérií")
    
    except Exception as e:
        logger.error(f"Worker {worker_id}: Kritická chyba: {str(e)}")
    
    finally:
        if driver:
            driver.quit()

def parallel_update_series_with_workers(max_workers=None, limit=None):
    """Paralelní aktualizace sérií s více workery, každý s vlastním driverem"""
    if max_workers is None:
        max_workers = CONFIG["max_workers"]
    
    start_time = time.time()
    
    # Získat seznam sérií k aktualizaci
    series_list = get_series_to_update()
    
    if limit and limit > 0:
        series_list = series_list[:limit]
    
    logger.info(f"Plánuji aktualizaci {len(series_list)} sérií s {max_workers} workery")
    
    # Výsledky budou sdílené mezi vlákny
    results = {
        'total': len(series_list),
        'updated': 0,
        'skipped': 0,
        'failed': 0
    }
    
    # Rozdělit série mezi workery
    worker_series = []
    series_per_worker = (len(series_list) + max_workers - 1) // max_workers
    
    for i in range(0, len(series_list), series_per_worker):
        worker_series.append(series_list[i:i+series_per_worker])
    
    # Spustit worker vlákna
    threads = []
    for i, series_batch in enumerate(worker_series):
        thread = threading.Thread(
            target=driver_worker,
            args=(i, series_batch, results)
        )
        threads.append(thread)
        thread.start()
    
    # Čekat na dokončení všech vláken
    for thread in threads:
        thread.join()
    
    # Závěrečná zpráva
    elapsed = time.time() - start_time
    logger.info(f"Aktualizace dokončena za {elapsed:.1f} sekund")
    logger.info(f"Celkem: {results['total']}, Aktualizováno: {results['updated']}, "
               f"Přeskočeno: {results['skipped']}, Chyba: {results['failed']}")
    
    return results

def parallel_update_series(max_workers=None, limit=None):
    """Zpětná kompatibilita s původní funkcí"""
    return parallel_update_series_with_workers(max_workers, limit)

def schedule_daily_update():
    """Funkce pro plánování denních aktualizací (lze použít s cron nebo scheduled tasks)"""
    logger.info("Zahajuji denní aktualizaci FRED dat")
    
    # Načíst konfiguraci (pokud existuje)
    config = dict(CONFIG)  # Kopie výchozí konfigurace
    
    try:
        # Načíst konfiguraci z souboru, pokud existuje
        if os.path.exists('fred_config.json'):
            with open('fred_config.json', 'r') as f:
                loaded_config = json.load(f)
                config.update(loaded_config)
                # Aktualizovat globální konfiguraci
                CONFIG.update(loaded_config)
    except Exception as e:
        logger.error(f"Chyba při načítání konfigurace: {str(e)}")
    
    # Nastavit timeout
    start_time = time.time()
    max_time = config.get('timeout', 3600)
    
    try:
        # Spustit aktualizaci
        results = parallel_update_series_with_workers(
            max_workers=config.get('max_workers', CONFIG["max_workers"]),
            limit=config.get('limit_per_run', None)
        )
        
        # Zaznamenat výsledky
        conn = sqlite3.connect(CONFIG["db_file"])
        cursor = conn.cursor()
        cursor.execute('''
        INSERT INTO update_log 
        (timestamp, series_id, action, status, message)
        VALUES (?, ?, ?, ?, ?)
        ''', (
            datetime.now().isoformat(),
            'SYSTEM',
            'DAILY_UPDATE',
            'SUCCESS',
            f"Aktualizováno: {results['updated']}, Přeskočeno: {results['skipped']}, Chyba: {results['failed']}"
        ))
        conn.commit()
        conn.close()
        
        return True
    
    except Exception as e:
        logger.error(f"Chyba při denní aktualizaci: {str(e)}")
        
        # Zaznamenat chybu
        try:
            conn = sqlite3.connect(CONFIG["db_file"])
            cursor = conn.cursor()
            cursor.execute('''
            INSERT INTO update_log 
            (timestamp, series_id, action, status, message)
            VALUES (?, ?, ?, ?, ?)
            ''', (
                datetime.now().isoformat(),
                'SYSTEM',
                'DAILY_UPDATE',
                'ERROR',
                str(e)
            ))
            conn.commit()
            conn.close()
        except:
            pass
        
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Aktualizace FRED dat')
    parser.add_argument('--limit', type=int, help='Limit počtu sérií k aktualizaci')
    parser.add_argument('--workers', type=int, help='Počet paralelních workerů')
    parser.add_argument('--db-file', type=str, help='Cesta k databázovému souboru')
    parser.add_argument('--proxy', type=str, help='Proxy server (volitelné)')
    parser.add_argument('--delay', type=float, help='Základní zpoždění mezi požadavky')
    
    args = parser.parse_args()
    
    # Aktualizovat konfiguraci podle argumentů
    if args.db_file:
        CONFIG["db_file"] = args.db_file
    
    if args.workers:
        CONFIG["max_workers"] = args.workers
    
    if args.proxy:
        CONFIG["use_proxy"] = args.proxy
    
    if args.delay:
        CONFIG["min_delay"] = args.delay
        CONFIG["max_delay"] = args.delay * 2
    
    # Spustit aktualizaci
    if args.limit:
        parallel_update_series(
            max_workers=CONFIG["max_workers"], 
            limit=args.limit
        )
    else:
        schedule_daily_update()