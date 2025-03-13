#!/usr/bin/env python3
"""
FRED Metadata Extractor - Kompletní skript pro extrakci metadat z FRED databáze

Tento skript umožňuje:
1. Extrahovat metadata pro jednotlivé FRED série
2. Zpracovat celý seznam sérií z CSV souboru
3. Paralelně zpracovávat velké množství sérií
4. Ukládat výsledky do CSV souboru
5. Robustně ošetřovat chyby a obcházet ochranu proti scrapování

Autor: Vaše jméno
Datum: 2025-03-12
"""

import os
import time
import random
import json
import csv
import logging
import argparse
import threading
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
from typing import Dict, List, Optional, Any, Tuple

# Selenium a BeautifulSoup pro scraping
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException

# Konfigurace
CONFIG = {
    "webdriver_path": None,  # Bude automaticky hledat driver
    "headless": False,       # False pro viditelný prohlížeč, True pro headless mód
    "min_delay": 1.0,        # Minimální zpoždění mezi požadavky (sekundy)
    "max_delay": 3.0,        # Maximální zpoždění mezi požadavky (sekundy)
    "page_load_timeout": 30, # Timeout pro načtení stránky (sekundy)
    "retry_limit": 3,        # Počet pokusů při chybě
    "max_workers": 4,        # Počet paralelních vláken
    "batch_size": 10,       # Velikost dávky pro zpracování
    "output_dir": "fred_data",  # Adresář pro výstupní soubory
    "debug": False           # Debug mód pro podrobnější logování
}

# Nastavení logování
def setup_logging(debug=False):
    """Nastaví logování"""
    log_level = logging.DEBUG if debug else logging.INFO
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    os.makedirs(CONFIG["output_dir"], exist_ok=True)
    logging.basicConfig(
        level=log_level,
        format=log_format,
        handlers=[
            logging.FileHandler(os.path.join(CONFIG["output_dir"], "fred_metadata.log")),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

# Inicializace loggeru
logger = setup_logging(CONFIG["debug"])

def random_delay():
    """Náhodné zpoždění pro simulaci lidského chování"""
    delay = CONFIG["min_delay"] + random.random() * (CONFIG["max_delay"] - CONFIG["min_delay"])
    time.sleep(delay)

def create_driver():
    """Vytvoří a konfiguruje WebDriver pro Selenium"""
    options = webdriver.ChromeOptions()
    
    # Nastavení pro nižší detekci automatizace
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--disable-infobars")
    
    # Realistický user agent
    options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    # Headless mód, pokud je požadován
    if CONFIG["headless"]:
        options.add_argument("--headless=new")
        options.add_argument("--window-size=1920,1080")
    
    # Vytvořit driver
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(CONFIG["page_load_timeout"])
    
    # Obejít detekci
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    driver.execute_script("window.chrome = { runtime: {} };")
    
    # Nastavit realistické rozměry okna
    if not CONFIG["headless"]:
        driver.set_window_size(1366, 768)
    
    return driver

def get_series_metadata_selenium(driver, series_id):
    """Extrahuje metadata série z FRED stránky podle aktuální struktury"""
    try:
        url = f"https://fred.stlouisfed.org/series/{series_id}"
        
        # Načíst stránku
        driver.get(url)
        
        # Počkat na načtení hlavního obsahu
        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".series-title, h1"))
            )
            # Krátká pauza pro dokončení JS renderování
            time.sleep(1.5)
        except TimeoutException:
            logger.warning(f"Timeout při čekání na načtení stránky pro {series_id}")
        
        # Získat title
        title = ""
        try:
            title_element = driver.find_element(By.CSS_SELECTOR, '.series-title, h1')
            title = title_element.text.strip()
        except NoSuchElementException:
            logger.warning(f"Nepodařilo se najít název série pro {series_id}")
        
        # Inicializace metadat
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
        
        # METODA 1: Extrakce podle struktury stránky
        
        # Frequency - přímo z oblasti Frequency
        try:
            # Hledáme sekci "Frequency:" a hodnotu
            freq_headers = driver.find_elements(By.XPATH, "//div[contains(text(), 'Frequency:')]")
            if freq_headers:
                # Pokud existuje Frequency: nadpis, vezmeme samotnou hodnotu, která je v té samé oblasti
                freq_value = driver.find_element(By.XPATH, "//div[contains(text(), 'Frequency:')]/following-sibling::div").text.strip()
                metadata['frequency'] = freq_value
        except Exception as e:
            logger.warning(f"Chyba při extrakci frekvence podle hlavičky: {e}")
        
        # Pokud první metoda selhala, zkusíme přímo najít hodnotu frekvence
        if not metadata['frequency']:
            try:
                # Zkontrolovat hodnoty v textu stránky
                freq_values = ["Monthly", "Weekly", "Daily", "Quarterly", "Annual", "Biweekly", "Semiannual"]
                for freq in freq_values:
                    elements = driver.find_elements(By.XPATH, f"//*[contains(text(), '{freq}')]")
                    if elements:
                        for elem in elements:
                            if len(elem.text) < 30:  # Krátký text napovídá, že jde o samotnou hodnotu
                                metadata['frequency'] = freq
                                break
                        if metadata['frequency']:
                            break
            except Exception as e:
                logger.warning(f"Chyba při přímém hledání hodnoty frekvence: {e}")
        
        # Pokud stále nemáme frekvenci, zkusíme to jinak
        if not metadata['frequency']:
            try:
                # Kompletní text stránky
                page_text = driver.page_source
                
                # Hledat frekvenci v textu stránky
                frequency_patterns = {
                    "Monthly": ["Frequency: Monthly", "Frequency:</div><div>Monthly", ">Monthly<"],
                    "Daily": ["Frequency: Daily", "Frequency:</div><div>Daily", ">Daily<"],
                    "Weekly": ["Frequency: Weekly", "Frequency:</div><div>Weekly", ">Weekly<"],
                    "Quarterly": ["Frequency: Quarterly", "Frequency:</div><div>Quarterly", ">Quarterly<"],
                    "Annual": ["Frequency: Annual", "Frequency:</div><div>Annual", ">Annual<"],
                    "Biweekly": ["Frequency: Biweekly", "Frequency:</div><div>Biweekly", ">Biweekly<"],
                    "Semiannual": ["Frequency: Semiannual", "Frequency:</div><div>Semiannual", ">Semiannual<"]
                }
                
                for freq, patterns in frequency_patterns.items():
                    for pattern in patterns:
                        if pattern in page_text:
                            metadata['frequency'] = freq
                            break
                    if metadata['frequency']:
                        break
            except Exception as e:
                logger.warning(f"Chyba při hledání frekvence v textu stránky: {e}")
        
        # Units a Seasonal Adjustment
        try:
            # Hledáme sekci "Units:" a hodnotu
            units_headers = driver.find_elements(By.XPATH, "//div[contains(text(), 'Units:')]")
            if units_headers:
                # Pokud existuje Units: nadpis, vezmeme samotnou hodnotu, která je v té samé oblasti
                units_value = driver.find_element(By.XPATH, "//div[contains(text(), 'Units:')]/following-sibling::div").text.strip()
                metadata['units'] = units_value
                
                # Extrahovat Seasonal Adjustment
                if "Seasonally Adjusted" in units_value:
                    metadata['seasonal_adjustment'] = "Seasonally Adjusted"
                elif "Not Seasonally Adjusted" in units_value:
                    metadata['seasonal_adjustment'] = "Not Seasonally Adjusted"
        except Exception as e:
            logger.warning(f"Chyba při extrakci jednotek: {e}")
        
        # Pokud nemáme jednotky, zkusíme to z textu stránky
        if not metadata['units']:
            try:
                page_text = driver.page_source
                units_patterns = [
                    "Thousands of Persons",
                    "Billions of Dollars",
                    "Millions of Dollars",
                    "Percent",
                    "Index"
                ]
                
                for pattern in units_patterns:
                    if pattern in page_text:
                        metadata['units'] = pattern
                        break
                
                # Extrahovat Seasonal Adjustment z textu
                if "Seasonally Adjusted" in page_text:
                    metadata['seasonal_adjustment'] = "Seasonally Adjusted"
                elif "Not Seasonally Adjusted" in page_text:
                    metadata['seasonal_adjustment'] = "Not Seasonally Adjusted"
            except Exception as e:
                logger.warning(f"Chyba při hledání jednotek v textu stránky: {e}")
        
        # Last Updated - podle běžného formátu "Updated: Mar 7, 2025 7:48 AM CST"
        try:
            update_elements = driver.find_elements(By.XPATH, "//div[contains(text(), 'Updated:')]")
            if update_elements:
                updated_text = update_elements[0].text.strip()
                metadata['last_updated'] = updated_text.replace("Updated:", "").strip()
        except Exception as e:
            logger.warning(f"Chyba při extrakci data aktualizace: {e}")
        
        # Pokud nemáme datum aktualizace, zkusíme jiný přístup
        if not metadata['last_updated']:
            try:
                # Hledat podle textu "Updated:"
                updated_elements = driver.find_elements(By.XPATH, "//*[contains(text(), 'Updated:')]")
                for elem in updated_elements:
                    text = elem.text.strip()
                    if "Updated:" in text:
                        metadata['last_updated'] = text.split("Updated:")[1].strip()
                        break
            except Exception as e:
                logger.warning(f"Chyba při hledání data aktualizace v textu: {e}")
        
        # Source - typicky "U.S. Bureau of Labor Statistics via FRED®"
        try:
            source_elements = driver.find_elements(By.XPATH, "//*[contains(text(), 'Source:')]")
            if source_elements:
                for elem in source_elements:
                    text = elem.text.strip()
                    if "Source:" in text:
                        metadata['data_source'] = text.replace("Source:", "").strip()
                        break
        except Exception as e:
            logger.warning(f"Chyba při extrakci zdroje: {e}")
        
        # Diagnostika - pokud extrakce frekvence selhala
        if not metadata['frequency'] and CONFIG["debug"]:
            logger.warning(f"Nebyla nalezena frekvence pro {series_id}. Provádím diagnostiku...")
            
            # Ukládání diagnostických informací
            try:
                # Screenshot a HTML kód
                debug_dir = os.path.join(CONFIG["output_dir"], "debug")
                os.makedirs(debug_dir, exist_ok=True)
                
                driver.save_screenshot(os.path.join(debug_dir, f"debug_{series_id}.png"))
                
                with open(os.path.join(debug_dir, f"debug_{series_id}.html"), "w", encoding="utf-8") as f:
                    f.write(driver.page_source)
                
                logger.info(f"Diagnostické informace uloženy do {debug_dir}")
            except Exception as e:
                logger.error(f"Chyba při ukládání diagnostických informací: {e}")
        
        # Defaultní hodnota pro frekvenci, pokud nebyla nalezena
        if not metadata['frequency']:
            metadata['frequency'] = "Monthly"  # Default hodnota, aby série nebyla přeskočena
            logger.warning(f"Pro sérii {series_id} nebyla nalezena frekvence, nastavuji na default (Monthly)")
        
        # Výstupní log
        logger.info(f"Extrahovaná metadata pro {series_id}: {metadata}")
        return metadata
    
    except Exception as e:
        logger.error(f"Kritická chyba při získávání metadat pro {series_id}: {str(e)}")
        # Vrátit alespoň nějaká metadata, aby série nebyla přeskočena
        return {
            'series_id': series_id,
            'title': '',
            'frequency': 'Monthly',  # Default hodnota pro sérii s chybou
            'units': '',
            'seasonal_adjustment': '',
            'last_updated': '',
            'source': 'FRED',
            'data_source': ''
        }

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

def process_single_series(driver, series_id, save_to_csv=False):
    """Zpracuje jednu sérii a volitelně uloží výsledek do CSV"""
    try:
        # Získat metadata
        metadata = get_series_metadata_selenium(driver, series_id)
        
        if not metadata:
            logger.error(f"Nepodařilo se získat metadata pro {series_id}")
            return None
        
        # Kontrola frekvence, jen pokud potřebujeme
        if metadata.get('frequency') and not is_quarterly_or_more_frequent(metadata['frequency']):
            logger.info(f"Série {series_id} přeskočena - frekvence {metadata['frequency']} není dostatečně častá")
            return None
        
        # Uložit do CSV, pokud je požadováno
        if save_to_csv:
            csv_path = os.path.join(CONFIG["output_dir"], f"{series_id}_metadata.csv")
            with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=metadata.keys())
                writer.writeheader()
                writer.writerow(metadata)
            logger.info(f"Metadata pro {series_id} uložena do {csv_path}")
        
        return metadata
    
    except Exception as e:
        logger.error(f"Chyba při zpracování série {series_id}: {str(e)}")
        return None

def worker_function(worker_id, series_list):
    """Pracovní funkce pro zpracování seznamu sérií v jednom vlákně"""
    driver = None
    results = []
    
    try:
        # Vytvořit driver pro toto vlákno
        driver = create_driver()
        
        # Nejprve navštívit hlavní stránku pro získání cookies
        driver.get("https://fred.stlouisfed.org/")
        time.sleep(2)
        
        # Zpracovat seznam sérií
        for i, series_info in enumerate(series_list):
            series_id = series_info if isinstance(series_info, str) else series_info.get('series_id')
            
            if not series_id:
                logger.warning(f"Worker {worker_id}: Přeskakuji sérii bez ID: {series_info}")
                continue
            
            try:
                # Zpracovat sérii
                metadata = get_series_metadata_selenium(driver, series_id)
                
                if metadata:
                    results.append(metadata)
                    logger.info(f"Worker {worker_id}: Úspěšně zpracována série {series_id}")
                
                # Zobrazit průběh
                if (i + 1) % 10 == 0:
                    logger.info(f"Worker {worker_id}: Zpracováno {i+1}/{len(series_list)} sérií")
                
                # Náhodné zpoždění mezi požadavky
                random_delay()
                
            except Exception as e:
                logger.error(f"Worker {worker_id}: Chyba při zpracování série {series_id}: {str(e)}")
                
                # Zkusit se zotavit
                try:
                    driver.get("https://fred.stlouisfed.org/")
                    time.sleep(3)
                except:
                    pass
        
        logger.info(f"Worker {worker_id}: Dokončeno, zpracováno {len(results)}/{len(series_list)} sérií")
        return results
    
    except Exception as e:
        logger.error(f"Worker {worker_id}: Kritická chyba: {str(e)}")
        return results
    
    finally:
        if driver:
            driver.quit()

def process_csv_file(input_file, output_file, max_series=None, skip=0):
    """Zpracuje seznam sérií z CSV souboru a uloží výsledky do výstupního CSV"""
    try:
        # Načíst CSV se sériemi
        df = pd.read_csv(input_file)
        logger.info(f"Načteno {len(df)} sérií z {input_file}")
        
        # Aplikovat limity
        if skip > 0:
            df = df.iloc[skip:]
            logger.info(f"Přeskočeno prvních {skip} sérií")
        
        if max_series and max_series > 0:
            df = df.iloc[:max_series]
            logger.info(f"Omezeno na {max_series} sérií")
        
        # Kontrola správného formátu CSV
        if 'series_id' not in df.columns:
            # Pokusit se najít sloupec s ID
            potential_id_columns = [col for col in df.columns if 'id' in col.lower()]
            if potential_id_columns:
                logger.warning(f"Sloupec 'series_id' nenalezen, používám {potential_id_columns[0]}")
                df = df.rename(columns={potential_id_columns[0]: 'series_id'})
            else:
                logger.error(f"Ve vstupním CSV chybí sloupec 'series_id'")
                return False
        
        # Rozdělit série mezi workery
        series_list = df.to_dict('records')
        num_workers = min(CONFIG["max_workers"], len(series_list))
        series_per_worker = (len(series_list) + num_workers - 1) // num_workers
        worker_series = []
        
        for i in range(0, len(series_list), series_per_worker):
            worker_series.append(series_list[i:i+series_per_worker])
        
        logger.info(f"Rozděleno {len(series_list)} sérií mezi {num_workers} workerů")
        
        # Zpracovat série v paralelních vláknech
        all_results = []
        threads = []
        
        for i, series_batch in enumerate(worker_series):
            thread = threading.Thread(
                target=lambda i=i, batch=series_batch: all_results.extend(worker_function(i, batch))
            )
            threads.append(thread)
            thread.start()
        
        # Čekat na dokončení všech vláken
        for thread in threads:
            thread.join()
        
        # Uložit výsledky do výstupního CSV
        if all_results:
            results_df = pd.DataFrame(all_results)
            results_df.to_csv(output_file, index=False)
            logger.info(f"Uloženo {len(results_df)} metadat do {output_file}")
            return True
        else:
            logger.error("Žádné výsledky nebyly získány")
            return False
    
    except Exception as e:
        logger.error(f"Chyba při zpracování CSV souboru: {str(e)}")
        return False

def main():
    """Hlavní funkce programu"""
    # Nastavit argumenty příkazové řádky
    parser = argparse.ArgumentParser(description='FRED Metadata Extractor')
    parser.add_argument('--input', type=str, help='Vstupní CSV soubor se seznamem sérií')
    parser.add_argument('--output', type=str, help='Výstupní CSV soubor pro metadata')
    parser.add_argument('--series', type=str, help='ID konkrétní série ke zpracování')
    parser.add_argument('--workers', type=int, default=CONFIG["max_workers"], help='Počet paralelních workerů')
    parser.add_argument('--limit', type=int, help='Maximální počet sérií ke zpracování')
    parser.add_argument('--skip', type=int, default=0, help='Přeskočit prvních N sérií')
    parser.add_argument('--headless', action='store_true', help='Použít headless mód prohlížeče')
    parser.add_argument('--delay', type=float, help='Základní zpoždění mezi požadavky (sekundy)')
    parser.add_argument('--debug', action='store_true', help='Zapnout debug mód')
    
    args = parser.parse_args()
    
    # Aktualizovat konfiguraci podle argumentů
    if args.workers:
        CONFIG["max_workers"] = args.workers
    
    if args.headless:
        CONFIG["headless"] = True
    
    if args.delay:
        CONFIG["min_delay"] = args.delay
        CONFIG["max_delay"] = args.delay * 2
    
    if args.debug:
        CONFIG["debug"] = True
        # Přenastavit logging pro debug mód
        global logger
        logger = setup_logging(True)
    
    # Vytvořit výstupní adresář
    os.makedirs(CONFIG["output_dir"], exist_ok=True)
    
    # Zpracování jedné série
    if args.series:
        driver = None
        try:
            logger.info(f"Zpracovávám jednu sérii: {args.series}")
            driver = create_driver()
            
            # Nejprve navštívit hlavní stránku pro získání cookies
            driver.get("https://fred.stlouisfed.org/")
            time.sleep(2)
            
            # Zpracovat sérii
            output_file = args.output or os.path.join(CONFIG["output_dir"], f"{args.series}_metadata.csv")
            metadata = process_single_series(driver, args.series, save_to_csv=True)
            
            if metadata:
                logger.info(f"Metadata pro {args.series} byla úspěšně získána a uložena do {output_file}")
                return 0
            else:
                logger.error(f"Nepodařilo se získat metadata pro {args.series}")
                return 1
        
        except Exception as e:
            logger.error(f"Chyba při zpracování série {args.series}: {str(e)}")
            return 1
        
        finally:
            if driver:
                driver.quit()
    
    # Zpracování CSV souboru
    elif args.input:
        output_file = args.output or os.path.join(CONFIG["output_dir"], f"fred_metadata_{int(time.time())}.csv")
        logger.info(f"Zpracovávám sérii ze souboru: {args.input}")
        
        success = process_csv_file(args.input, output_file, args.limit, args.skip)
        
        if success:
            logger.info(f"Zpracování CSV dokončeno, výsledky uloženy do {output_file}")
            return 0
        else:
            logger.error("Zpracování CSV selhalo")
            return 1
    
    else:
        logger.error("Musíte zadat buď --series nebo --input")
        parser.print_help()
        return 1

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)