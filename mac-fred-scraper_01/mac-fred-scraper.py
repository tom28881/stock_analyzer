from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import time
import random
import logging
import os
import pandas as pd
import argparse

# Nastavení logování
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("fred_mac_scraper.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Globální sety pro sledování zpracovaných URL
processed_categories = set()
processed_series = set()
all_series_list = []

def random_delay(min_delay=2, max_delay=5):
    """Náhodné zpoždění pro simulaci lidského chování"""
    delay = min_delay + random.random() * (max_delay - min_delay)
    time.sleep(delay)

def create_driver(proxy=None):
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
    if proxy:
        logger.info(f"Používám proxy: {proxy}")
        options.add_argument(f'--proxy-server={proxy}')
    
    # Vytvořit driver
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(30)
    
    # Obejití detekce
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    return driver

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
                
        except TimeoutException:
            logger.warning(f"Timeout při načítání {url}, pokus {attempt+1}/{max_retries}")
            if attempt < max_retries - 1:
                time.sleep(5)
            else:
                logger.error(f"Nepodařilo se načíst {url} po {max_retries} pokusech")
                return False
        except Exception as e:
            logger.error(f"Chyba při načítání {url}: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(5)
            else:
                return False
    
    return False

def extract_series(driver, category_url):
    """Extrahuje série z aktuální kategorie"""
    series_list = []
    try:
        series_links = driver.find_elements(By.CSS_SELECTOR, 'a[href*="/series/"]')
        
        for link in series_links:
            try:
                href = link.get_attribute('href')
                if href and '/series/' in href:
                    series_id = href.split('/')[-1]
                    
                    if href not in processed_series:
                        processed_series.add(href)
                        series_list.append({
                            'series_id': series_id,
                            'name': link.text.strip(),
                            'url': href,
                            'source_category': category_url
                        })
            except Exception as e:
                logger.error(f"Chyba při extrakci série: {str(e)}")
        
        logger.info(f"Nalezeno {len(series_list)} sérií v kategorii {category_url}")
        return series_list
        
    except Exception as e:
        logger.error(f"Chyba při extrakci sérií z {category_url}: {str(e)}")
        return []

def extract_subcategories(driver, parent_url):
    """Extrahuje podkategorie z aktuální stránky"""
    subcategories = []
    try:
        category_links = driver.find_elements(By.CSS_SELECTOR, 'a[href*="/categories/"]')
        
        for link in category_links:
            try:
                href = link.get_attribute('href')
                if href and '/categories/' in href and href != 'https://fred.stlouisfed.org/categories/':
                    if href not in processed_categories and href != parent_url:
                        category_id = href.split('/')[-1]
                        category_name = link.text.strip()
                        
                        subcategories.append({
                            'id': category_id,
                            'name': category_name,
                            'url': href
                        })
            except Exception as e:
                logger.error(f"Chyba při extrakci podkategorie: {str(e)}")
        
        logger.info(f"Nalezeno {len(subcategories)} podkategorií v {parent_url}")
        return subcategories
        
    except Exception as e:
        logger.error(f"Chyba při extrakci podkategorií z {parent_url}: {str(e)}")
        return []

def check_pagination(driver, current_url):
    """Kontroluje a vrací odkaz na další stránku, pokud existuje"""
    try:
        next_links = driver.find_elements(By.XPATH, "//ul[contains(@class, 'pagination')]//a[text()='Next']")
        for link in next_links:
            next_url = link.get_attribute('href')
            if next_url and next_url != current_url and next_url not in processed_categories:
                return next_url
        return None
    except Exception as e:
        logger.error(f"Chyba při kontrole paginace: {str(e)}")
        return None

def save_results(filename='fred_series.csv'):
    """Uloží výsledky do CSV souboru"""
    # Deduplikace
    unique_series = []
    seen_ids = set()
    
    for series in all_series_list:
        if series['series_id'] not in seen_ids:
            seen_ids.add(series['series_id'])
            unique_series.append(series)
    
    # Uložit do CSV
    df = pd.DataFrame(unique_series)
    df.to_csv(filename, index=False)
    logger.info(f"Uloženo {len(unique_series)} unikátních sérií do {filename}")
    return filename

def crawl_recursive(proxy=None, start_url='https://fred.stlouisfed.org/categories/'):
    """Rekurzivní procházení FRED kategorií"""
    driver = None
    try:
        driver = create_driver(proxy)
        categories_to_process = [{'url': start_url, 'name': 'Root Category'}]
        
        # Nejprve načíst hlavní stránku pro cookies
        logger.info("Navštěvuji hlavní stránku FRED...")
        if not safe_get_url(driver, "https://fred.stlouisfed.org/"):
            logger.error("Nelze přistoupit k hlavní stránce FRED! Zkontrolujte proxy nebo síťové připojení.")
            return False
        
        categories_processed = 0
        start_time = time.time()
        
        # Rekurzivní zpracování kategorií
        while categories_to_process:
            # Vzít kategorii z fronty
            category = categories_to_process.pop(0)
            category_url = category['url']
            category_name = category.get('name', 'Unknown')
            
            if category_url in processed_categories:
                continue
            
            logger.info(f"Zpracovávám kategorii: {category_name} ({category_url})")
            
            # Načíst stránku kategorie
            if not safe_get_url(driver, category_url):
                logger.warning(f"Přeskakuji kategorii {category_name}, nelze načíst")
                continue
            
            # Označit jako zpracovanou
            processed_categories.add(category_url)
            categories_processed += 1
            
            # 1. Získat série v této kategorii
            series_list = extract_series(driver, category_url)
            all_series_list.extend(series_list)
            
            # 2. Zkontrolovat paginaci
            next_page = check_pagination(driver, category_url)
            if next_page:
                categories_to_process.append({
                    'url': next_page,
                    'name': f"{category_name} (další stránka)"
                })
            
            # 3. Získat podkategorie
            subcategories = extract_subcategories(driver, category_url)
            categories_to_process.extend(subcategories)
            
            # Zobrazit průběžné statistiky
            if categories_processed % 10 == 0:
                elapsed = time.time() - start_time
                logger.info(f"Postup: {categories_processed} kategorií zpracováno, "
                           f"{len(categories_to_process)} zbývá, "
                           f"{len(all_series_list)} sérií nalezeno, "
                           f"čas: {elapsed:.2f}s")
                
                # Průběžné ukládání
                save_results(f'fred_series_progress_{int(time.time())}.csv')
        
        # Finální uložení
        csv_file = save_results(f'fred_series_complete_{int(time.time())}.csv')
        elapsed = time.time() - start_time
        logger.info(f"Crawling dokončen za {elapsed:.2f}s. Zpracováno {categories_processed} kategorií, "
                   f"nalezeno {len(all_series_list)} sérií (před deduplikací)")
        return csv_file
        
    except Exception as e:
        logger.error(f"Neočekávaná chyba: {str(e)}", exc_info=True)
        return False
    
    finally:
        if driver:
            driver.quit()

def test_proxy(proxy):
    """Otestuje, zda proxy funguje pro přístup k FRED"""
    driver = None
    try:
        logger.info(f"Testuji proxy: {proxy}")
        driver = create_driver(proxy)
        
        # Zkusit přístup na hlavní stránku
        if not safe_get_url(driver, "https://fred.stlouisfed.org/"):
            logger.error(f"Proxy {proxy} nefunguje pro přístup k FRED (hlavní stránka)")
            return False
        
        # Zkusit přístup ke kategoriím
        if not safe_get_url(driver, "https://fred.stlouisfed.org/categories/"):
            logger.error(f"Proxy {proxy} nefunguje pro přístup ke kategoriím FRED")
            return False
        
        # Zkusit přístup ke konkrétní kategorii
        if not safe_get_url(driver, "https://fred.stlouisfed.org/categories/33940"):
            logger.error(f"Proxy {proxy} nefunguje pro přístup ke konkrétní kategorii")
            return False
        
        logger.info(f"Proxy {proxy} funguje pro přístup k FRED!")
        return True
    
    except Exception as e:
        logger.error(f"Chyba při testování proxy {proxy}: {str(e)}")
        return False
    
    finally:
        if driver:
            driver.quit()

def main():
    parser = argparse.ArgumentParser(description='Jednoduchý FRED scraper pro macOS')
    parser.add_argument('--proxy', type=str, help='Proxy server v formátu ip:port')
    parser.add_argument('--test', action='store_true', help='Pouze otestovat přístup, nestahovat data')
    parser.add_argument('--category', type=str, help='Začít od konkrétní kategorie (URL)')
    
    args = parser.parse_args()
    
    if args.test:
        if args.proxy:
            test_proxy(args.proxy)
        else:
            logger.info("Testuji přístup bez proxy...")
            test_proxy(None)
    else:
        # Spustit crawling
        start_url = args.category if args.category else 'https://fred.stlouisfed.org/categories/'
        proxy = args.proxy
        
        logger.info(f"Spouštím crawling od: {start_url}")
        logger.info(f"Proxy: {proxy if proxy else 'nepoužívám'}")
        
        crawl_recursive(proxy, start_url)

if __name__ == "__main__":
    main()