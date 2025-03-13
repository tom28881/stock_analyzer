import sqlite3
import logging
import pandas as pd
from datetime import datetime, timedelta
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
import json
import matplotlib.pyplot as plt
import seaborn as sns
from concurrent.futures import ThreadPoolExecutor
import time
import threading
import sys
import argparse

# Přidat cestu k modulům
script_dir = os.path.dirname(os.path.abspath(__file__))
if script_dir not in sys.path:
    sys.path.append(script_dir)

# Import nových modulů
try:
    # Importovat nové funkce z aktualizovaných modulů
    from updated_database_module import (
        create_driver, store_series, create_database, 
        CONFIG as DB_CONFIG
    )
    from updated_daily_updater import (
        extract_series_data_selenium, update_series_selenium,
        CONFIG as UPDATER_CONFIG
    )
    selenium_available = True
except ImportError:
    # Záložní import, pokud nové moduly nejsou dostupné
    try:
        from database import store_series
        from series_scraper import scrape_series_data
        selenium_available = False
    except ImportError:
        print("CHYBA: Nelze importovat potřebné moduly!")
        sys.exit(1)

# Nastavení logování
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("fred_monitoring.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class FredMonitor:
    def __init__(self, config_file='monitoring_config.json', db_file=None):
        """Inicializace monitorovacího systému"""
        # Výchozí konfigurace
        self.config = {
            'email_notifications': False,
            'email_from': '',
            'email_to': [],
            'smtp_server': '',
            'smtp_port': 587,
            'smtp_user': '',
            'smtp_password': '',
            'error_threshold': 100,  # počet chyb, po kterém se odešle notifikace
            'daily_report': True,
            'report_dir': 'reports',
            'retry_failed': True,
            'retry_limit': 3,
            'retry_delay': 300,  # 5 minut
            'max_workers': 3,    # počet paralelních workerů pro retry
            'use_proxy': None,   # proxy server (volitelné)
            'db_file': db_file or 'fred_data.db',
            'use_selenium': selenium_available  # automaticky detekovat, zda je Selenium k dispozici
        }
        
        # Načíst konfiguraci, pokud existuje
        self.load_config(config_file)
        
        # Vytvořit adresář pro reporty, pokud neexistuje
        if self.config['daily_report']:
            os.makedirs(self.config['report_dir'], exist_ok=True)
    
    def load_config(self, config_file):
        """Načtení konfigurace z JSON souboru"""
        try:
            if os.path.exists(config_file):
                with open(config_file, 'r') as f:
                    loaded_config = json.load(f)
                    self.config.update(loaded_config)
                logger.info("Konfigurace úspěšně načtena")
            else:
                logger.warning(f"Konfigurační soubor {config_file} nenalezen, použije se výchozí konfigurace")
                # Uložit výchozí konfiguraci
                with open(config_file, 'w') as f:
                    json.dump(self.config, f, indent=2)
        except Exception as e:
            logger.error(f"Chyba při načítání konfigurace: {str(e)}")
    
    def check_database_integrity(self):
        """Kontrola integrity databáze"""
        try:
            conn = sqlite3.connect(self.config['db_file'])
            cursor = conn.cursor()
            
            # Kontrola existence a struktury tabulek
            tables = [
                ("series_metadata", ["series_id", "title", "frequency", "units", "seasonal_adjustment", "last_updated", "last_checked"]),
                ("series_values", ["series_id", "date", "value"]),
                ("update_log", ["id", "timestamp", "series_id", "action", "status", "message"])
            ]
            
            issues = []
            
            for table_name, expected_columns in tables:
                # Kontrola existence tabulky
                cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
                if not cursor.fetchone():
                    issues.append(f"Tabulka {table_name} neexistuje")
                    continue
                
                # Kontrola sloupců
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = [row[1] for row in cursor.fetchall()]
                
                for col in expected_columns:
                    if col not in columns:
                        issues.append(f"Chybí sloupec {col} v tabulce {table_name}")
            
            # Kontrola dat
            cursor.execute("SELECT COUNT(*) FROM series_metadata")
            series_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM series_values")
            values_count = cursor.fetchone()[0]
            
            conn.close()
            
            integrity_info = {
                'issues': issues,
                'tables_ok': len(issues) == 0,
                'series_count': series_count,
                'values_count': values_count
            }
            
            if issues:
                logger.warning(f"Nalezeny problémy s integritou databáze: {issues}")
            else:
                logger.info(f"Databáze je v pořádku. Počet sérií: {series_count}, Počet hodnot: {values_count}")
            
            return integrity_info
        
        except Exception as e:
            logger.error(f"Chyba při kontrole integrity databáze: {str(e)}")
            return {
                'issues': [f"Chyba při kontrole: {str(e)}"],
                'tables_ok': False,
                'series_count': 0,
                'values_count': 0
            }
    
    def analyze_errors(self, days=1):
        """Analýza chyb za posledních X dní"""
        try:
            conn = sqlite3.connect(self.config['db_file'])
            
            # Získat log za posledních X dní
            start_date = (datetime.now() - timedelta(days=days)).isoformat()
            
            query = f"""
            SELECT timestamp, series_id, action, status, message
            FROM update_log
            WHERE timestamp >= '{start_date}'
            ORDER BY timestamp DESC
            """
            
            df = pd.read_sql_query(query, conn)
            conn.close()
            
            if df.empty:
                logger.info(f"Žádné záznamy za posledních {days} dní")
                return {
                    'total_logs': 0,
                    'error_count': 0,
                    'success_count': 0,
                    'error_rate': 0,
                    'most_common_errors': [],
                    'affected_series': []
                }
            
            # Transformace a analýza
            df['date'] = pd.to_datetime(df['timestamp'])
            
            # Základní metriky
            error_logs = df[df['status'] == 'ERROR']
            success_logs = df[df['status'] == 'SUCCESS']
            
            total_logs = len(df)
            error_count = len(error_logs)
            success_count = len(success_logs)
            error_rate = error_count / total_logs if total_logs > 0 else 0
            
            # Nejčastější chyby
            most_common_errors = []
            if not error_logs.empty:
                error_counts = error_logs['message'].value_counts().head(10)
                most_common_errors = [
                    {'message': message, 'count': count}
                    for message, count in error_counts.items()
                ]
            
            # Série s nejvíce chybami
            affected_series = []
            if not error_logs.empty:
                series_error_counts = error_logs['series_id'].value_counts().head(10)
                affected_series = [
                    {'series_id': series_id, 'error_count': count}
                    for series_id, count in series_error_counts.items()
                ]
            
            # Výsledky analýzy
            analysis_result = {
                'total_logs': total_logs,
                'error_count': error_count,
                'success_count': success_count,
                'error_rate': error_rate,
                'most_common_errors': most_common_errors,
                'affected_series': affected_series
            }
            
            logger.info(f"Analýza chyb za posledních {days} dní: "
                       f"Celkem záznamů: {total_logs}, Chyby: {error_count}, "
                       f"Úspěch: {success_count}, Míra chybovosti: {error_rate:.2%}")
            
            return analysis_result
        
        except Exception as e:
            logger.error(f"Chyba při analýze chyb: {str(e)}")
            return {
                'total_logs': 0,
                'error_count': 0,
                'success_count': 0,
                'error_rate': 0,
                'most_common_errors': [],
                'affected_series': [],
                'error': str(e)
            }
    
    def _retry_single_series_selenium(self, driver, series_id):
        """Opakovat stažení jedné série pomocí Selenia"""
        try:
            logger.info(f"Pokus o znovustažení série {series_id} pomocí Selenia")
            
            # Stáhnout data
            series_data = extract_series_data_selenium(driver, series_id)
            
            if not series_data:
                logger.warning(f"Nepodařilo se získat data pro {series_id}")
                
                # Zaznamenat pokus
                conn = sqlite3.connect(self.config['db_file'])
                cursor = conn.cursor()
                cursor.execute('''
                INSERT INTO update_log (timestamp, series_id, action, status, message)
                VALUES (?, ?, ?, ?, ?)
                ''', (
                    datetime.now().isoformat(),
                    series_id,
                    'RETRY',
                    'ERROR',
                    "Nepodařilo se získat data"
                ))
                conn.commit()
                conn.close()
                return False
            
            # Uložit do databáze
            success = store_series(series_data)
            
            if success:
                logger.info(f"Série {series_id} úspěšně znovustažena")
                
                # Zaznamenat úspěch
                conn = sqlite3.connect(self.config['db_file'])
                cursor = conn.cursor()
                cursor.execute('''
                INSERT INTO update_log (timestamp, series_id, action, status, message)
                VALUES (?, ?, ?, ?, ?)
                ''', (
                    datetime.now().isoformat(),
                    series_id,
                    'RETRY',
                    'SUCCESS',
                    f"Úspěšně znovustaženo s {len(series_data.get('data', []))} datovými body"
                ))
                conn.commit()
                conn.close()
            else:
                logger.warning(f"Problém při ukládání dat pro {series_id}")
                
                # Zaznamenat neúspěch
                conn = sqlite3.connect(self.config['db_file'])
                cursor = conn.cursor()
                cursor.execute('''
                INSERT INTO update_log (timestamp, series_id, action, status, message)
                VALUES (?, ?, ?, ?, ?)
                ''', (
                    datetime.now().isoformat(),
                    series_id,
                    'RETRY',
                    'ERROR',
                    "Problém při ukládání dat"
                ))
                conn.commit()
                conn.close()
            
            return success
        
        except Exception as e:
            logger.error(f"Chyba při znovustažení série {series_id}: {str(e)}")
            
            # Zaznamenat chybu
            try:
                conn = sqlite3.connect(self.config['db_file'])
                cursor = conn.cursor()
                cursor.execute('''
                INSERT INTO update_log (timestamp, series_id, action, status, message)
                VALUES (?, ?, ?, ?, ?)
                ''', (
                    datetime.now().isoformat(),
                    series_id,
                    'RETRY',
                    'ERROR',
                    str(e)
                ))
                conn.commit()
                conn.close()
            except:
                pass
            
            return False
    
    def _retry_single_series(self, series_id):
        """Opakovat stažení jedné série (kompatibilita se starším API)"""
        if self.config['use_selenium']:
            driver = None
            try:
                # Vytvořit dočasný driver
                driver = create_driver()
                # Zpoždění před pokusem (při opakování)
                time.sleep(self.config['retry_delay'])
                return self._retry_single_series_selenium(driver, series_id)
            finally:
                if driver:
                    driver.quit()
        else:
            try:
                logger.info(f"Pokus o znovustažení série {series_id}")
                
                # Zpoždění před pokusem (při opakování)
                time.sleep(self.config['retry_delay'])
                
                # Stáhnout data
                series_data = scrape_series_data(series_id)
                
                if not series_data:
                    logger.warning(f"Nepodařilo se získat data pro {series_id}")
                    
                    # Zaznamenat pokus
                    conn = sqlite3.connect(self.config['db_file'])
                    cursor = conn.cursor()
                    cursor.execute('''
                    INSERT INTO update_log (timestamp, series_id, action, status, message)
                    VALUES (?, ?, ?, ?, ?)
                    ''', (
                        datetime.now().isoformat(),
                        series_id,
                        'RETRY',
                        'ERROR',
                        "Nepodařilo se získat data"
                    ))
                    conn.commit()
                    conn.close()
                    return False
                
                # Uložit do databáze
                success = store_series(series_data)
                
                if success:
                    logger.info(f"Série {series_id} úspěšně znovustažena")
                    
                    # Zaznamenat úspěch
                    conn = sqlite3.connect(self.config['db_file'])
                    cursor = conn.cursor()
                    cursor.execute('''
                    INSERT INTO update_log (timestamp, series_id, action, status, message)
                    VALUES (?, ?, ?, ?, ?)
                    ''', (
                        datetime.now().isoformat(),
                        series_id,
                        'RETRY',
                        'SUCCESS',
                        f"Úspěšně znovustaženo s {len(series_data.get('data', []))} datovými body"
                    ))
                    conn.commit()
                    conn.close()
                else:
                    logger.warning(f"Problém při ukládání dat pro {series_id}")
                    
                    # Zaznamenat neúspěch
                    conn = sqlite3.connect(self.config['db_file'])
                    cursor = conn.cursor()
                    cursor.execute('''
                    INSERT INTO update_log (timestamp, series_id, action, status, message)
                    VALUES (?, ?, ?, ?, ?)
                    ''', (
                        datetime.now().isoformat(),
                        series_id,
                        'RETRY',
                        'ERROR',
                        "Problém při ukládání dat"
                    ))
                    conn.commit()
                    conn.close()
                
                return success
            
            except Exception as e:
                logger.error(f"Chyba při znovustažení série {series_id}: {str(e)}")
                
                # Zaznamenat chybu
                try:
                    conn = sqlite3.connect(self.config['db_file'])
                    cursor = conn.cursor()
                    cursor.execute('''
                    INSERT INTO update_log (timestamp, series_id, action, status, message)
                    VALUES (?, ?, ?, ?, ?)
                    ''', (
                        datetime.now().isoformat(),
                        series_id,
                        'RETRY',
                        'ERROR',
                        str(e)
                    ))
                    conn.commit()
                    conn.close()
                except:
                    pass
                
                return False
    
    def retry_worker(self, worker_id, series_list, results):
        """Worker vlákno pro opakování stažení sérií"""
        driver = None
        try:
            if self.config['use_selenium']:
                driver = create_driver()
                # Nejprve navštívit hlavní stránku pro získání cookies
                driver.get("https://fred.stlouisfed.org/")
                time.sleep(2)
            
            for i, series_id in enumerate(series_list):
                try:
                    if self.config['use_selenium'] and driver:
                        success = self._retry_single_series_selenium(driver, series_id)
                    else:
                        success = self._retry_single_series(series_id)
                    
                    if success:
                        results['success'] += 1
                    else:
                        results['failed'] += 1
                    
                    # Zobrazit průběh
                    if (i + 1) % 5 == 0 or i == len(series_list) - 1:
                        logger.info(f"Worker {worker_id}: Zpracováno {i+1}/{len(series_list)} sérií")
                    
                    # Zpoždění mezi požadavky
                    time.sleep(2 + random.random() * 2)
                    
                except Exception as e:
                    results['failed'] += 1
                    logger.error(f"Worker {worker_id}: Chyba při zpracování {series_id}: {str(e)}")
                    
                    # Zkusit se zotavit, pokud používáme Selenium
                    if self.config['use_selenium'] and driver:
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
    
    def retry_failed_series(self, days=1, max_retries=None):
        """Opakovat stažení sérií, které selhaly"""
        if not self.config['retry_failed']:
            logger.info("Opakované stahování je vypnuto v konfiguraci")
            return False
        
        max_retries = max_retries or self.config['retry_limit']
        
        try:
            # Získat seznam sérií, které selhaly
            conn = sqlite3.connect(self.config['db_file'])
            
            start_date = (datetime.now() - timedelta(days=days)).isoformat()
            
            query = f"""
            SELECT DISTINCT series_id
            FROM update_log
            WHERE timestamp >= '{start_date}'
            AND status = 'ERROR'
            AND series_id != 'SYSTEM'
            """
            
            df = pd.read_sql_query(query, conn)
            
            # Filtrovat série, které již dosáhly maximálního počtu pokusů
            filtered_series = []
            for _, row in df.iterrows():
                series_id = row['series_id']
                
                retry_query = f"""
                SELECT COUNT(*) FROM update_log
                WHERE series_id = '{series_id}'
                AND timestamp >= '{start_date}'
                AND action = 'RETRY'
                """
                retry_count = pd.read_sql_query(retry_query, conn).iloc[0, 0]
                
                if retry_count < max_retries:
                    filtered_series.append(series_id)
                else:
                    logger.info(f"Série {series_id} již dosáhla limitu pokusů ({retry_count}/{max_retries})")
            
            conn.close()
            
            if not filtered_series:
                logger.info("Žádné série ke znovustažení")
                return True
            
            logger.info(f"Nalezeno {len(filtered_series)} sérií ke znovustažení")
            
            # Počítadla výsledků
            results = {
                'total': len(filtered_series),
                'success': 0,
                'failed': 0
            }
            
            # Pokud máme jen pár sérií, stačí sekvenční zpracování
            if len(filtered_series) <= 5:
                logger.info("Spouštím sekvenční zpracování pro malý počet sérií")
                for series_id in filtered_series:
                    success = self._retry_single_series(series_id)
                    if success:
                        results['success'] += 1
                    else:
                        results['failed'] += 1
            else:
                # Jinak použít paralelní zpracování
                # Rozdělit série mezi workery
                max_workers = min(self.config['max_workers'], len(filtered_series))
                series_per_worker = (len(filtered_series) + max_workers - 1) // max_workers
                worker_series = []
                
                for i in range(0, len(filtered_series), series_per_worker):
                    worker_series.append(filtered_series[i:i+series_per_worker])
                
                # Spustit worker vlákna
                threads = []
                for i, series_batch in enumerate(worker_series):
                    thread = threading.Thread(
                        target=self.retry_worker,
                        args=(i, series_batch, results)
                    )
                    threads.append(thread)
                    thread.start()
                
                # Čekat na dokončení všech vláken
                for thread in threads:
                    thread.join()
            
            logger.info(f"Opakované stažení dokončeno: {results['success']} úspěšných, {results['failed']} neúspěšných")
            return True
        
        except Exception as e:
            logger.error(f"Chyba při opakování stažení sérií: {str(e)}")
            return False
    
    def generate_daily_report(self):
        """Vygeneruje denní report o stavu systému"""
        if not self.config['daily_report']:
            logger.info("Denní reporty jsou vypnuty v konfiguraci")
            return None
        
        try:
            # Datum pro název souboru
            today = datetime.now().strftime('%Y-%m-%d')
            report_file = os.path.join(self.config['report_dir'], f'fred_report_{today}.html')
            
            # Získat data pro report
            integrity = self.check_database_integrity()
            error_analysis = self.analyze_errors(days=1)
            error_analysis_week = self.analyze_errors(days=7)
            
            # Statistiky o datech
            conn = sqlite3.connect(self.config['db_file'])
            
            # Počet aktualizovaných sérií za posledních 24 hodin
            yesterday = (datetime.now() - timedelta(days=1)).isoformat()
            updated_query = f"""
            SELECT COUNT(DISTINCT series_id) 
            FROM update_log 
            WHERE timestamp >= '{yesterday}'
            AND status = 'SUCCESS'
            AND series_id != 'SYSTEM'
            """
            
            updated_series = pd.read_sql_query(updated_query, conn).iloc[0, 0]
            
            # Top 10 nejčastěji aktualizovaných sérií
            top_updated_query = """
            SELECT series_id, COUNT(*) as update_count
            FROM update_log
            WHERE status = 'SUCCESS'
            AND series_id != 'SYSTEM'
            GROUP BY series_id
            ORDER BY update_count DESC
            LIMIT 10
            """
            
            top_updated = pd.read_sql_query(top_updated_query, conn)
            
            # Frekvence aktualizace
            frequency_query = """
            SELECT frequency, COUNT(*) as series_count
            FROM series_metadata
            GROUP BY frequency
            ORDER BY series_count DESC
            """
            
            frequency_data = pd.read_sql_query(frequency_query, conn)
            
            conn.close()
            
            # Vytvořit HTML report
            html = f"""
            <!DOCTYPE html>
            <html>
            <head>
                <title>FRED Data Report - {today}</title>
                <style>
                    body {{ font-family: Arial, sans-serif; line-height: 1.6; padding: 20px; }}
                    h1, h2, h3 {{ color: #333; }}
                    .container {{ max-width: 1200px; margin: 0 auto; }}
                    .section {{ margin-bottom: 30px; }}
                    .summary {{ display: flex; flex-wrap: wrap; gap: 20px; }}
                    .summary-item {{ background: #f5f5f5; padding: 15px; border-radius: 5px; flex: 1; }}
                    .success {{ color: green; }}
                    .warning {{ color: orange; }}
                    .error {{ color: red; }}
                    table {{ width: 100%; border-collapse: collapse; }}
                    th, td {{ padding: 8px; text-align: left; border-bottom: 1px solid #ddd; }}
                    th {{ background-color: #f2f2f2; }}
                </style>
            </head>
            <body>
                <div class="container">
                    <h1>FRED Data Report - {today}</h1>
                    
                    <div class="section">
                        <h2>Souhrn systému</h2>
                        <div class="summary">
                            <div class="summary-item">
                                <h3>Databáze</h3>
                                <p><strong>Stav:</strong> <span class="{'success' if integrity['tables_ok'] else 'error'}">{'V pořádku' if integrity['tables_ok'] else 'Problémy'}</span></p>
                                <p><strong>Počet sérií:</strong> {integrity['series_count']}</p>
                                <p><strong>Počet hodnot:</strong> {integrity['values_count']}</p>
                            </div>
                            
                            <div class="summary-item">
                                <h3>Aktualizace (24h)</h3>
                                <p><strong>Míra chyb:</strong> <span class="{'success' if error_analysis['error_rate'] < 0.05 else 'warning' if error_analysis['error_rate'] < 0.2 else 'error'}">{error_analysis['error_rate']:.2%}</span></p>
                                <p><strong>Aktualizované série:</strong> {updated_series}</p>
                                <p><strong>Úspěšné operace:</strong> {error_analysis['success_count']}</p>
                                <p><strong>Chyby:</strong> {error_analysis['error_count']}</p>
                            </div>
                            
                            <div class="summary-item">
                                <h3>Trendy (7 dní)</h3>
                                <p><strong>Míra chyb:</strong> {error_analysis_week['error_rate']:.2%}</p>
                                <p><strong>Úspěšné operace:</strong> {error_analysis_week['success_count']}</p>
                                <p><strong>Chyby:</strong> {error_analysis_week['error_count']}</p>
                            </div>
                        </div>
                    </div>
            """
            
            # Přidat sekci o problémech, pokud existují
            if integrity['issues']:
                html += f"""
                    <div class="section">
                        <h2>Problémy s integritou databáze</h2>
                        <ul>
                """
                
                for issue in integrity['issues']:
                    html += f"<li>{issue}</li>\n"
                
                html += """
                        </ul>
                    </div>
                """
            
            # Přidat sekci o nejčastějších chybách
            if error_analysis['most_common_errors']:
                html += f"""
                    <div class="section">
                        <h2>Nejčastější chyby (24h)</h2>
                        <table>
                            <tr>
                                <th>Chyba</th>
                                <th>Počet</th>
                            </tr>
                """
                
                for error in error_analysis['most_common_errors']:
                    html += f"""
                        <tr>
                            <td>{error['message']}</td>
                            <td>{error['count']}</td>
                        </tr>
                    """
                
                html += """
                        </table>
                    </div>
                """
            
            # Přidat nejproblematičtější série
            if error_analysis['affected_series']:
                html += f"""
                    <div class="section">
                        <h2>Série s nejvíce chybami (24h)</h2>
                        <table>
                            <tr>
                                <th>Series ID</th>
                                <th>Počet chyb</th>
                            </tr>
                """
                
                for series in error_analysis['affected_series']:
                    html += f"""
                        <tr>
                            <td>{series['series_id']}</td>
                            <td>{series['error_count']}</td>
                        </tr>
                    """
                
                html += """
                        </table>
                    </div>
                """
            
            # Přidat nejvíce aktualizované série
            if not top_updated.empty:
                html += f"""
                    <div class="section">
                        <h2>Nejčastěji aktualizované série</h2>
                        <table>
                            <tr>
                                <th>Series ID</th>
                                <th>Počet aktualizací</th>
                            </tr>
                """
                
                for _, row in top_updated.iterrows():
                    html += f"""
                        <tr>
                            <td>{row['series_id']}</td>
                            <td>{row['update_count']}</td>
                        </tr>
                    """
                
                html += """
                        </table>
                    </div>
                """
            
            # Přidat frekvence aktualizace
            if not frequency_data.empty:
                html += f"""
                    <div class="section">
                        <h2>Frekvence aktualizace</h2>
                        <table>
                            <tr>
                                <th>Frekvence</th>
                                <th>Počet sérií</th>
                            </tr>
                """
                
                for _, row in frequency_data.iterrows():
                    html += f"""
                        <tr>
                            <td>{row['frequency'] or 'Neuvedeno'}</td>
                            <td>{row['series_count']}</td>
                        </tr>
                    """
                
                html += """
                        </table>
                    </div>
                """
            
            # Přidat informaci o implementaci
            html += f"""
                <div class="section">
                    <h2>Informace o systému</h2>
                    <p><strong>Databáze:</strong> {self.config['db_file']}</p>
                    <p><strong>Implementace:</strong> {'Selenium (obcházení blokování)' if self.config['use_selenium'] else 'Standardní (requests/BeautifulSoup)'}</p>
                    <p><strong>Použití proxy:</strong> {'Ano' if self.config['use_proxy'] else 'Ne'}</p>
                    <p><strong>Paralelní zpracování:</strong> {self.config['max_workers']} workerů</p>
                </div>
            """
            
            # Uzavřít HTML
            html += """
                </div>
            </body>
            </html>
            """
            
            # Uložit report
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write(html)
            
            logger.info(f"Denní report vygenerován a uložen do {report_file}")
            
            # Pokud je nastaveno odesílání e-mailem
            if self.config['email_notifications']:
                self.send_email_report(html, f"FRED Data Report - {today}")
            
            return report_file
        
        except Exception as e:
            logger.error(f"Chyba při generování denního reportu: {str(e)}")
            return None
    
    def send_email_report(self, html_content, subject):
        """Odeslat report e-mailem"""
        if not self.config['email_notifications']:
            logger.info("E-mailové notifikace jsou vypnuty v konfiguraci")
            return False
        
        try:
            # Vytvořit e-mail
            msg = MIMEMultipart()
            msg['From'] = self.config['email_from']
            msg['To'] = ', '.join(self.config['email_to'])
            msg['Subject'] = subject
            
            # Přidat HTML obsah
            msg.attach(MIMEText(html_content, 'html'))
            
            # Připojit se k SMTP serveru
            server = smtplib.SMTP(self.config['smtp_server'], self.config['smtp_port'])
            server.starttls()
            server.login(self.config['smtp_user'], self.config['smtp_password'])
            
            # Odeslat e-mail
            server.send_message(msg)
            server.quit()
            
            logger.info(f"E-mail úspěšně odeslán na {', '.join(self.config['email_to'])}")
            return True
        
        except Exception as e:
            logger.error(f"Chyba při odesílání e-mailu: {str(e)}")
            return False
    
    def check_for_alerts(self):
        """Kontroluje, zda je třeba odeslat upozornění o chybách"""
        try:
            # Analyzovat chyby za posledních 24 hodin
            error_analysis = self.analyze_errors(days=1)
            
            # Pokud překročí práh, odeslat upozornění
            if error_analysis['error_count'] >= self.config['error_threshold']:
                subject = f"FRED Data Alert - {error_analysis['error_count']} chyb za posledních 24 hodin"
                
                message = f"""
                <html>
                <body>
                    <h2>ALERT: Vysoký počet chyb při aktualizaci FRED dat</h2>
                    <p>Za posledních 24 hodin došlo k {error_analysis['error_count']} chybám, 
                    což překračuje nastavený práh {self.config['error_threshold']}.</p>
                    
                    <h3>Statistiky:</h3>
                    <ul>
                        <li>Celkem operací: {error_analysis['total_logs']}</li>
                        <li>Úspěšných: {error_analysis['success_count']}</li>
                        <li>Chyb: {error_analysis['error_count']}</li>
                        <li>Míra chyb: {error_analysis['error_rate']:.2%}</li>
                    </ul>
                    
                    <h3>Nejčastější chyby:</h3>
                    <ol>
                """
                
                for error in error_analysis['most_common_errors'][:5]:
                    message += f"<li>{error['message']} ({error['count']}x)</li>"
                
                message += """
                    </ol>
                    
                    <h3>Nejvíce zasažené série:</h3>
                    <ol>
                """
                
                for series in error_analysis['affected_series'][:5]:
                    message += f"<li>{series['series_id']} ({series['error_count']}x)</li>"
                
                message += """
                    </ol>
                </body>
                </html>
                """
                
                if self.config['email_notifications']:
                    self.send_email_report(message, subject)
                
                logger.warning(subject)
                return True
            
            return False
        
        except Exception as e:
            logger.error(f"Chyba při kontrole upozornění: {str(e)}")
            return False

def main():
    parser = argparse.ArgumentParser(description='FRED monitoring a správa systému')
    parser.add_argument('--config', type=str, default='monitoring_config.json', help='Cesta ke konfiguračnímu souboru')
    parser.add_argument('--db-file', type=str, help='Cesta k databázovému souboru')
    parser.add_argument('--check', action='store_true', help='Zkontrolovat integritu databáze')
    parser.add_argument('--report', action='store_true', help='Vygenerovat report')
    parser.add_argument('--retry', action='store_true', help='Zkusit znovu stáhnout série, které selhaly')
    parser.add_argument('--retry-days', type=int, default=1, help='Počet dní zpět pro vyhledávání neúspěšných sérií')
    parser.add_argument('--alerts', action='store_true', help='Zkontrolovat a odeslat případné alerty')
    parser.add_argument('--full', action='store_true', help='Spustit všechny funkce (kontrola, retry, report, alerty)')
    parser.add_argument('--email', action='store_true', help='Aktivovat e-mailové notifikace')
    parser.add_argument('--proxy', type=str, help='Použít proxy server pro spojení')
    
    args = parser.parse_args()
    
    # Vytvořit instanci monitoru
    monitor = FredMonitor(config_file=args.config, db_file=args.db_file)
    
    # Aktualizovat konfiguraci podle argumentů
    if args.proxy:
        monitor.config['use_proxy'] = args.proxy
    
    if args.email:
        monitor.config['email_notifications'] = True
    
    # Spustit požadované funkce
    if args.check or args.full:
        monitor.check_database_integrity()
    
    if args.retry or args.full:
        monitor.retry_failed_series(days=args.retry_days)
    
    if args.report or args.full:
        monitor.generate_daily_report()
    
    if args.alerts or args.full:
        monitor.check_for_alerts()
    
    # Pokud nebyl zadán žádný argument, zkontrolovat integritu
    if not any([args.check, args.report, args.retry, args.alerts, args.full]):
        monitor.check_database_integrity()
        print("Pro zobrazení všech dostupných možností použijte --help")

# Příklad použití
if __name__ == "__main__":
    # Kontrola, zda jsme spuštěni přímo nebo importováni
    if len(sys.argv) > 1:
        main()
    else:
        # Standardní chování při importu jako modul
        random = __import__('random')  # Skrytý import, aby se dal použít v retry_worker
        monitor = FredMonitor()
        
        # Kontrola integrity
        monitor.check_database_integrity()
        
        # Analýza chyb
        monitor.analyze_errors()
        
        # Znovustažení sérií, které selhaly
        monitor.retry_failed_series()
        
        # Vygenerovat denní report
        monitor.generate_daily_report()
        
        # Kontrola upozornění
        monitor.check_for_alerts()