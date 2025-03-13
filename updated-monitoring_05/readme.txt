# Dokumentace systému pro scrapování FRED ekonomických dat

## Obsah
1. [Přehled systému](#přehled-systému)
2. [Instalace a požadavky](#instalace-a-požadavky)
3. [Komponenty systému](#komponenty-systému)
   - [Crawler kategorií](#1-crawler-kategorií-mac-fred-scraperpy)
   - [Downloader sérií](#2-downloader-sérií-fred-series-downloaderpy)
   - [Databázový modul](#3-databázový-modul-updated-database-modulepy)
   - [Modul denních aktualizací](#4-modul-denních-aktualizací-updated-daily-updaterpy)
   - [Monitorovací systém](#5-monitorovací-systém-updated-monitoringpy)
4. [Pracovní postupy](#pracovní-postupy)
   - [První inicializace](#první-inicializace)
   - [Denní aktualizace](#denní-aktualizace)
   - [Monitoring a údržba](#monitoring-a-údržba)
5. [Konfigurace](#konfigurace)
6. [Řešení problémů](#řešení-problémů)
7. [Rozšíření a úpravy](#rozšíření-a-úpravy)

## Přehled systému

Tento systém slouží ke komplexnímu scrapování ekonomických dat z Federal Reserve Economic Data (FRED) databáze. Umožňuje získat přes 800 000 ekonomických metrik, ukládat je do lokální databáze a provádět denní aktualizace.

**Klíčové vlastnosti:**
- Rekurzivní procházení všech kategorií a podkategorií FRED
- Stahování dat sérií s podporou filtrování podle frekvence aktualizace
- Ukládání do SQLite databáze optimalizované pro časové řady
- Inteligentní denní aktualizace, které stahují pouze změněná data
- Robustní monitoring včetně reportů a e-mailových upozornění
- Obcházení ochrany proti automatizovanému scrapování

## Instalace a požadavky

### Potřebné závislosti:

```bash
pip install selenium pandas matplotlib seaborn sqlite3
```

### Webdriver:
- Pro běh Selenia je nutný Chrome webdriver, který se instaluje automaticky

### Struktura adresářů:
```
fred_scraper/
├── mac-fred-scraper.py          # Crawler pro procházení kategorií
├── fred-series-downloader.py    # Downloader dat sérií
├── updated-database-module.py   # Databázový modul
├── updated-daily-updater.py     # Modul denních aktualizací
├── updated-monitoring.py        # Monitorovací systém
├── fred_data.db                 # Databáze SQLite (vytvoří se automaticky)
├── monitoring_config.json       # Konfigurace monitoringu (vytvoří se automaticky)
└── reports/                     # Adresář pro reporty (vytvoří se automaticky)
```

## Komponenty systému

### 1. Crawler kategorií (`mac-fred-scraper.py`)

**Účel:**
- Prochází všechny kategorie a podkategorie FRED
- Sbírá kompletní seznam všech dostupných časových řad
- Ukládá seznam do CSV souboru pro další zpracování

**Použití:**
```bash
# Základní použití
python mac-fred-scraper.py

# Testovat přístup s proxy
python mac-fred-scraper.py --proxy 123.45.67.89:8080 --test

# Začít od konkrétní kategorie
python mac-fred-scraper.py --category https://fred.stlouisfed.org/categories/33940
```

**Jak funguje:**
1. Zahajuje na úvodní stránce kategorií FRED
2. Rekurzivně prochází každou kategorii a extrahuje série dat
3. Kontroluje paginaci (stránkování) a podkategorie
4. Postupně zpracovává seznam, dokud nejsou navštíveny všechny kategorie
5. Průběžně ukládá výsledky pro odolnost proti přerušení

**Výstup:**
- CSV soubor `fred_series_complete_TIMESTAMP.csv` obsahující všechny nalezené série

### 2. Downloader sérií (`fred-series-downloader.py`)

**Účel:**
- Stahuje data pro jednotlivé série z FRED
- Extrahuje metadata (frekvence, jednotky, atd.)
- Filtruje série podle frekvence aktualizace
- Ukládá data do SQLite databáze

**Použití:**
```bash
# Základní použití
python fred-series-downloader.py --input fred_series_complete_1741805123.csv

# Omezení počtu stahovaných sérií (pro testování)
python fred-series-downloader.py --input fred_series_complete_1741805123.csv --limit 100

# Změna počtu paralelních workerů
python fred-series-downloader.py --input fred_series_complete_1741805123.csv --workers 3

# Použití proxy serveru
python fred-series-downloader.py --input fred_series_complete_1741805123.csv --proxy 123.45.67.89:8080
```

**Jak funguje:**
1. Načte seznam sérií z CSV souboru
2. Vytvoří několik paralelních instancí prohlížeče (workerů)
3. Každý worker stahuje data a metadata pro přidělené série
4. Filtruje série podle frekvence aktualizace (denní, týdenní, měsíční, čtvrtletní)
5. Ukládá data do SQLite databáze

**Parametry:**
- `--input`: Vstupní CSV soubor se seznamem sérií (povinný)
- `--workers`: Počet paralelních workerů (výchozí: 5)
- `--limit`: Maximální počet sérií ke zpracování
- `--proxy`: Proxy server ve formátu IP:port
- `--skip`: Přeskočit prvních N sérií
- `--db-file`: Cesta k databázovému souboru (výchozí: fred_data.db)

### 3. Databázový modul (`updated-database-module.py`)

**Účel:**
- Poskytuje rozhraní pro práci s databází
- Zajišťuje optimalizovanou strukturu pro časové řady
- Implementuje funkce pro kontrolu aktualizací

**Použití:**
```bash
# Inicializace databáze
python updated-database-module.py --init

# Zobrazení statistik
python updated-database-module.py --stats

# Použití jiného umístění databáze
python updated-database-module.py --init --db-file /cesta/k/databazi.db
```

**Struktura databáze:**
- `series_metadata` - informace o sériích (frekvence, jednotky, atd.)
- `series_values` - hodnoty časových řad (datum, hodnota)
- `update_log` - záznamy o aktualizacích a chybách

**Klíčové funkce:**
- `create_database()`: Inicializuje strukturu databáze
- `store_series()`: Ukládá data série
- `check_if_needs_update_selenium()`: Kontroluje, zda série potřebuje aktualizaci
- `get_latest_date_in_db()`: Vrací poslední datum v databázi pro danou sérii

### 4. Modul denních aktualizací (`updated-daily-updater.py`)

**Účel:**
- Zajišťuje automatické denní aktualizace dat
- Aktualizuje pouze série, které se změnily
- Efektivně pracuje s velkým množstvím sérií

**Použití:**
```bash
# Základní použití - aktualizace všech sérií
python updated-daily-updater.py

# Omezení počtu aktualizovaných sérií
python updated-daily-updater.py --limit 100

# Nastavení počtu paralelních workerů
python updated-daily-updater.py --workers 3

# Použití proxy a vlastní databáze
python updated-daily-updater.py --proxy 123.45.67.89:8080 --db-file /cesta/k/databazi.db
```

**Jak funguje:**
1. Získá seznam sérií, které by měly být aktualizovány
2. Prioritizuje série podle frekvence aktualizace (denní > týdenní > měsíční > čtvrtletní)
3. Kontroluje, zda každá série potřebuje aktualizaci (kontrolou last_updated)
4. Stahuje a ukládá pouze nové nebo změněné data
5. Zaznamenává průběh a výsledky aktualizací

**Parametry:**
- `--limit`: Maximální počet sérií k aktualizaci
- `--workers`: Počet paralelních workerů
- `--db-file`: Cesta k databázovému souboru
- `--proxy`: Proxy server ve formátu IP:port
- `--delay`: Základní zpoždění mezi požadavky (v sekundách)

### 5. Monitorovací systém (`updated-monitoring.py`)

**Účel:**
- Monitoruje integritu databáze
- Analyzuje chyby a problémy
- Generuje denní reporty
- Zkouší znovu stáhnout série, které selhaly
- Odesílá e-mailová upozornění při problémech

**Použití:**
```bash
# Základní kontrola integrity
python updated-monitoring.py

# Generování reportu
python updated-monitoring.py --report

# Opakované zpracování sérií, které selhaly
python updated-monitoring.py --retry

# Kompletní monitoring
python updated-monitoring.py --full

# S e-mailovými notifikacemi a proxy
python updated-monitoring.py --full --email --proxy 123.45.67.89:8080
```

**Jak funguje:**
1. Kontroluje integritu databázových tabulek a dat
2. Analyzuje záznamy o chybách a problémech
3. Identifikuje a znovu zpracovává série, které selhaly
4. Generuje HTML reporty s přehledem stavu systému
5. Odesílá e-mailová upozornění, pokud počet chyb překročí nastavený práh

**Konfigurace:**
- `monitoring_config.json` obsahuje nastavení pro monitoring a notifikace
- Lze nastavit SMTP server, příjemce e-mailů, práh pro upozornění, atd.

## Pracovní postupy

### První inicializace

Pro první kompletní scrapování a naplnění databáze:

1. **Získání seznamu všech sérií:**
   ```bash
   python mac-fred-scraper.py
   ```
   Vytvoří CSV soubor se seznamem všech dostupných sérií.

2. **Inicializace databáze:**
   ```bash
   python updated-database-module.py --init
   ```
   Vytvoří správnou strukturu databáze.

3. **Stažení dat:**
   ```bash
   python fred-series-downloader.py --input fred_series_complete_TIMESTAMP.csv
   ```
   Stáhne a uloží data pro všechny série.

4. **Kontrola výsledků:**
   ```bash
   python updated-monitoring.py --full
   ```
   Zkontroluje stav databáze a vygeneruje report.

### Denní aktualizace

Pro pravidelnou denní aktualizaci dat:

1. **Nastavení pro cron nebo Task Scheduler:**
   ```bash
   # V crontab (Linux/Mac)
   0 1 * * * cd /cesta/k/fred_scraper && python updated-daily-updater.py
   0 6 * * * cd /cesta/k/fred_scraper && python updated-monitoring.py --full --email
   ```

2. **Ruční spuštění aktualizace:**
   ```bash
   python updated-daily-updater.py
   ```

3. **Kontrola a reporting:**
   ```bash
   python updated-monitoring.py --full
   ```

### Monitoring a údržba

Pro údržbu systému a řešení problémů:

1. **Kontrola integrity databáze:**
   ```bash
   python updated-monitoring.py --check
   ```

2. **Generování reportu:**
   ```bash
   python updated-monitoring.py --report
   ```

3. **Zpracování sérií, které selhaly:**
   ```bash
   python updated-monitoring.py --retry
   ```

4. **Kompletní kontrola a údržba:**
   ```bash
   python updated-monitoring.py --full
   ```

## Konfigurace

### Sdílené nastavení

Každý modul má svoji konfiguraci v rámci objektu `CONFIG`, který lze upravit:

```python
CONFIG = {
    "db_file": "fred_data.db",
    "max_workers": 5,
    "min_delay": 1.0,
    "max_delay": 3.0,
    "use_proxy": None
}
```

### Monitoring konfigurace

Monitorovací systém používá vlastní konfigurační soubor `monitoring_config.json`:

```json
{
    "email_notifications": true,
    "email_from": "monitoring@example.com",
    "email_to": ["admin@example.com"],
    "smtp_server": "smtp.example.com",
    "smtp_port": 587,
    "smtp_user": "user",
    "smtp_password": "password",
    "error_threshold": 100,
    "daily_report": true,
    "report_dir": "reports",
    "retry_failed": true,
    "retry_limit": 3
}
```

## Řešení problémů

### Problémy s přístupem (Access Denied)

1. **Použití proxy:**
   ```bash
   python mac-fred-scraper.py --proxy 123.45.67.89:8080
   ```

2. **Zpomalení požadavků:**
   Upravte hodnoty `min_delay` a `max_delay` v konfiguračním objektu na vyšší hodnoty.

3. **Méně paralelních workerů:**
   ```bash
   python fred-series-downloader.py --input list.csv --workers 3
   ```

### Problémy s pamětí při velkém množství dat

1. **Omezení počtu stahovaných sérií najednou:**
   ```bash
   python fred-series-downloader.py --input list.csv --limit 10000
   ```

2. **Postupné zpracování:**
   ```bash
   # První dávka
   python fred-series-downloader.py --input list.csv --limit 10000
   
   # Druhá dávka
   python fred-series-downloader.py --input list.csv --limit 10000 --skip 10000
   ```

## Rozšíření a úpravy

### Přidání nových funkcí

1. Všechny moduly jsou navrženy modulárně, takže lze snadno přidat nové funkce.
2. Pro přidání podpory pro další zdroje dat, stačí vytvořit podobné moduly s odpovídající strukturou.

### Změna struktury databáze

Upravte funkci `create_database()` v `updated-database-module.py` a přidejte nové tabulky nebo sloupce podle potřeby.

### Vlastní reporty

Upravte funkci `generate_daily_report()` v `updated-monitoring.py` pro přizpůsobení formátu a obsahu reportů.

---

Tato dokumentace poskytuje kompletní přehled o systému pro scrapování FRED dat. Při dotazech nebo problémech se obraťte na správce systému nebo vytvořte issue v repozitáři projektu.