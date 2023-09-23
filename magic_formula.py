"""
Magic Formula Script

Version 1.1

Author: Nathan Hsu
"""

__version__ = "1.0"
__author__ = "Nathan Hsu"

from yahoofinancials import YahooFinancials
import json
import csv
from re import sub
import argparse
import time
import os
from multiprocessing import Process
import threading
import sqlite3 as sq
import pandas as pd
from datetime import date, timedelta
import requests
from bs4 import BeautifulSoup
from functools import cmp_to_key

fn_balance = 'quarterly_balance_sheet'
fn_income = 'quarterly_income_statement'
fn_cap = 'market_cap_info'
fn_tickers = 'ticker_dict'
fn_price = 'price_dict'
fn_stock_info_db = 'stock_info.db'
batch_size = 10
max_threads = 3  # Somewhere between 10 and 15 threads with batch_size of 10 seems to be allowed
min_market_cap = 50000000
min_dollar_volume = 10000000  # based on 10-day and 90-day average volume
TICKER_VALID = 1
TICKER_INVALID = 0
TICKER_NOT_VALIDATED = -1
TICKER_REMOVE = -2
TICKER_MISSING_INFO = -3

dict_lock = threading.Lock()
sect_lock = threading.Lock()
ticker_lock = threading.Lock()

debug = False
verbose = False  # This can be set in the command line


def get_roc(ticker):
    return get_ebit(ticker) / (get_net_working_capital(ticker) + get_fixed_assets(ticker))


def get_yield(ticker):
    return get_ebit(ticker) / get_ev(ticker)


def get_ev(ticker):
    total_debt = get_longTermDebt(ticker) + get_totalCurrentLiabilities(ticker)
    return max(0, get_market_cap(ticker) + total_debt - get_excess_cash(ticker))


def get_net_working_capital(ticker):
    return max(0, get_totalCurrentAssets(ticker) - get_excess_cash(ticker) - get_accountsPayable(ticker))


def get_fixed_assets(ticker):
    # return get_netTangibleAssets(ticker) - get_totalCurrentAssets(ticker) + get_totalLiab(ticker)

    return get_totalAssets(ticker) - get_totalCurrentAssets(ticker) - get_intangibles(ticker)


def get_excess_cash(ticker):
    return get_cash(ticker) - max(0, get_totalCurrentLiabilities(ticker) - get_totalCurrentAssets(ticker) +
                                  get_cash(ticker))


def get_accountsPayable(ticker):
    balance_dict = list(balance_sheet[ticker][0].values())[-1]
    try:
        return balance_dict['accountsPayable']
    except Exception as e:
        insert_error(ticker, f"Missing {e} information for {ticker}")
        return 0


def get_intangibles(ticker):
    balance_dict = list(balance_sheet[ticker][0].values())[-1]
    try:
        return balance_dict['intangibleAssets']
    except Exception as e:
        insert_error(ticker, f"Missing {e} information for {ticker}, trying different method")
        try:
            return get_totalAssets(ticker) - get_netTangibleAssets(ticker) - get_totalLiab(ticker)
        except Exception as e:
            insert_error(ticker, f"Missing {e} information for {ticker}")
            return 0


def get_goodwill(ticker):
    balance_dict = list(balance_sheet[ticker][0].values())[-1]
    try:
        return balance_dict['goodWill']
    except Exception as e:
        insert_error(ticker, f"Missing {e} information for {ticker}")
        return 0


def get_totalLiab(ticker):
    balance_dict = list(balance_sheet[ticker][0].values())[-1]
    try:
        return balance_dict['totalLiab']
    except Exception as e:
        insert_error(ticker, f"missing {e} information for {ticker}, trying different method")
        try:
            return balance_dict['totalLiabilitiesNetMinorityInterest']
        except Exception as e:
            insert_error(ticker, f"missing {e} information for {ticker}")
            return 0


def get_totalAssets(ticker):
    balance_dict = list(balance_sheet[ticker][0].values())[-1]
    try:
        return balance_dict['totalAssets']
    except Exception as e:
        insert_error(ticker, f"Missing {e} information for {ticker}")
        return 0


def get_netTangibleAssets(ticker):
    balance_dict = list(balance_sheet[ticker][0].values())[-1]
    try:
        return balance_dict['netTangibleAssets']
    except Exception as e:
        insert_error(ticker, f"Missing {e} information for {ticker}")
        return 0


def get_totalCurrentAssets(ticker):
    balance_dict = list(balance_sheet[ticker][0].values())[-1]
    try:
        return balance_dict['totalCurrentAssets']
    except Exception as e:
        insert_error(ticker, f"Missing {e} information for {ticker}, trying different method")
        try:
            return get_totalAssets(ticker) - balance_dict['totalNonCurrentAssets']
        except Exception as e:
            insert_error(ticker, f"Missing {e} information for {ticker}")
            return 0


def get_longTermDebt(ticker):
    balance_dict = list(balance_sheet[ticker][0].values())[-1]
    try:
        return balance_dict['longTermDebt']
    except Exception as e:
        insert_error(ticker, f"Missing {e} information for {ticker}")
        return 0


def get_totalCurrentLiabilities(ticker):
    balance_dict = list(balance_sheet[ticker][0].values())[-1]
    try:
        return balance_dict['totalCurrentLiabilities']
    except Exception as e:
        insert_error(ticker, f"Missing {e} information for {ticker}, trying different method")
        try:
            return balance_dict['currentLiabilities']
        except Exception as e:
            insert_error(ticker, f"Missing {e} information for {ticker}")
            return 0


def get_cash(ticker):
    balance_dict = list(balance_sheet[ticker][0].values())[-1]
    try:
        return balance_dict['cash']
    except Exception as e:
        insert_error(ticker, f"Missing {e} information for {ticker}, trying different method")
        try:
            return balance_dict['cashAndCashEquivalents']
        except Exception as e:
            insert_error(ticker, f"Missing {e} information for {ticker}")
            return 0


def date_compare(date1, date2):
    if date.fromisoformat(list(date1.keys())[0]) <= date.fromisoformat(list(date2.keys())[0]):
        return -1
    else:
        return 1


def get_ebit(ticker):
    # First sort by date
    income_list = [i for i in income_statement[ticker]]
    income_list.sort(key=cmp_to_key(date_compare))
    # Sum the most recent 4 quarters
    ttm_ebit = sum([list(i.values())[0]['ebit'] for i in income_list[-4:]])

    # Add them up
    if verbose:
        print()
        print(f"------{ticker}--------")
        ebit_sum = 0
        for element in income_list[-4:]:
            for verbose_date, value in element.items():
                print(f"{verbose_date}: {value['ebit']}")
                ebit_sum += value['ebit']
        print(f"Debug Total ebit: {ebit_sum}")
        print(f"Returned Total ebit: {ttm_ebit}")
    return ttm_ebit


def get_market_cap(ticker):
    return market_cap_dict[ticker]


# Gets the most recent dates of the balance sheet and income statement, and returns the least recent between the two
# The point is to check how recent the stock's information is.
def get_financials_date(ticker):
    income_list = [i for i in income_statement[ticker]]
    income_list.sort(key=cmp_to_key(date_compare))

    balance_list = [i for i in balance_sheet[ticker]]
    balance_list.sort(key=cmp_to_key(date_compare))

    income_date = date.fromisoformat(list(income_list[-1].keys())[0])
    balance_date = date.fromisoformat(list(balance_list[-1].keys())[0])
    if verbose:
        print(f"Most recent income statement: {income_date.isoformat()}")
        print(f"Most recent balance sheet: {balance_date.isoformat()}")

    # Return the less recent date (i.e. smaller date)
    if balance_date > income_date:
        return income_date
    else:
        return balance_date


def get_sector(ticker):
    return sector_dict[ticker]['sector']


def get_industry(ticker):
    return sector_dict[ticker]['industry']


def get_country(ticker):
    return sector_dict[ticker]['country']


# TODO Maybe insert sector, industry, and country in a separate UPDATE sql command, and insert null if they raise
#   exceptions (e.g. if the information doesn't exist)
def insert_data(conn, ticker_info):
    sql = ''' REPLACE INTO stock_info (ticker, roc, yield, market_cap, most_recent, sector, industry, country)
              VALUES(?,?,?,?,?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, ticker_info)
    conn.commit()


# TODO Have a command line argument option to drop the table and refresh (instead of always dropping the table)
# TODO Add columns for sector, country, and industry
def update_db(tickers):
    print("Updating database...")
    conn = sq.connect(fn_stock_info_db, detect_types=sq.PARSE_DECLTYPES)
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS stock_info")
    cursor.execute('''CREATE TABLE IF NOT EXISTS stock_info (
    ticker text PRIMARY KEY,
    roc real NOT NULL,
    yield real NOT NULL,
    market_cap int NOT NULL,
    most_recent DATE,
    sector text,
    industry text,
    country text
    );''')
    for ticker in tickers:
        try:
            data = (ticker, get_roc(ticker), get_yield(ticker), get_market_cap(ticker), get_financials_date(ticker),
                    get_sector(ticker), get_industry(ticker), get_country(ticker))
            insert_data(conn, data)
        except Exception as e:
            insert_error(ticker, f"Update DB, data error for ticker {ticker}: {e}. Going to next ticker.")
    if conn:
        conn.close()


# TODO Add Error Type (eventually)
# TODO Add Errors for when yahoo_financials fails to get financial statements
def create_errors_table():
    """ Create table in db to store errors
    """
    print("Creating errors table...")
    conn = sq.connect(fn_stock_info_db, detect_types=sq.PARSE_DECLTYPES)
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS errors")
    cursor.execute('''CREATE TABLE IF NOT EXISTS errors (
    error_id INTEGER PRIMARY KEY,
    ticker text,
    error text
    );''')
    conn.commit()


def insert_error(ticker, error):
    sql = ''' INSERT INTO errors (ticker, error)
              VALUES(?,?) '''
    conn = sq.connect(fn_stock_info_db, detect_types=sq.PARSE_DECLTYPES)
    cur = conn.cursor()
    cur.execute(sql, (ticker, error))
    conn.commit()
    print(error)


def rank_stocks(db):
    print("Ranking stocks based on Magic Formula...")
    conn = sq.connect(rf'{db}')
    cursor = conn.cursor()

    last_year_date = date.today() - timedelta(days=400)
    print(f"Last year's date: {last_year_date.strftime('%Y-%m-%d')}")

    sql_string = f'''
    SELECT * FROM (
        SELECT *, roc_rank + yield_rank AS magic_rank FROM
        (
            SELECT *,  RANK ()  OVER( ORDER BY roc DESC) AS roc_rank,
            RANK () OVER( ORDER BY yield DESC) AS yield_rank FROM stock_info      
        )
    ) WHERE most_recent > date('{last_year_date.strftime('%Y-%m-%d')}')
    ORDER BY magic_rank ASC
    '''
    query = cursor.execute(sql_string)

    cols = [column[0] for column in query.description]
    df = pd.DataFrame.from_records(data=query.fetchall(), columns=cols)
    df.to_csv('stock_info.csv')

    if conn:
        conn.close()


def print_db(db_file_name):
    conn = sq.connect(rf"{db_file_name}")
    print(pd.read_sql_query("SELECT * FROM stock_info", conn))
    if conn:
        conn.close()


# Return new ticker list based on if ticker exceeds a market cap and average dollar volume threshold
# Does not modify ticker_dict.json because that is a list of POTENTIALLY valid tickers (i.e. it only does not include
# tickers that are missing financial statements)
# Also updates the market cap info for valid tickers
def validate_tickers(tickers, cap_dict, batch_sz=batch_size, newonly=False):
    ticker_keys = list(tickers.keys())

    if newonly:
        # We only validate the tickers that are marked as "not validated"
        ticker_keys = [ticker for ticker in ticker_keys if tickers[ticker] == TICKER_NOT_VALIDATED]

    if batch_sz == 0:
        batch_sz = len(ticker_keys)
    batches = len(ticker_keys) // batch_sz
    thread_jobs = []
    print("Creating Threads to validate tickers...")
    for i in range(batches + 1):
        ticker_sublist = ticker_keys[i * batch_sz: min((i + 1) * batch_sz, len(ticker_keys))]
        if len(ticker_sublist) == 0:  # This is for when the batch_size evenly divides into the ticker_dict size
            break

        thread = threading.Thread(target=validate_tickers_thread, args=(ticker_sublist, tickers, cap_dict, i))
        thread_jobs.append(thread)

    running = 0
    next_count = 0
    join_count = 0
    while join_count < len(thread_jobs):
        if running < max_threads and next_count < len(thread_jobs):
            thread_jobs[next_count].start()
            time.sleep(3)  # So that requests don't come it TOO fast
            running += 1
            next_count += 1
        elif join_count < next_count:
            thread_jobs[join_count].join()
            running -= 1
            join_count += 1


def validate_tickers_thread(ticker_keys, tickers, cap_dict, batch_no):
    print(f"Batch {batch_no + 1}: Validating tickers {ticker_keys}")

    start_loop = time.time()

    # For the sake of efficiency, don't get average 10 day volume of tickers that don't meet market cap minimum
    new_ticker_keys = []

    for ticker in ticker_keys:
        if ticker not in cap_dict:
            tickers[ticker] = TICKER_REMOVE
            continue
        elif cap_dict[ticker] < min_market_cap:
            if verbose:
                print(f"Setting {ticker} to invalid")
            tickers[ticker] = TICKER_INVALID
            continue
        new_ticker_keys.append(ticker)

    if len(new_ticker_keys) == 0:
        if verbose:
            print("No tickers were valid")
        end_loop = time.time()
        print(f"Time elapsed for ticker validation, batch {batch_no + 1}: {end_loop - start_loop}")
        print()
        return

    yh = YahooFinancials(new_ticker_keys)

    if verbose:
        print(f"Batch {batch_no + 1}: Retrieving volume information from Yahoo Finance...")
    avg_ten_day_volume = yh.get_ten_day_avg_daily_volume()

    if verbose:
        print(f"Batch {batch_no + 1}: Calculating average dollar volumes...")
    avg_ten_day_dollar_volume = {}
    for ticker, value in avg_ten_day_volume.items():
        if ticker is None or value is None or ticker not in price_dict:
            tickers[ticker] = TICKER_REMOVE
        else:
            avg_ten_day_dollar_volume[ticker] = value * price_dict[ticker]
            ticker_lock.acquire()
            if avg_ten_day_dollar_volume[ticker] > min_dollar_volume:
                if verbose:
                    print(f"Setting {ticker} to valid")
                tickers[ticker] = TICKER_VALID
            else:
                if verbose:
                    print(f"Setting {ticker} to invalid")
                tickers[ticker] = TICKER_INVALID
            ticker_lock.release()
    json.dump(tickers, open(fn_tickers + '.json', 'w'))

    end_loop = time.time()
    print(f"Time elapsed for ticker validation, batch {batch_no + 1}: {end_loop - start_loop}")
    print()


def is_tickers_validated():
    return TICKER_NOT_VALIDATED not in ticker_dict.values()


def is_common_stock(description):
    description = description.upper()
    return "Warrant".upper() not in description and "Preferred".upper() not in description and \
           "Unit".upper() not in description and "ETF" not in description and "Index".upper() not in description


def get_valid_ticker_list():
    "Gets a list of tickers that a valid -> There should be no validation/checking of value outside of this function"
    temp = [ticker for ticker, value in ticker_dict.items() if value == TICKER_VALID]
    print(f"Getting valid ticker list of {len(temp)} tickers")
    return temp


def retrieve_data(batch_sz, ticker_keys, metric, file_name, data_dict):
    if batch_sz == 0:
        batch_sz = len(ticker_keys)
    batches = len(ticker_keys) // batch_sz
    thread_jobs = []
    print("Creating Threads...")
    for i in range(batches + 1):
        ticker_sublist = ticker_keys[i * batch_sz: min((i + 1) * batch_sz, len(ticker_keys))]
        if len(ticker_sublist) == 0:  # This is for when the batch_size evenly divides into the ticker_dict size
            break

        thread = threading.Thread(target=create_retrieve_thread, args=(ticker_sublist, metric, file_name, data_dict, i))
        thread_jobs.append(thread)

    running = 0
    next_count = 0
    join_count = 0
    while join_count < len(thread_jobs):
        if running < max_threads and next_count < len(thread_jobs):
            thread_jobs[next_count].start()
            time.sleep(3)  # So that requests don't come it TOO fast
            running += 1
            next_count += 1
        elif join_count < next_count:
            thread_jobs[join_count].join()
            running -= 1
            join_count += 1


def create_retrieve_thread(ticker_keys, metric, file_name, data_dict, batch_no):
    start_loop = time.time()
    print(f"Batch/thread {batch_no + 1}: Tickers to be retrieved are: {ticker_keys}")

    yahoo_financials = YahooFinancials(ticker_keys)

    if metric == "balance":
        print(f"Retrieving quarterly balance sheets from Yahoo Finance...")
        financial_statement = yahoo_financials.get_financial_stmts('quarterly', 'balance')[
            'balanceSheetHistoryQuarterly']

    elif metric == "income":
        print(f"Retrieving quarterly income statement history from Yahoo Finance...")
        financial_statement = yahoo_financials.get_financial_stmts('quarterly', 'income')[
            'incomeStatementHistoryQuarterly']

    # For the most part, "cap" should not be used; it should be parsed from nasdaq_stocks.csv. Kept this here, just in case
    elif metric == "cap":
        print(f"Retrieving market cap information from Yahoo Finance...")
        financial_statement = yahoo_financials.get_market_cap()

    else:
        print("Metric entered is not recognized.")
        financial_statement = {}

    dict_lock.acquire()
    data_dict.update(financial_statement)

    end_loop = time.time()

    print(f"Saving batch {batch_no + 1} to JSON file...")
    json.dump(data_dict, open(file_name + '.json', 'w'))
    dict_lock.release()

    print(f"Time elapsed for batch {batch_no + 1}: {end_loop - start_loop}, metric: {metric}")
    print()


def clean_tickers():
    """ Checks balance_sheet, income_statement, and market_cap_dict dictionaries for None values and empty list values, and removes
        those entries from the dictionaries, then updates their respective JSON files.
        ticker_dict changes are not saved to the json because info might be missing due to communication errors, and not
        necessarily because the data is missing (e.g. if we made too many requests to Yahoo Finance and the site refuses.
        This way, if we continue retrieving, all the tickers will be retrieved, since they are still in the ticker_dict
        Always called after refreshing data
    """
    global ticker_dict
    print("Cleaning tickers...")
    remove_tickers = set()
    get_valid_ticker_list()     # Here as a test to see when valid tickers go to 0

    for ticker, value in ticker_dict.items():
        # if get_net_working_capital(ticker) < 0 or get_fixed_assets(ticker) < 0:
        #     none_tickers.add(ticker)

        # Logic is: Remove ticker if flagged for removal OR for valid tickers (for which all data should be retrieved),
        # remove tickers with missing info
        if value == TICKER_REMOVE:
            remove_tickers.add(ticker)
        elif value == TICKER_VALID:
            if ticker not in balance_sheet or balance_sheet[ticker] is None or balance_sheet[ticker] == []:
                ticker_dict[ticker] = TICKER_MISSING_INFO
                insert_error(ticker, "Missing balance sheet")
            if ticker not in income_statement or income_statement[ticker] is None or income_statement[ticker] == []:
                ticker_dict[ticker] = TICKER_MISSING_INFO
                insert_error(ticker, "Missing income statement")
            if ticker not in market_cap_dict or market_cap_dict[ticker] is None or market_cap_dict[ticker] == []:
                ticker_dict[ticker] = TICKER_MISSING_INFO
                insert_error(ticker, "Missing market cap")

    clean_ticker_dict = {ticker: value for ticker, value in ticker_dict.items() if ticker not in remove_tickers}
    json.dump(clean_ticker_dict, open(fn_tickers + '.json', 'w'))
    ticker_dict = clean_ticker_dict


# TODO Instead of calling "continue retrieval", it should automatically retrieve if valid tickers are missing info
# Implement a MISSING_INFO flag so that script knows when a ticker has already been checked and is actually missing info
#   * Can I differentiate between different errors, so that I can check if there was a communication error vs actual missing info?


def create_process(batch_sz, p_tickers, p_id):
    """ Create empty dictionaries for the process, since the process does not have access to the global variables
        These will be accessed through their saved JSON files.
    """
    balance_sheet = {}
    income_statement = {}
    retrieve_data(batch_sz, p_tickers[0], "balance", f"{fn_balance}_{p_id}", balance_sheet)
    retrieve_data(batch_sz, p_tickers[1], "income", f"{fn_income}_{p_id}", income_statement)


def consolidate_json(remove=False):
    """ Takes the various JSON files from processes and updates the dictionaries: balance_sheet, income_statement, market_cap_dict
        Also removes the JSON files after consolidating
    :param remove:
    :return:
    """
    process_id = 0
    while os.path.isfile(f"{fn_balance}_{process_id}.json"):
        with open(f'{fn_balance}_{process_id}.json') as json_file:
            temp_dict = json.load(json_file)
            balance_sheet.update(temp_dict)
        if remove:
            try:
                os.remove(f"{fn_balance}_{process_id}.json")
            except OSError as e:
                print(f"Error deleting JSON file {fn_balance}_{process_id}.json: {e}")
        process_id += 1

    process_id = 0
    while os.path.isfile(f"{fn_income}_{process_id}.json"):
        with open(f'{fn_income}_{process_id}.json') as json_file:
            temp_dict = json.load(json_file)
            income_statement.update(temp_dict)
        if remove:
            try:
                os.remove(f"{fn_income}_{process_id}.json")
            except OSError as e:
                print(f"Error deleting JSON file {fn_income}_{process_id}.json: {e}")
        process_id += 1

    process_id = 0
    while os.path.isfile(f"{fn_cap}_{process_id}.json"):
        with open(f'{fn_cap}_{process_id}.json') as json_file:
            temp_dict = json.load(json_file)
            market_cap_dict.update(temp_dict)
        if remove:
            try:
                os.remove(f"{fn_cap}_{process_id}.json")
            except OSError as e:
                print(f"Error deleting JSON file {fn_cap}_{process_id}.json: {e}")
        process_id += 1


def old_refresh_tickers():
    """ The old method of refreshing ticker list. This uses nasdaqlist.txt and otherlisted.txt to get tickers.
        It was replaced by using nasdaq_stocks.csv instead, because that includes industry and market cap data """
    print("Stripping text files to get list of stocks...")
    fhr = open('nasdaqlisted.txt', 'r')
    lines = fhr.readlines()
    for line in lines:
        fields = line.split('|')
        if is_common_stock(fields[1]):
            ticker_dict[fields[0]] = TICKER_NOT_VALIDATED
    fhr.close()
    fhr = open('otherlisted.txt', 'r')
    lines = fhr.readlines()
    for line in lines:
        fields = line.split('|')
        if is_common_stock(fields[1]):
            ticker_dict[fields[0]] = TICKER_NOT_VALIDATED
    fhr.close()
    json.dump(ticker_dict, open(fn_tickers + ".json", "w"))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process refresh options')
    parser.add_argument('--refresh', '-r', action='store_true', dest='refresh',
                        help='flag determines if we refresh the yahoo finance data')
    parser.add_argument('--tickers', '-t', action='store_true', dest='refresh_tickers',
                        help='gets list of stocks from nasdaq_stocks.csv')
    parser.add_argument('--continue', '-c', action='store_true', dest='continue_refresh',
                        help='Refreshes only tickers not already stored in each JSON file')
    parser.add_argument('--multiprocess', '-mc', type=int, nargs='?', default=1, dest='n_processes',
                        help='Specify the number of processes to scrape data with')
    parser.add_argument('--verbose', '-v', action='store_true', dest='verbose',
                        help='Flag for extra print statements')
    parser.add_argument('--validate', action='store_true', dest='validate',
                        help='validates tickers and gets market cap data')
    parser.add_argument('--debug', '-d', action='store_true', dest='debug',
                        help='Reduces size of ticker_dict for debugging purposes')
    args = parser.parse_args()
    verbose = args.verbose
    debug = args.debug

    balance_sheet = {}
    income_statement = {}
    market_cap_dict = {}
    # Tickers are mapped to validity values, 0=invalid, 1=valid. Above market cap and Avg dollar volume threshold
    # A value of -1 means it has not been validated yet; if this dict contains a -1, it means its not fully validated
    # A value of -2 means it was flagged for removal (it should be removed, but for some reason wasn't), due to missing
    # volume, price, or market cap information during the validation process
    ticker_dict = {}
    create_errors_table()

    start = time.time()

    # refresh the tickers, volume, market cap, and sector info based on nasdaq_stocks.csv
    if args.refresh_tickers:
        sector_dict = {}
        price_dict = {}
        with open("nasdaq_stocks.csv", newline="") as csvfile:
            reader = csv.reader(csvfile, delimiter=",")
            next(reader, None)  # skip the header
            for row in reader:
                if any([not cell for cell in row]):
                    continue
                print(row)
                ticker = row[0]
                description = row[1]
                country = row[6]
                sector = row[9]
                industry = row[10]

                if not is_common_stock(description):
                    continue

                price = float(sub(r'[^\d.]', '', row[2]))   # I use a float here instead of Decimal because for my purposes, precision isn't a big deal
                market_cap = float(row[5])
                daily_volume = int(row[8])

                ticker_dict[ticker] = TICKER_NOT_VALIDATED
                market_cap_dict[ticker] = market_cap
                price_dict[ticker] = price
                sector_dict[ticker] = {}
                sector_dict[ticker]["sector"] = sector
                sector_dict[ticker]["industry"] = industry
                sector_dict[ticker]["country"] = country
        json.dump(market_cap_dict, open(fn_cap + '.json', 'w'))
        json.dump(price_dict, open(fn_price + '.json', 'w'))

    else:
        print("Loading ticker list...")
        with open(fn_tickers + '.json') as json_list:
            ticker_dict = json.load(json_list)
        print("Loading price dict")
        with open(fn_price + '.json') as price_dict_file:
            price_dict = json.load(price_dict_file)
        print("Loading sector, industry, and country info from JSON file...")
        with open('sector_info.json') as json_file:
            sector_dict = json.load(json_file)

    print(f"Number of tickers in ticker_dict: {len(ticker_dict)}")

    if debug:
        ticker_dict = {key: ticker_dict[key] for key in list(ticker_dict.keys())[0:100]}  # For debugging purposes, when we want a smaller ticker_dict to work with

    # Validates tickers and gets market cap info
    if args.validate:
        print("Validating tickers and getting market caps...")
        validate_tickers(ticker_dict, market_cap_dict)

    # Retrieve the ticker information (balance sheets, income statements, and market caps), scrape the data in
    # batches, and save in batches.
    if args.refresh and not args.continue_refresh:
        print("Refreshing all stock data...")

        if not is_tickers_validated():
            print("Validating new tickers..")
            validate_tickers(ticker_dict, market_cap_dict, newonly=True)

        ticker_list = get_valid_ticker_list()

        retrieve_data(batch_size, ticker_list, "balance", fn_balance, balance_sheet)
        retrieve_data(batch_size, ticker_list, "income", fn_income, income_statement)
        clean_tickers()

    else:
        print("Loading all stock data from json files...")
        print("Loading quarterly balance sheets from json file...")
        with open(fn_balance + '.json') as json_file:
            balance_sheet = json.load(json_file)
        print("Loading quarterly income statement history from json file...")
        with open(fn_income + '.json') as json_file:
            income_statement = json.load(json_file)

    # Retrieves the data for tickers that have not been retrieved yet (i.e. not in the dictionary yet)
    # Note: balance_sheet, income_statement, and market_cap_dict are already loaded in the args.refresh if-else block
    if args.continue_refresh:
        print("Continuing retrieval of stock data that is not already in json files...")

        # check that volume and market cap exceed specific thresholds for non-validated tickers
        if not is_tickers_validated():
            validate_tickers(ticker_dict, market_cap_dict, newonly=True)

        ticker_list = get_valid_ticker_list()

        # Step 1: Consolidate any JSON sub_files into the dictionaries for the respective metric
        # Iterate through all sub_lists of each process
        print("Consolidating JSON files and removing sub_files...")
        consolidate_json(remove=True)

        # Step 2: Find the tickers in the ticker_dict whose data has not been retrieved yet
        balance_keys = balance_sheet.keys()
        income_keys = income_statement.keys()
        cap_keys = market_cap_dict.keys()
        balance_sublist = [i for i in ticker_list if i not in balance_keys]
        income_sublist = [i for i in ticker_list if i not in income_keys]
        cap_sublist = [i for i in ticker_list if i not in cap_keys]

        # Step 3: Separate the un-retrieved tickers for each metric into the desired number of processes
        p_balance_list = list()
        p_income_list = list()
        p_cap_list = list()
        print("Splitting tickers up between processes...")
        print(f"Number of processes: {args.n_processes}")
        if len(balance_sublist) % args.n_processes == 0:
            p_size = len(balance_sublist) // args.n_processes
        else:  # If the number of processes does not divide evenly into the number of tickers
            # TODO Find better method when # of processes does not divide evenly into # number of tickers
            p_size = (len(balance_sublist) // args.n_processes) + 1
        for i in range(args.n_processes):
            # Store the separate lists of tickers for each process into a list
            p_balance_list.append(balance_sublist[i * p_size: min((i + 1) * p_size, len(balance_sublist))])

        if len(income_sublist) % args.n_processes == 0:
            p_size = len(income_sublist) // args.n_processes
        else:  # If the number of processes does not divide evenly into the number of tickers
            # TODO Find better method when # of processes does not divide evenly into # number of tickers
            p_size = (len(income_sublist) // args.n_processes) + 1
        for i in range(args.n_processes):
            p_income_list.append(income_sublist[i * p_size: min((i + 1) * p_size, len(income_sublist))])

        if len(cap_sublist) % args.n_processes == 0:
            p_size = len(cap_sublist) // args.n_processes
        else:  # If the number of processes does not divide evenly into the number of tickers
            # TODO Find better method when # of processes does not divide evenly into # number of tickers
            p_size = (len(cap_sublist) // args.n_processes) + 1
        for i in range(args.n_processes):
            p_cap_list.append(cap_sublist[i * p_size: min((i + 1) * p_size, len(cap_sublist))])

        # Step 4: Retrieve the data; each list in the process lists gets its own process
        p_ticker_list = list(zip(p_balance_list, p_income_list, p_cap_list))

        print("Creating processes...")
        jobs = []
        for i in range(args.n_processes):
            process = Process(target=create_process, args=(batch_size, p_ticker_list[i], i))
            jobs.append(process)

        print("Starting processes...")
        for j in jobs:
            j.start()

        print("Waiting for processes to finish...")
        for j in jobs:
            j.join()

        # Consolidate the dictionaries for each process into a main dictionary for each metric
        # This is completed through the JSON files that were saved. Then remove the process's JSON files.
        consolidate_json(remove=True)

        # Step 7: Clean the tickers; this also saves the dictionaries into the main JSON file
        clean_tickers()


    # update db with tickers that have the data for balance sheet, income statement, and market cap
    ticker_list = list()
    for matched_ticker in get_valid_ticker_list():
        if matched_ticker in balance_sheet and matched_ticker in income_statement and matched_ticker in market_cap_dict:
            ticker_list.append(matched_ticker)
        else:
            print(f"Not inserting {matched_ticker} into db: Missing Data")
    update_db(ticker_list)

    rank_stocks(fn_stock_info_db)
    end = time.time()

    print(f"Execution time: {end - start}")
