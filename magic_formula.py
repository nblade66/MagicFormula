from yahoofinancials import YahooFinancials
import json
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
import re
from functools import cmp_to_key

fn_balance = 'quarterly_balance_sheet'
fn_income = 'quarterly_income_statement'
fn_cap = 'market_cap_info'
fn_tickers = 'ticker_dict'
batch_size = 5
max_threads = 7  # Somewhere between 10 and 15 threads with batch_size of 10 seems to be allowed
min_market_cap = 50000000
min_dollar_volume = 10000000  # based on 10-day and 90-day average volume
TICKER_VALID = 1
TICKER_INVALID = 0
TICKER_NOT_VALIDATED = -1
TICKER_REMOVE = -2

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
        print(f"Missing {e} information for {ticker}")
        return 0


def get_intangibles(ticker):
    balance_dict = list(balance_sheet[ticker][0].values())[-1]
    try:
        return balance_dict['intangibleAssets']
    except Exception as e:
        print(f"Missing {e} information for {ticker}, trying different method")
        try:
            return get_totalAssets(ticker) - get_netTangibleAssets(ticker) - get_totalLiab(ticker)
        except Exception as e:
            print(f"Missing {e} information for {ticker}")
            return 0


def get_goodwill(ticker):
    balance_dict = list(balance_sheet[ticker][0].values())[-1]
    try:
        return balance_dict['goodWill']
    except Exception as e:
        print(f"Missing {e} information for {ticker}")
        return 0


def get_totalLiab(ticker):
    balance_dict = list(balance_sheet[ticker][0].values())[-1]
    try:
        return balance_dict['totalLiab']
    except Exception as e:
        print(f"missing {e} information for {ticker}, trying different method")
        try:
            return balance_dict['totalLiabilitiesNetMinorityInterest']
        except Exception as e:
            print(f"missing {e} information for {ticker}")
            return 0


def get_totalAssets(ticker):
    balance_dict = list(balance_sheet[ticker][0].values())[-1]
    try:
        return balance_dict['totalAssets']
    except Exception as e:
        print(f"Missing {e} information for {ticker}")
        return 0


def get_netTangibleAssets(ticker):
    balance_dict = list(balance_sheet[ticker][0].values())[-1]
    try:
        return balance_dict['netTangibleAssets']
    except Exception as e:
        print(f"Missing {e} information for {ticker}")
        return 0


def get_totalCurrentAssets(ticker):
    balance_dict = list(balance_sheet[ticker][0].values())[-1]
    try:
        return balance_dict['totalCurrentAssets']
    except Exception as e:
        print(f"Missing {e} information for {ticker}, trying different method")
        try:
            return get_totalAssets(ticker) - balance_dict['totalNonCurrentAssets']
        except Exception as e:
            print(f"Missing {e} information for {ticker}")
            return 0


def get_longTermDebt(ticker):
    balance_dict = list(balance_sheet[ticker][0].values())[-1]
    try:
        return balance_dict['longTermDebt']
    except Exception as e:
        print(f"Missing {e} information for {ticker}")
        return 0


def get_totalCurrentLiabilities(ticker):
    balance_dict = list(balance_sheet[ticker][0].values())[-1]
    try:
        return balance_dict['totalCurrentLiabilities']
    except Exception as e:
        print(f"Missing {e} information for {ticker}, trying different method")
        try:
            return balance_dict['currentLiabilities']
        except Exception as e:
            print(f"Missing {e} information for {ticker}")
            return 0


def get_cash(ticker):
    balance_dict = list(balance_sheet[ticker][0].values())[-1]
    try:
        return balance_dict['cash']
    except Exception as e:
        print(f"Missing {e} information for {ticker}, trying different method")
        try:
            return balance_dict['cashAndCashEquivalents']
        except Exception as e:
            print(f"Missing {e} information for {ticker}")
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
    return market_cap[ticker]


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
    conn = sq.connect(r'stock_info.db', detect_types=sq.PARSE_DECLTYPES)
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
            print(f"Insert data error for ticker {ticker}: {e}. Going to next ticker.")
    if conn:
        conn.close()


# TODO Is there a way to exclude financial industry and utilities from the list? Also, to exclude stuff like ETFs, etc.
#   This could be a good chance to practice BeautifulSoup4 to get sector, industry, and location from Finviz
# TODO Should I also exclude stocks below a certain volume?
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
    if not newonly:
        # Set all tickers as "not validated"
        for ticker in tickers:
            tickers[ticker] = TICKER_NOT_VALIDATED

    ticker_keys = list(tickers.keys())
    if batch_sz == 0:
        batch_sz = len(ticker_keys)
    batches = len(ticker_keys) // batch_sz
    thread_jobs = []
    print("Creating Threads to validate tickers...")
    for i in range(batches + 1):
        ticker_sublist = ticker_keys[i * batch_sz: min((i + 1) * batch_sz, len(ticker_keys))]
        if len(ticker_sublist) == 0:  # This is for when the batch_size evenly divides into the ticker_dict size
            break

        thread = threading.Thread(target=validate_tickers_thread, args=(ticker_sublist, tickers, cap_dict, i, newonly))
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


def validate_tickers_thread(ticker_keys, tickers, cap_dict, batch_no, newonly):
    # ticker_sublist is only to indicate to YahooFinancials what data is retrieved, and to indicate what tickers should
    # be validated. The original "tickers" dict is still the one being modified in the end.
    if newonly:
        ticker_keys = [ticker for ticker in ticker_keys if tickers[ticker] == TICKER_NOT_VALIDATED]
        if len(ticker_keys) == 0:
            if verbose:
                print(f"All tickers in Batch {batch_no + 1} are validated already.")
            return

    print(f"Batch {batch_no + 1}: Validating tickers {ticker_keys}")

    start_loop = time.time()

    yh = YahooFinancials(ticker_keys)
    if verbose:
        print(f"Batch {batch_no + 1}: Retrieving volume and price information from Yahoo Finance...")
    avg_ten_day_volume = yh.get_ten_day_avg_daily_volume()
    current_price = yh.get_current_price()

    if verbose:
        print(f"Batch {batch_no + 1}: Calculating average dollar volumes...")
    missing_data = set()
    avg_ten_day_dollar_volume = {}
    for ticker, value in avg_ten_day_volume.items():
        if ticker is None or value is None or ticker not in current_price or current_price[ticker] is None:
            missing_data.add(ticker)
        else:
            avg_ten_day_dollar_volume[ticker] = value * current_price[ticker]

    # For the sake of efficiency, don't get market caps of already invalid tickers
    new_ticker_keys = []

    ticker_lock.acquire()
    for ticker in ticker_keys:
        if ticker in missing_data:
            tickers[ticker] = TICKER_REMOVE
        elif avg_ten_day_dollar_volume[ticker] > min_dollar_volume:
            new_ticker_keys.append(ticker)
        else:
            tickers[ticker] = TICKER_INVALID
    json.dump(tickers, open(fn_tickers + '.json', 'w'))
    ticker_lock.release()

    if len(new_ticker_keys) == 0:
        if verbose:
            print("No tickers were valid")
        end_loop = time.time()
        print(f"Time elapsed for ticker validation, batch {batch_no + 1}: {end_loop - start_loop}")
        print()
        return

    yh = YahooFinancials(new_ticker_keys)

    if verbose:
        print(f"Batch {batch_no + 1}: Retrieving market cap information from Yahoo Finance...")
    market_caps = yh.get_market_cap()

    ticker_lock.acquire()
    for ticker in new_ticker_keys:
        if ticker not in market_caps or market_caps[ticker] is None:
            tickers[ticker] = TICKER_REMOVE
        elif market_caps[ticker] > min_market_cap:
            if verbose:
                print(f"Setting {ticker} to valid")
            tickers[ticker] = TICKER_VALID
        else:
            if verbose:
                print(f"Setting {ticker} to invalid")
            tickers[ticker] = TICKER_INVALID
    json.dump(tickers, open(fn_tickers + '.json', 'w'))
    ticker_lock.release()

    dict_lock.acquire()
    cap_dict.update(market_caps)
    if verbose:
        print(f"Saving market cap batch {batch_no + 1} to JSON file...")
    json.dump(cap_dict, open(fn_cap + '.json', 'w'))
    dict_lock.release()

    end_loop = time.time()
    print(f"Time elapsed for ticker validation, batch {batch_no + 1}: {end_loop - start_loop}")
    print()


def is_tickers_validated():
    return -1 in ticker_dict.values()


def is_common_stock(description):
    return "Warrant" not in description and "Preferred" not in description and "preferred" not in description and \
           "Unit" not in description and "ETF" not in description and "Index" not in description


# Gets a list of tickers that a valid -> There should be no validation/checking of value outside of this function
def get_valid_ticker_list():
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

    # elif metric == "cap":
    #     print(f"Retrieving market cap information from Yahoo Finance...")
    #     financial_statement = yahoo_financials.get_market_cap()

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


# Checks balance_sheet, income_statement, and market_cap dictionaries for None values and empty list values, and removes
# those entries from the dictionaries, then updates their respective JSON files.
# ticker_dict changes are not saved to the json because info might be missing due to communication errors, and not
# necessarily because the data is missing (e.g. if we made too many requests to Yahoo Finance and the site refuses.
# This way, if we continue retrieving, all the tickers will be retrieved, since they are still in the ticker_dict
# Always called after refreshing data
def clean_tickers():
    print("Cleaning tickers...")
    none_tickers = set()
    global balance_sheet, income_statement, market_cap, ticker_dict
    temp_balance = {}
    temp_income = {}
    temp_cap = {}

    # for k, v in balance_sheet.items():
    #     if v is not None and v != []:
    #         temp_balance[k] = v
    #     else:
    #         none_tickers.add(k)
    # for k, v in income_statement.items():
    #     if v is not None and v != []:
    #         temp_income[k] = v
    #     else:
    #         none_tickers.add(k)
    # for k, v in market_cap.items():
    #     if v is not None and v != []:
    #         temp_cap[k] = v
    #     else:
    #         none_tickers.add(k)

    for ticker, value in ticker_dict.items():
        # if get_net_working_capital(ticker) < 0 or get_fixed_assets(ticker) < 0:
        #     none_tickers.add(ticker)

        # Logic is: Remove ticker if flagged for removal OR for valid tickers (for which all data should be retrieved),
        # remove tickers with missing info
        if value == TICKER_REMOVE or\
                (value == TICKER_VALID and (ticker not in balance_sheet or ticker not in income_statement
                                            or ticker not in market_cap or balance_sheet[ticker] is None
                                            or income_statement[ticker] is None or market_cap[ticker] is None
                                            or balance_sheet[ticker] == [] or income_statement[ticker] == []
                                            or market_cap[ticker] == [])):
            none_tickers.add(ticker)

    ticker_dict = {ticker: value for ticker, value in ticker_dict.items() if ticker not in none_tickers}
    json.dump(ticker_dict, open(fn_tickers + '.json', 'w'))

    # Modify dictionaries to only contain tickers that have all the data; save them to json files
    for ticker in get_valid_ticker_list():
        temp_balance[ticker] = balance_sheet[ticker]
        temp_income[ticker] = income_statement[ticker]
        temp_cap[ticker] = market_cap[ticker]

    balance_sheet = temp_balance
    json.dump(balance_sheet, open(fn_balance + '.json', 'w'))
    income_statement = temp_income
    json.dump(income_statement, open(fn_income + '.json', 'w'))
    market_cap = temp_cap
    json.dump(market_cap, open('market_cap_info.json', 'w'))


def create_process(batch_sz, p_tickers, p_id):
    # create empty dictionaries for the process, since the process does not have access to the global variables
    # These will be accessed through their saved JSON files.
    balance_sheet = {}
    income_statement = {}
    market_cap = {}
    retrieve_data(batch_sz, p_tickers[0], "balance", f"{fn_balance}_{p_id}", balance_sheet)
    retrieve_data(batch_sz, p_tickers[1], "income", f"{fn_income}_{p_id}", income_statement)

    # Commented out because validate_tickers already gets the market cap
    # retrieve_data(batch_sz, p_tickers[2], "cap", f"{fn_cap}_{p_id}", market_cap)


# Takes the various JSON files from processes and updates the dictionaries: balance_sheet, income_statement, market_cap
# Also removes the JSON files after consolidating
def consolidate_json(remove=False):
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
            market_cap.update(temp_dict)
        if remove:
            try:
                os.remove(f"{fn_cap}_{process_id}.json")
            except OSError as e:
                print(f"Error deleting JSON file {fn_cap}_{process_id}.json: {e}")
        process_id += 1


# Returns dict, {'ticker': {'sector': sector, 'industry': industry, 'location': location}}
# TODO Unescape unicode and html code characters; low priority meh
def scrape_sector(ticker):
    try:
        URL = f'https://finance.yahoo.com/quote/{ticker.lower()}/profile?p={ticker.lower()}'
        page = requests.get(URL)

        soup = BeautifulSoup(page.content, 'html.parser')

        results = soup.find_all('span', class_='Fw(600)')

        location = soup.find_all('p', class_='D(ib) W(47.727%) Pend(40px)')

        print(f"Ticker: {ticker}")

        # location = str(location[0]).split('15 -->')[1].split('<!-- /react-text')[0]

        # location = list(filter(lambda a: a != ' ', re.findall(r'[0-9]*(\D*)[0-9]+[http]', location[0].text)))[-1]

        location_text = location[0].text
        sector = results[0].text
        industry = results[1].text
    except IndexError as e:
        print(f'Ticker: {ticker}, Error in finding profile: {e}. Setting sector, industry, and country as "TBD"')
        location_text = '1234TBD1234http'
        sector = 'TBD'
        industry = 'TBD'
    except Exception as e:
        print(f'Ticker: {ticker}, Error in finding profile: {e}. Setting sector, industry, and country as "TBD"')
        location_text = '1234TBD1234http'
        sector = 'TBD'
        industry = 'TBD'

    # location_text = "".join(location_text.split())
    # TODO This doesn't work perfectly. E.g. frickin UK zip codes can have letters, and some companies don't even have
    #   phone numbers or websites, which is what the below REGEX relies on. It's not too important right now, though
    try:
        country = re.findall(r'[0-9]*(\D*)[0-9 \-]+[http]', location_text)[-1]
    except IndexError as e:
        print(f'Ticker: {ticker}, Error in finding country: {e}. Setting country as "TBD"')
        country = "TBD"

    print(f"Sector: {sector}, Industry: {industry}, Country: {country}")

    return {ticker.upper(): {'sector': sector, 'industry': industry, 'country': country}}


def scrape_sector_all(tickers, batch_sz=0):
    if batch_sz == 0:
        for ticker in tickers:
            sect_lock.acquire()
            sector_dict.update(scrape_sector(ticker))
            sect_lock.release()

    else:
        batches = len(tickers) // batch_sz
        thread_jobs = []
        print("Creating Threads...")
        for i in range(batches + 1):
            ticker_sublist = tickers[i * batch_sz: min((i + 1) * batch_sz, len(tickers))]
            if len(ticker_sublist) == 0:  # This is for when the batch_size evenly divides into the ticker_dict size
                break

            thread = threading.Thread(target=scrape_sector_all, args=(ticker_sublist,))
            thread_jobs.append(thread)

        running = 0
        next_count = 0
        join_count = 0
        while join_count < len(thread_jobs):
            if running < max_threads and next_count < len(thread_jobs):
                thread_jobs[next_count].start()
                time.sleep(1)  # So that requests don't come it TOO fast
                running += 1
                next_count += 1
            elif join_count < next_count:
                thread_jobs[join_count].join()
                running -= 1
                join_count += 1

    sect_lock.acquire()
    json.dump(sector_dict, open('sector_info.json', 'w'))
    sect_lock.release()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process refresh options')
    parser.add_argument('--refresh', '-r', action='store_true', dest='refresh',
                        help='flag determines if we refresh the yahoo finance data')
    parser.add_argument('--tickers', '-t', action='store_true', dest='refresh_tickers',
                        help='gets list of stocks from text files')
    parser.add_argument('--continue', '-c', action='store_true', dest='continue_refresh',
                        help='Refreshes only tickers not already stored in each JSON file')
    parser.add_argument('--multiprocess', '-mc', type=int, nargs='?', default=1, dest='n_processes',
                        help='Specify the number of processes to scrape data with')
    parser.add_argument('--sector', '-s', type=int, nargs='?', const=0, default=-1, dest='retrieve_sector',
                        help='Retrieves company sector, industry, and country information')
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
    market_cap = {}
    # Tickers are mapped to validity values, 0=invalid, 1=valid. Above market cap and Avg dollar volume threshold
    # A value of -1 means it has not been validated yet; if this dict contains a -1, it means its not fully validated
    # A value of -2 means it was flagged for removal (it should be removed, but for some reason wasn't), due to missing
    # volume, price, or market cap information during the validation process
    ticker_dict = {}

    start = time.time()
    # Refresh the ticker list based on the nasdaqlisted.txt and otherlisted.txt files in the directory
    if args.refresh_tickers:
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
        json.dump(ticker_dict, open("ticker_dict.json", "w"))
    else:
        print("Loading ticker list...")
        with open('ticker_dict.json') as json_list:
            ticker_dict = json.load(json_list)

    print(f"Number of tickers in ticker_dict: {len(ticker_dict)}")

    if debug:
        ticker_dict = {key: ticker_dict[key] for key in list(ticker_dict.keys())[0:20]}  # For debugging purposes, when we want a smaller ticker_dict to work with

    # Validates tickers and gets market cap info
    if args.validate:
        print("Validating tickers and getting market caps...")
        validate_tickers(ticker_dict, market_cap)

    # Retrieve the ticker information (balance sheets, income statements, and market caps), scrape the data in
    # batches, and save in batches.
    if args.refresh and not args.continue_refresh:
        print("Refreshing all stock data...")

        # check that volume and market cap exceed specific thresholds, and update market cap data
        # Commented out because I think user should have the option of re-validating or not
        # if not args.validate:
        #     validate_tickers(ticker_dict, market_cap)
        if not args.validate and not is_tickers_validated():
            validate_tickers(ticker_dict, market_cap, newonly=True)

        ticker_list = get_valid_ticker_list()

        retrieve_data(batch_size, ticker_list, "balance", fn_balance, balance_sheet)
        retrieve_data(batch_size, ticker_list, "income", fn_income, income_statement)
        # commented out b/c validate_tickers gets market cap
        # retrieve_data(batch_size, ticker_dict, "cap", fn_cap, market_cap)
        clean_tickers()
    else:
        print("Loading all stock data from json files...")
        print("Loading quarterly balance sheets from json file...")
        with open(fn_balance + '.json') as json_file:
            balance_sheet = json.load(json_file)
        print("Loading quarterly income statement history from json file...")
        with open(fn_income + '.json') as json_file:
            income_statement = json.load(json_file)
        print("Loading market cap information from json file...")
        with open(fn_cap + '.json') as json_file:
            market_cap = json.load(json_file)

    # Retrieves the data for tickers that have not been retrieved yet (i.e. not in the dictionary yet)
    # Note: balance_sheet, income_statement, and market_cap are already loaded in the args.refresh if-else block
    if args.continue_refresh:
        print("Continuing retrieval of stock data that is not already in json files...")

        # check that volume and market cap exceed specific thresholds for non-validated tickers
        if not args.validate and not is_tickers_validated():
            validate_tickers(ticker_dict, market_cap, newonly=True)

        ticker_list = get_valid_ticker_list()

        # Step 1: Consolidate any JSON sub_files into the dictionaries for the respective metric
        # Iterate through all sub_lists of each process
        print("Consolidating JSON files and removing sub_files...")
        consolidate_json(remove=True)

        # Step 2: Find the tickers in the ticker_dict whose data has not been retrieved yet
        balance_keys = balance_sheet.keys()
        income_keys = income_statement.keys()
        cap_keys = market_cap.keys()
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

    if args.retrieve_sector != -1:
        print(f"Retrieving sector, industry, and country info.... Batch Size: {args.retrieve_sector}")
        sector_dict = {}
        # TODO Should I modify this to just use the same "batch_size" variable as when I get financial info?
        scrape_sector_all(ticker_dict, batch_sz=args.retrieve_sector)
    else:
        print("Loading sector, industry, and country info from JSON file...")
        with open('sector_info.json') as json_file:
            sector_dict = json.load(json_file)

    # update db with tickers that have the data for balance sheet, income statement, and market cap
    ticker_list = list()
    for matched_ticker in get_valid_ticker_list():
        if matched_ticker in balance_sheet and matched_ticker in income_statement and matched_ticker in market_cap:
            ticker_list.append(matched_ticker)
        else:
            print(f"Not inserting {matched_ticker} into db: Missing Data")
    update_db(ticker_list)

    rank_stocks('stock_info.db')
    end = time.time()

    print(f"Execution time: {end - start}")
