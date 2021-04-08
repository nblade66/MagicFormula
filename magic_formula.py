from yahoofinancials import YahooFinancials
import json
import argparse
import time
import os
from multiprocessing import Process
import sqlite3 as sq
import pandas as pd


def get_roc(ticker):
    return get_ebit(ticker) / (get_net_working_capital(ticker) + get_fixed_assets(ticker))


def get_yield(ticker):
    return get_ebit(ticker) / get_ev(ticker)


def get_ev(ticker):
    total_debt = get_longTermDebt(ticker) + get_totalCurrentLiabilities(ticker)
    return get_market_cap(ticker) + total_debt - get_cash(ticker)


def get_net_working_capital(ticker):
    return get_totalCurrentAssets(ticker) - get_totalCurrentLiabilities(ticker)


# TODO Find a company with goodwill on its balance sheet and check if netTangibleAssets subtracts off goodwill or not
#   (Or if it only subtracts off "intangibleAssets")
def get_fixed_assets(ticker):
    return get_netTangibleAssets(ticker) - get_totalCurrentAssets(ticker) + get_totalLiab(ticker)


def get_totalLiab(ticker):
    balance_dict = list(balance_sheet[ticker][0].values())[0]
    try:
        return balance_dict['totalLiab']
    except Exception as e:
        print(f"Missing {e} information for {ticker}")
        return 0


def get_netTangibleAssets(ticker):
    balance_dict = list(balance_sheet[ticker][0].values())[0]
    try:
        return balance_dict['netTangibleAssets']
    except Exception as e:
        print(f"Missing {e} information for {ticker}")
        return 0


def get_totalCurrentAssets(ticker):
    balance_dict = list(balance_sheet[ticker][0].values())[0]
    try:
        return balance_dict['totalCurrentAssets']
    except Exception as e:
        print(f"Missing {e} information for {ticker}")
        return 0


def get_longTermDebt(ticker):
    balance_dict = list(balance_sheet[ticker][0].values())[0]
    try:
        return balance_dict['longTermDebt']
    except Exception as e:
        print(f"Missing {e} information for {ticker}")
        return 0


def get_totalCurrentLiabilities(ticker):
    balance_dict = list(balance_sheet[ticker][0].values())[0]
    try:
        return balance_dict['totalCurrentLiabilities']
    except Exception as e:
        print(f"Missing {e} information for {ticker}")
        return 0


def get_cash(ticker):
    balance_dict = list(balance_sheet[ticker][0].values())[0]
    try:
        return balance_dict['cash']
    except Exception as e:
        print(f"Missing {e} information for {ticker}")
        return 0


def get_ebit(ticker):
    return list(income_statement[ticker][0].values())[0]['ebit']


def get_market_cap(ticker):
    return market_cap[ticker]


def insert_data(conn, ticker_info):
    sql = ''' REPLACE INTO stock_info (ticker, roc, yield, market_cap)
              VALUES(?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, ticker_info)
    conn.commit()


# TODO Add a column for the date of the latest report? That way I can filter by date
def update_db(tickers):
    print("Updating database...")
    conn = sq.connect(r'stock_info.db')
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS stock_info")
    cursor.execute('''CREATE TABLE IF NOT EXISTS stock_info (
    ticker text PRIMARY KEY,
    roc real NOT NULL,
    yield real NOT NULL,
    market_cap int NOT NULL
    );''')
    for ticker in tickers:
        try:
            data = (ticker, get_roc(ticker), get_yield(ticker), get_market_cap(ticker))
            insert_data(conn, data)
        except Exception as e:
            print(f"Insert data error: {e}. Going to next ticker.")
    print(pd.read_sql_query("SELECT * FROM stock_info", conn))
    if conn:
        conn.close()


def get_mf_rankings(db, tickers):
    # TODO Filter out the stocks below a certain market cap
    # TODO Generate the magic formula rankings based on Return on Capital and Earning Yield
    # TODO Return a database that's sorted by rankings?
    None


# TODO This is mainly for decreasing the time to retrieve data. Most filtering, otherwise, should be done in SQL
def filter_tickers(cap):
    ticker_temp = []
    for i, ticker in enumerate(ticker_list):
        if get_market_cap(ticker) >= cap:
            ticker_temp.append(ticker)

    return ticker_temp


# TODO Add file_name to the argument for the purposes of multi_processing
def retrieve_data(batch_sz, tickers, metric, file_name, balance, income, cap):
    if batch_sz == 0:
        batch_sz = len(tickers)
    batches = len(tickers) // batch_sz
    for i in range(batches + 1):
        start_loop = time.time()
        ticker_sublist = tickers[i * batch_sz: min((i + 1) * batch_sz, len(tickers))]
        if len(ticker_sublist) == 0:  # This is for when the batch_size evenly divides into the ticker_list size
            break

        print(f"Batch {i + 1}: Tickers to be retrieved are: {ticker_sublist}")
        yahoo_financials = YahooFinancials(ticker_sublist)

        if metric == "balance":
            print(f"Retrieving annual balance sheets from Yahoo Finance...")
            balance.update(yahoo_financials.get_financial_stmts('annual', 'balance')['balanceSheetHistory'])

            print(f"Saving batch {i + 1} to JSON file...")
            json.dump(balance, open(file_name + '.json', 'w'))

        elif metric == "income":
            print(f"Retrieving annual income statement history from Yahoo Finance...")
            income.update(yahoo_financials.get_financial_stmts('annual', 'income')['incomeStatementHistory'])

            print(f"Saving batch {i + 1} to JSON file...")
            json.dump(income, open(file_name + '.json', 'w'))

        elif metric == "cap":
            print(f"Retrieving market cap information from Yahoo Finance...")
            cap.update(yahoo_financials.get_market_cap())

            print(f"Saving batch {i + 1} to JSON file...")
            json.dump(cap, open(file_name + '.json', 'w'))

        else:
            print("Metric entered is not recognized.")

        end_loop = time.time()

        print(f"Time elapsed for batch {i + 1}: {end_loop - start_loop}, metric: {metric}")
        print()


# Checks balance_sheet, income_statement, and market_cap dictionaries for None values and removes those entries from
# the dictionaries and the ticker_list, then updates their respective JSON files. Always call this after refreshing data
def clean_tickers():
    none_tickers = set()
    global balance_sheet, income_statement, market_cap, ticker_list
    temp_balance = {}
    temp_income = {}
    temp_cap = {}
    for k, v in balance_sheet.items():
        if v is not None:
            temp_balance[k] = v
        else:
            none_tickers.add(k)
    for k, v in income_statement.items():
        if v is not None:
            temp_income[k] = v
        else:
            none_tickers.add(k)
    for k, v in market_cap.items():
        if v is not None:
            temp_cap[k] = v
        else:
            none_tickers.add(k)

    balance_sheet = temp_balance
    json.dump(balance_sheet, open('annual_balance_sheet.json', 'w'))
    income_statement = temp_income
    json.dump(income_statement, open('annual_income_statement.json', 'w'))
    market_cap = temp_cap
    json.dump(market_cap, open('market_cap_info.json', 'w'))

    ticker_list = [i for i in ticker_list if i not in none_tickers]
    json.dump(ticker_list, open('ticker_list.json', 'w'))


def create_process(batch_sz, p_tickers, p_id):
    # create empty dictionaries for the process, since the process does not have access to the global variables
    balance_sheet = {}
    income_statement = {}
    market_cap = {}
    fn_balance = 'annual_balance_sheet'
    fn_income = 'annual_income_statement'
    fn_cap = 'market_cap_info'
    retrieve_data(batch_sz, p_tickers[0], "balance", f"{fn_balance}_{p_id}", balance_sheet, income_statement, market_cap)
    retrieve_data(batch_sz, p_tickers[1], "income", f"{fn_income}_{p_id}", balance_sheet, income_statement, market_cap)
    retrieve_data(batch_sz, p_tickers[2], "cap", f"{fn_cap}_{p_id}", balance_sheet, income_statement, market_cap)


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
                print(f"Error deleting JSON file {fn_balance}_{process_id}.json")
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
                print(f"Error deleting JSON file {fn_income}_{process_id}.json")
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
                print(f"Error deleting JSON file {fn_cap}_{process_id}.json")
        process_id += 1


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process refresh options')
    parser.add_argument('--refresh', '-r', action='store_true', dest='refresh', help='flag determines if we refresh the yahoo finance data')
    parser.add_argument('--tickers', '-t', action='store_true', dest='refresh_tickers', help='gets list of stocks from text files')
    parser.add_argument('--update_market_caps', '-m', action='store_true', dest='refresh_market_caps', help='refreshes market cap info')
    parser.add_argument('--continue', '-c', action='store_true', dest='continue_refresh', help='Refreshes only tickers not in each JSON file')
    parser.add_argument('--multiprocess', '-mc', type=int, nargs='?', const=1, default=1, dest='n_processes',
                        help='Specify the number of processes to scrape data with')
    args = parser.parse_args()

    fn_balance = 'annual_balance_sheet'
    fn_income = 'annual_income_statement'
    fn_cap = 'market_cap_info'

    start = time.time()

    # Save the most recent balance sheets that are fetched, so that I don't need to fetch every time. Have the option to
    # refresh the market caps.
    if args.refresh_tickers:
        print("Stripping text files to get list of stocks...")
        fhr = open('nasdaqlisted.txt', 'r')
        lines = fhr.readlines()
        ticker_list = []
        for line in lines:
            fields = line.split('|')
            ticker_list.append(fields[0])
        fhr.close()
        fhr = open('otherlisted.txt', 'r')
        lines = fhr.readlines()
        for line in lines:
            fields = line.split('|')
            ticker_list.append(fields[0])
        json.dump(ticker_list, open("ticker_list.json", "w"))
    else:
        print("Loading ticker list...")
        with open('ticker_list.json') as json_list:
            ticker_list = json.load(json_list)

    print("Retrieving ticker information from Yahoo Finance...")
    yahoo_financials = YahooFinancials(ticker_list)

    batch_size = 10

    if not args.refresh and args.refresh_market_caps:
        with open('market_cap_info.json') as json_list:
            market_cap = json_list
        cap_keys = market_cap.keys()
        print(f"Keys in market_cap dictionary: {cap_keys}")
        yahoo_financials = YahooFinancials(cap_keys)

        print("Retrieving market cap information from Yahoo Finance...")
        market_cap = yahoo_financials.get_market_cap()
        json.dump(market_cap, open('market_cap_info.json', 'w'))

    # (for debugging) ticker_list = ['FB', 'UAL', 'UAA', 'UAV'] # If not debugging, make sure to use -t flag

    # If refreshing the ticker information (balance sheets, income statements, and market caps), scrape the data in
    # batches, and save in batches.
    # TODO Implement multiprocess webscraping
    if args.refresh and not args.continue_refresh:
        balance_sheet = {}
        income_statement = {}
        market_cap = {}
        retrieve_data(batch_size, ticker_list, "balance", fn_balance, balance_sheet, income_statement, market_cap)
        retrieve_data(batch_size, ticker_list, "income", fn_income, balance_sheet, income_statement, market_cap)
        retrieve_data(batch_size, ticker_list, "cap", fn_cap, balance_sheet, income_statement, market_cap)
        clean_tickers()
    else:
        print("Loading annual balance sheets from json file...")
        with open(fn_balance + '.json') as json_file:
            balance_sheet = json.load(json_file)
        print("Loading annual income statement history from json file...")
        with open(fn_income + '.json') as json_file:
            income_statement = json.load(json_file)
        print("Loading market cap information from json file...")
        with open(fn_cap + '.json') as json_file:
            market_cap = json.load(json_file)

    ticker_list = ticker_list[0:60]
    # Retrieves the data for tickers that have not been retrieved yet (i.e. not in the dictionary yet)
    # Note: balance_sheet, income_statement, and market_cap are already loaded in the args.refresh if-else block
    # TODO Implement multiprocess webscraping
    if args.continue_refresh:
        # Step 1: Consolidate any JSON sub_files into the dictionaries for the respective metric
        # Iterate through all sub_lists of each process
        print("Consolidating JSON files and removing sub_files...")
        consolidate_json()

        # Step 2: Find the tickers in the ticker_list whose data has not been retrieved yet
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
            p_balance_list.append(balance_sublist[i * p_size: min((i+1) * p_size, len(balance_sublist))])

        if len(income_sublist) % args.n_processes == 0:
            p_size = len(income_sublist) // args.n_processes
        else:  # If the number of processes does not divide evenly into the number of tickers
            # TODO Find better method when # of processes does not divide evenly into # number of tickers
            p_size = (len(income_sublist) // args.n_processes) + 1
        for i in range(args.n_processes):
            p_income_list.append(income_sublist[i * p_size: min((i+1) * p_size, len(income_sublist))])

        if len(cap_sublist) % args.n_processes == 0:
            p_size = len(cap_sublist) // args.n_processes
        else:  # If the number of processes does not divide evenly into the number of tickers
            # TODO Find better method when # of processes does not divide evenly into # number of tickers
            p_size = (len(cap_sublist) // args.n_processes) + 1
        for i in range(args.n_processes):
            p_cap_list.append(cap_sublist[i * p_size: min((i+1) * p_size, len(cap_sublist))])

        # TODO Step 4: Retrieve the data; each list in the process lists gets its own process
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

        # TODO Step 6: Consolidate the dictionaries for each process into a main dictionary for each metric
        #   This is completed through the JSON files that were saved. Then remove the process's JSON files.
        consolidate_json(remove=True)

        # Step 7: Clean the tickers; this also saves the dictionaries into the main JSON file
        clean_tickers()

    update_db(ticker_list)
    # TODO Filter out the stocks with annual reports that are not within the last year
    ticker_list = filter_tickers(50000000)

    end = time.time()

    print(f"Execution time: {end - start}")
