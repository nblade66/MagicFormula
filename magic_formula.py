from yahoofinancials import YahooFinancials
import pickle
import json
import argparse
import time
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
    sql = ''' REPLACE INTO stock_info (ticker, roc, yield)
              VALUES(?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, ticker_info)
    conn.commit()


def update_db(tickers):
    print("Updating database...")
    conn = sq.connect(r'stock_info.db')
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS stock_info")
    cursor.execute('''CREATE TABLE IF NOT EXISTS stock_info (
    ticker text PRIMARY KEY,
    roc real NOT NULL,
    yield real NOT NULL
    );''')
    for ticker in tickers:
        try:
            data = (ticker, get_roc(ticker), get_yield(ticker))
            insert_data(conn, data)
        except Exception as e:
            print(f"Insert data error: {e}. Going to next ticker.")
    print(pd.read_sql_query("SELECT * FROM stock_info", conn))
    if conn:
        conn.close()
    # TODO Puts data into a database and filters out any stocks with missing data


def get_mf_rankings(db, tickers):
    # TODO Filter out the stocks below a certain market cap
    # TODO Generate the magic formula rankings based on Return on Capital and Earning Yield
    # TODO Return a database that's sorted by rankings?
    None


def filter_tickers(cap):
    ticker_temp = []
    for i, ticker in enumerate(ticker_list):
        if get_market_cap(ticker) >= cap:
            ticker_temp.append(ticker)

    return ticker_temp


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process refresh options')
    parser.add_argument('--refresh', '-r', action='store_true', dest='refresh', help='flag determines if we refresh the yahoo finance data')
    parser.add_argument('--tickers', '-t', action='store_true', dest='refresh_tickers', help='gets list of stocks from text files')
    parser.add_argument('--update_market_caps', '-m', action='store_true', dest='refresh_market_caps', help='refreshes market cap info')
    parser.add_argument('--continue', '-c', action='store_true', dest='continue_refresh', help='Refreshes only tickers not in each JSON file')
    args = parser.parse_args()

    start = time.time()

    # Save the most balance sheets that are fetched, so that I don't need to fetch every time. Have the option to
    # refresh the balance sheets.

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
        temp = {'ticker_list': ticker_list}
        json.dump(temp, open("ticker_list.json", "w"))
    else:
        print("Loading ticker list...")
        with open('ticker_list.json') as json_list:
            ticker_list = json.load(json_list)['ticker_list']

    print("Retrieving ticker information from Yahoo Finance...")
    yahoo_financials = YahooFinancials(ticker_list)

    # TODO Modify to only update market caps of tickers that are in market_cap_info.json
    if not args.refresh and args.refresh_market_caps:
        print("Retrieving market cap information from Yahoo Finance...")
        market_cap = yahoo_financials.get_market_cap()
        json.dump(market_cap, open('market_cap_info.json', 'w'))

    ticker_list = ['FB', 'UAL']

    # If refreshing the ticker information (balance sheets, income statements, and market caps), scrape the data in
    # batches, and save in batches.
    # TODO Accept a --continue argument that continues the web scraping using only tickers that aren't already in the
    #   JSON data structure
    if args.refresh:
        balance_sheet = {}
        income_statement = {}
        market_cap = {}
        batch_size = 1
        batches = len(ticker_list) // batch_size
        for i in range(batches + 1):
            start_loop = time.time()
            ticker_sublist = ticker_list[i * batch_size: min((i + 1) * batch_size, len(ticker_list))]
            if len(ticker_sublist) == 0:    # This is for when the batch_size evenly divides into the ticker_list size
                break

            yahoo_financials = YahooFinancials(ticker_sublist)

            print(f"Batch {i + 1}: Retrieving annual balance sheets from Yahoo Finance...")
            balance_sheet.update(yahoo_financials.get_financial_stmts('annual', 'balance')['balanceSheetHistory'])

            print(f"Batch {i + 1}: Retrieving annual income statement history from Yahoo Finance...")
            income_statement.update(yahoo_financials.get_financial_stmts('annual', 'income')['incomeStatementHistory'])

            print(f"Batch {i + 1}: Retrieving market cap information from Yahoo Finance...")
            market_cap.update(yahoo_financials.get_market_cap())

            # Use JSON to store balance sheets, income statements, and market cap
            print(f"Saving batch {i + 1} to JSON file...")
            json.dump(balance_sheet, open('annual_balance_sheet.json', 'w'))
            json.dump(income_statement, open('annual_income_statement.json', 'w'))
            json.dump(market_cap, open('market_cap_info.json', 'w'))

            end_loop = time.time()

            print(f"Time elapsed for batch {i+1}: {end_loop - start_loop}")
            print()
    else:
        print("Loading annual balance sheets from json file...")
        with open('annual_balance_sheet.json') as json_file:
            balance_sheet = json.load(json_file)
        print("Loading annual income statement history from json file...")
        with open('annual_income_statement.json') as json_file:
            income_statement = json.load(json_file)
        print("Loading market cap information from json file...")
        with open('market_cap_info.json') as json_file:
            market_cap = json.load(json_file)


    if args.continue_refresh:
        # TODO Find what tickers are already in balance sheet, income statement, and market cap
        #   Create a new temp_ticker_list that has all tickers EXCEPT those that already have data
        #   Continue finding the data for the tickers in the temp_ticker_list (balance sheet, income, and market cap
        #   each get their own ticker list)
        None


    update_db(ticker_list)
    ticker_list = filter_tickers(50000000)

    end = time.time()

    print(f"Execution time: {end - start}")