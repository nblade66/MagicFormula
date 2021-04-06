from yahoofinancials import YahooFinancials
import pickle
import json
import argparse
import time
import sqlite3 as sq
import pandas as pd


# TODO Rewrite to take a balance sheet dictionary object in as an argument
def get_net_working_capital(ticker):
    balance_dict = list(balance_sheet[ticker][0].values())[0]
    # print(balance_dict)
    return balance_dict['totalCurrentAssets'] - balance_dict['totalCurrentLiabilities']


# TODO Rewrite to take a balance sheet dictionary object in as an argument
def get_fixed_assets(ticker):
    balance_dict = list(balance_sheet[ticker][0].values())[0]
    return balance_dict['netTangibleAssets'] - balance_dict['totalCurrentAssets'] + balance_dict['totalLiab']


# TODO Rewrite to take an ebit dictionary object in as an argument
def get_roc(ticker):
    # TODO get ebit from income statement
    return get_ebit(ticker) / (get_net_working_capital(ticker) + get_fixed_assets(ticker))


# TODO Rewrite to take a balance sheet and market cap dictionary object in as an argument
def get_ev(ticker):
    balance_dict = list(balance_sheet[ticker][0].values())[0]
    total_debt = balance_dict['longTermDebt'] + balance_dict['totalCurrentLiabilities']
    return get_market_cap(ticker) + total_debt - balance_dict['cash']


# TODO Rewrite to take an ebit dictionary object in as an argument
def get_yield(ticker):
    # TODO get ebit from income statement
    return get_ebit(ticker) / get_ev(ticker)


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


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process refresh options')
    parser.add_argument('--refresh', '-r', action='store_true', dest='refresh', help='flag determines if we refresh the yahoo finance data')
    parser.add_argument('--strip', '-s', action='store_true', dest='refresh_stocks', help='gets list of stocks from text files')
    args = parser.parse_args()

    start = time.time()

    # Save the most balance sheets that are fetched, so that I don't need to fetch every time. Have the option to
    # refresh the balance sheets.

    if args.refresh_stocks:
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
        pickle.dump(ticker_list, open("ticker_list.p", "wb"))
    else:
        print("Loading ticker list...")
        ticker_list = pickle.load(open("ticker_list.p", "rb"))

    if args.refresh:
        # Probably don't have to save the YahooFinancials object, since I don't think it does any webscraping
        print("Retrieving ticker information from Yahoo Finance...")
        yahoo_financials = YahooFinancials(ticker_list)

        print("Retrieving annual balance sheets from Yahoo Finance...")
        balance_sheet = yahoo_financials.get_financial_stmts('annual', 'balance')['balanceSheetHistory']

        print("Retrieving annual income statement history from Yahoo Finance...")
        income_statement = yahoo_financials.get_financial_stmts('annual', 'income')['incomeStatementHistory']

        print("Retrieving market cap information from Yahoo Finance...")
        market_cap = yahoo_financials.get_market_cap()

        pickle.dump(yahoo_financials, open("yh_finance.p", "wb"))

        # Use JSON to store balance sheets, income statements, and market cap
        json.dump(balance_sheet, open('annual_balance_sheet.json', 'w'))
        json.dump(income_statement, open('annual_income_statement.json', 'w'))
        json.dump(market_cap, open('market_cap_info.json', 'w'))
    else:
        print("Loading ticker information from disk...")
        yahoo_financials = pickle.load(open("yh_finance.p", "rb"))

        print("Loading annual balance sheets from json file...")
        with open('annual_balance_sheet.json') as json_file:
            balance_sheet = json.load(json_file)
        print("Loading annual income statement history from json file...")
        with open('annual_income_statement.json') as json_file:
            income_statement = json.load(json_file)
        print("Loading market cap information from json file...")
        with open('market_cap_info.json') as json_file:
            market_cap = json.load(json_file)

    update_db(ticker_list)
    # TODO Test getting the most recent date from the annual income statement

    end = time.time()

    print(f"Execution time: {end - start}")