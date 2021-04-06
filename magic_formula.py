from yahoofinancials import YahooFinancials
import pickle
import argparse
import time


def get_net_working_capital(yh_f, ticker):
    balance_sheet = yh_f.get_financial_stmts('annual', 'balance')['balanceSheetHistory']
    balance_dict = list(balance_sheet[ticker][0].values())[0]
    # print(balance_dict)
    return balance_dict['totalCurrentAssets'] - balance_dict['totalCurrentLiabilities']


def get_fixed_assets(yh_f, ticker):
    balance_sheet = yh_f.get_financial_stmts('annual', 'balance')['balanceSheetHistory']
    balance_dict = list(balance_sheet[ticker][0].values())[0]
    return balance_dict['netTangibleAssets'] - balance_dict['totalCurrentAssets'] + balance_dict['totalLiab']


def get_roc(yh_f, ticker):
    return yh_f.get_ebit()[ticker] / (get_net_working_capital(yh_f, ticker) + get_fixed_assets(yh_f, ticker))


def get_ev(yh_f, ticker):
    balance_sheet = yh_f.get_financial_stmts('annual', 'balance')['balanceSheetHistory']
    balance_dict = list(balance_sheet[ticker][0].values())[0]
    total_debt = balance_dict['longTermDebt'] + balance_dict['totalCurrentLiabilities']
    return yh_f.get_market_cap()[ticker] + total_debt - balance_dict['cash']


def get_yield(yh_f, ticker):
    return yh_f.get_ebit()[ticker] / get_ev(yh_f, ticker)


def update_db(tickers):
    None


def get_mf_rankings(yh_f, tickers):
    None


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process refresh options')
    parser.add_argument('--refresh', '-r', action='store_true', dest='refresh', help='flag determines if we refresh the yahoo finance data')
    parser.add_argument('--strip', '-s', action='store_true', dest='refresh_stocks', help='gets list of stocks from text files')
    args = parser.parse_args()

    start = time.time()

    test = YahooFinancials('XPEV')
    print(test.get_stock_quote_type_data())

    # Save the most balance sheets that are fetched, so that I don't need to fetch every time. Have the option to
    # refresh the balance sheets.
    stocks = ['MVIS']
    if args.refresh:
        print("Refreshing values...")
        yahoo_financials = YahooFinancials(stocks)
        pickle.dump(yahoo_financials, open("yh_finance.p", "wb"))
    else:
        print("Loading values...")
        yahoo_financials = pickle.load(open("yh_finance.p", "rb"))

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

    for stock in stocks:
        # print(f"Fixed Assets: {get_fixed_assets(yahoo_financials, stock)}")
        # print(f"Working Capital: {get_net_working_capital(yahoo_financials, stock)}")
        print(f"{stock}'s Return on Capital: {get_roc(yahoo_financials, stock)}")
        print(f"{stock}'s Earnings Yield: {get_yield(yahoo_financials, stock)}")


    # print(yahoo_financials.get_financial_stmts('annual', 'balance'))

    end = time.time()

    print(f"Execution time: {end - start}")