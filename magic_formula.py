from yahoofinancials import YahooFinancials
import pickle

refresh = False


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

if __name__ == '__main__':
    # Save the most balance sheets that are fetched, so that I don't need to fetch every time. Have the option to
    # refresh the balance sheets.
    stocks = ['AAPL', 'MVIS']
    if refresh:
        yahoo_financials = YahooFinancials(stocks)
        pickle.dump(yahoo_financials, open("yh_finance.p", "wb"))
    else:
        yahoo_financials = pickle.load(open("yh_finance.p", "rb"))
    for stock in stocks:
        get_net_working_capital(yahoo_financials, stock)
        # print(f"Fixed Assets: {get_fixed_assets(yahoo_financials, stock)}")
        # print(f"Working Capital: {get_net_working_capital(yahoo_financials, stock)}")
        print(f"{stock}'s Return on Capital: {get_roc(yahoo_financials, stock)}")

    # print(yahoo_financials.get_financial_stmts('annual', 'balance'))