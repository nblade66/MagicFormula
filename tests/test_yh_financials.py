import unittest
from yahoofinancials import YahooFinancials


class TestYHFinancials(unittest.TestCase):
    def test_get_stock_price_data(self):
        """
            Test the YahooFinancials method get_stock_price_data to make sure the dict keys still work and return the
            expected values.

            Tested keys are:
                'regularMarketPrice'
                'regularMarketVolume'
                'averageDailyVolume10Day'
                'marketCap'
                'currency'

            Specific to currency, make sure to include stock tickers that use foreign currencies in their financials.
        """
        yh = YahooFinancials(['AMZN'])#, 'TSM', 'TSLA', 'MVIS', 'BWMX', 'XPEV'])  # A mix of US and foreign companies
        print(yh.get_current_price())
        print("Getting stock price data")
        price_data = yh.get_stock_price_data()
        print(price_data)
        self.assertEqual(True, False)


if __name__ == '__main__':
    unittest.main()
