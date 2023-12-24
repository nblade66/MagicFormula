import unittest
import yfinance as yf
import json

def pretty_print(json_dict):
    print(json.dumps(json_dict, indent=2))

class TestYFinance(unittest.TestCase):
    def test_stock_info(self):
        """
            Test the yfinance stock info to make sure the dict keys still work and return the
            expected values.

            Tested keys are:
                'averageVolume10days'
                'marketCap'
                'currency'
                'ebitda'

            Specific to currency, make sure to include stock tickers that use foreign currencies in their financials.
        """
        amzn = yf.Ticker("amzn")
        pretty_print(amzn.info)
        self.assertEqual(amzn.info["currency"], "USD")
        self.assertEqual(amzn.info["averageVolume10days"], not None)
        self.assertEqual(amzn.info["marketCap"], not None)
        self.assertEqual(amzn.info["ebitda"], not None)
        self.assertEqual(amzn.info["randomNonexistentKey"], None)


if __name__ == '__main__':
    unittest.main()
