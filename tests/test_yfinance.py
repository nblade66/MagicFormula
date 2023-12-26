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
                'ebitda'

            Specific to currency, make sure to include stock tickers that use foreign currencies in their financials.
        """
        amzn = yf.Ticker("amzn")
        amzn_avg_volume = amzn.info["averageVolume10days"]
        amzn_market_cap = amzn.info["marketCap"]
        amzn_ebitda = amzn.info["ebitda"]
        pretty_print(amzn.info)
        self.assertEqual(amzn.info["financialCurrency"], "USD")
        self.assertIsNotNone(amzn_avg_volume)
        self.assertIsNotNone(amzn_market_cap)
        self.assertIsNotNone(amzn_ebitda)
        with self.assertRaises(KeyError):
            amzn.info["randomNonexistentKey"]



    def test_currency(self):
        """ Test that correct currencies are retrieved from Yahoo Finance.
        """
        tickers = yf.Tickers("xpev bwmx tsm")

        tsm = tickers.tickers["TSM"]
        tsm_avg_volume = tsm.info["averageVolume10days"]
        tsm_market_cap = tsm.info["marketCap"]
        tsm_ebitda = tsm.info["ebitda"]
        pretty_print(tsm.info)
        self.assertEqual(tsm.info["financialCurrency"], "TWD")
        self.assertIsNotNone(tsm_avg_volume)
        self.assertIsNotNone(tsm_market_cap)
        self.assertIsNotNone(tsm_ebitda)

        xpev = tickers.tickers["XPEV"]
        xpev_avg_volume = xpev.info["averageVolume10days"]
        xpev_market_cap = xpev.info["marketCap"]
        xpev_ebitda = xpev.info["ebitda"]
        pretty_print(xpev.info)
        self.assertEqual(xpev.info["financialCurrency"], "CNY")
        self.assertIsNotNone(xpev_avg_volume)
        self.assertIsNotNone(xpev_market_cap)
        self.assertIsNotNone(xpev_ebitda)


        bwmx = tickers.tickers["BWMX"]
        bwmx_avg_volume = bwmx.info["averageVolume10days"]
        bwmx_market_cap = bwmx.info["marketCap"]
        bwmx_ebitda = bwmx.info["ebitda"]
        pretty_print(bwmx.info)
        self.assertEqual(bwmx.info["financialCurrency"], "MXN")
        self.assertIsNotNone(bwmx_avg_volume)
        self.assertIsNotNone(bwmx_market_cap)
        self.assertIsNotNone(bwmx_ebitda)


if __name__ == '__main__':
    unittest.main()
