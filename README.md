# MagicFormula

A program to implement the Magic Formula from the book "The Little Book that Still Beats the Market" by Joel Greenblatt. The book is pretty short, and it's actually a really fun read, so go check it out if you want the exact details about his "Magic Formula". I know the list of stocks are already available on his website https://magicformulainvesting.com, but the top stocks are listed in alphabetical order. I wanted to know the exact rankings, so here we are.

Stock data is retrieved using the YahooFinancials library (unofficial, so it uses web scraping).

Lists of stocks were found on http://ftp.nasdaqtrader.com/, in Symbol Directory. I used `nasdaqlisted.txt` and `otherlisted.txt`.

It then calculates Return on Capital and Earnings Yield, as specified by Joel Greenblatt. Some of the exact metrics he used were not available on Yahoo Finance, so I had to do some Googling to figure out what the equivalent metrics were (for example, I found that Short Term Debt is also known as Current Liabilities). He also does not reveal some of his exact calculation methods, such as for Net Working Capital, so I did my best based on this [site](https://www.businessinsider.com/magic-formula-investing-amp-the-little-book-that-beats-the-markets-greenblatts-roc-amp-earnings-yield-approach-2011-4). If things are wrong, let me know.
Stocks below a certain market cap are then filtered out (as he suggests to do), and ranked based on Return on Capital and Earnings Yield.

Balance Sheet metrics use the most recent Quarter's values. Income statement metrics use the TTM, based on Quarterly data.
Stocks are also filtered out for Market Cap and Average Dollar Volume (although the calculation is rough, since it only uses the current share price)

## Getting started

Here is how to download the source code, install all dependencies, and run the program:

```
git clone https://github.com/nblade66/MagicFormula.git
cd MagicFormula
pip install -r requirements.txt
python magic_formula.py
```

## Configuration

There are six flags to be aware of:

* `-r`    Retrieves the data (balance_sheet, income_statement, market_cap) of all the stocks listed in the "ticker_list" list. Uses 10 threads. This will take a really long time, since it uses web scraping. As such, I implement retrieval in batches, which are then saved to the JSON file. If no flag, then data will load from the JSON file
* `-t`    Retrieves the ticker_list using the files "nasdaqlisted.txt" and "otherlisted.txt"; if flag isn't used, ticker_list will load from the JSON file
* `-c`    Continues retrieving the data of the stocks that aren't in the JSON files, but are in the ticker_list. Generally used if for some reason retrieval was interrupted.
* `-mc`   Allows for multiprocessing (or multi-core) to fetch data from Yahoo Finance web scraping faster. Only applies to continued retrieval. An integer specifies how many processes should be run
* `-s`    Retrieves the sector info (sector, industry, and country) of all stocks in ticker_list and saves it to the stock_info.json file. Takes a single integer argument that specifies the desired batch size. If no integer is given, a single batch is used
* `--validate` Validates the tickers based on Market Cap and Average Dollar Volume (and also updates Market Cap Data on valid tickers)

### How to Use

Anytime you run `python magicformula.py`, a CSV file with magic formula ranks will be generated, regardless of flags.
Of course, if you run it without retrieving new data, the ranks generated will also be the same.

On the first time running, usually run `python magicformula.py -t -r` to refresh the ticker list, then retrieve all the data. This will also remove tickers with missing data from the ticker list, and validate tickers (set tickers that don't meet the market cap and dollar average volume criteria to "invalid").
You should rarely use the `-t` flag after this because it will cause run time to take unnecessarily long.

On subsequent runs, `python magicformula.py -r` will retrieve updated values for only valid tickers. This is to speed up retrieval.
Because it's possible for average dollar cost volumes and market caps to change, use `python magicformula.py --validate -r` to also recheck if tickers are valid (and thus, potentially retrieve data from newly valid tickers).

Things to be aware of:

* The `ticker_list` used to debug can just be any Python list; when actually running the code, make sure to use the `-t` flag to get a refreshed list of tickers.
* Running on ~10,000 could take 6+ hours the first time, as it filters through stocks

Some things I'm working on:

* Making the code more modular, so that the functions can run on their own without breaking. This is pretty low priority, though.
* Increasing speed of data fetching
* Updating Market Caps using Threading
