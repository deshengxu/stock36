# stock36

1) download stock price history from google

    python stocks/stockticker.py -h

*-market: select market, currently support NASDAQ, NYSE, AMEX

*-root: (optional) root folder to store downloaded price history files

*-max: max stocks to download

*-sector: filter by sector, for example: "Health Care". Multiple sectors with comma seperated.

*-industry: filter by industry

*-additional: filter by additional column in table.

    python stocks/stockticker.py -market NASDAQ -max 20 -sector "Health Care,Finance"

2) download description of stock and analysis keywords

    keywords in defined in ./basedata/keywords.csv

    description is retrived from finance.google.com; Finance.yahoo.com seems like has problem with some stock.

    python stocks/stockdesc.py -h

    parameters are as same as stockticker.py

        python stocks/stockdesc.py -market AMEX -max 20