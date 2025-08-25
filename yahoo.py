from yfinance import EquityQuery
# q = EquityQuery('and', [
#        EquityQuery('eq', ['region', 'us']),
#        EquityQuery('lt', ['intradaymarketcap', 20000000000]),
#        EquityQuery('gt', ['intradaymarketcap', 1000000000]),

#        EquityQuery('or',[
#                    EquityQuery('eq', ['sector','Healthcare']),
#                    EquityQuery('eq', ['sector', 'Technology'])])
# ])
# response = yf.screen(q,offset=1000)
# print(len(response['quotes']))

# print([i['symbol'] for i in response['quotes']])


# with open('target_stocks.csv', 'w') as f:
#     f.write('\n'.join(a))

import yfinance as yf

def page_through_screen(query,
                        sort_field="ticker",
                        sort_ascending=False,
                        max_per_call=250):
    """
    Generator that yields one page of tickers at a time.
    Keeps requesting pages until Yahoo stops returning data.
    """

    # Decide which parameter Yahoo uses for this query type
    use_count = isinstance(query, str) and query in yf.PREDEFINED_SCREENER_QUERIES
    param_name = "count" if use_count else "size"

    offset = 0
    while True:
        kwargs = {
            "query": query,
            "offset": offset,
            "sortField": sort_field,
            "sortAsc": sort_ascending,
            param_name: max_per_call,
        }

        page = yf.screen(**kwargs)
        tickers = page["quotes"]        # list of dicts
        if not tickers:                 # Yahoo returned nothing → we are done
            break

        yield tickers                   # hand this page to the caller
        offset += len(tickers)          # advance cursor

        # Safety valve – remove if you really want to walk 10k+ results
        if offset >= 10_000:
            break


# ------------------------------------------------------------------
# Example usage
# ------------------------------------------------------------------

# 1) Pre-defined query -------------------------------------------------
query_name = EquityQuery('and', [
       EquityQuery('eq', ['region', 'us']),
       EquityQuery('lt', ['intradaymarketcap', 20000000000]),
       EquityQuery('gt', ['intradaymarketcap', 1000000]),

       EquityQuery('or',[
                   EquityQuery('eq', ['sector','Healthcare']),
                   EquityQuery('eq', ['sector', 'Technology'])])
])
a = ['ticker']
for batch in page_through_screen(query_name, max_per_call=250):
    symbols = [t["symbol"] for t in batch]
    a.extend(symbols)
    print("Got", len(symbols), "tickers:", symbols[:10], "...")
with open('target_stocks.csv', 'w') as f:
    f.write('\n'.join(a))
