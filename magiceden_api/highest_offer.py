import time
import pandas as pd
from magiceden_api import MagicParser

mp = MagicParser()

def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))


collection = 'degenerate_trash_pandas'
skip = 0
n_listings = 660

listings=[]
for n in range(0, n_listings, 20):
    batch = mp.get_listed_nfts('degenerate_trash_pandas', limit=20, skip=n)
    listings.extend(batch)
    time.sleep(2)

df_listings = pd.json_normalize(listings)
mint_addresses = df_listings['mintAddress'].to_list()

bids = []
for group in chunker(mint_addresses, 20):
    batch = mp.get_biddings(group)
    bids.extend(batch)
    time.sleep(2)

df_bids = pd.json_normalize(bids)
df_bids['bid'] = df_bids['bidderAmountLamports'].apply(lambda x: x/1000000000)

max_bid = df_bids.loc[df_bids['bid'] == df_bids['bid'].max()]
top_bids = df_bids.sort_values('bid', ascending=False).head(10)

mp._close_driver()