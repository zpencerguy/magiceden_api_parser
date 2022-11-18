import time
from datetime import datetime
import pandas as pd
from magiceden_api import MagicParser

collections = [
    # 'degenerate_trash_pandas',
    'aurory'
    ,'bat_city_underground'
    ,'cets_on_creck'
    ,'degenerate_ape_academy'
    ,'degods'
    ,'famous_fox_federation'
    ,'galactic_geckos'
    ,'grim_syndicate'
    ,'meerkat_millionaires_country_club'
    ,'mrn_s1_vikings_premium_drop'
    ,'okay_bears'
    ,'pesky_penguins'
    ,'photo_finish'
    ,'solana_monkey_business'
    ,'solsteads_surreal_estate'
    # ,'Star Atlas'
    ,'stylish_studs'
    ,'the_suites'
    ,'taiyo_robotics'
    ,'thugbirdz'
    ,'turtles'
]


def convert_ts(ts):
    ts = int(str(ts)[:10])
    return datetime.utcfromtimestamp(ts)


mp = MagicParser()
collection = 'degenerate_trash_pandas'
dfs = []
for collection in collections:
    print(collection)
    data = mp.get_collection_stats(collection, '10m')
    print(len(data))
    df = pd.json_normalize(data)
    df['symbol'] = collection
    df.to_csv(f'/Users/spencerguy/projects/magiceden_api_parser/examples/{collection}_10m_fp.csv')
    dfs.append(df)
    time.sleep(7)

chunk2.to_sql(
    'MarketplaceFloorPrices',
    engine,
    index=False,
    if_exists="append",
    schema="dbo",
    chunksize=500,
    method='multi'
)