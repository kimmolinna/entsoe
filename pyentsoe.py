from entsoe import EntsoePandasClient
from datetime import datetime
import pandas as pd
import awswrangler as wr
import keyring
from dateutil.relativedelta import relativedelta

client = EntsoePandasClient(api_key = str(keyring.get_password('entsoe','kimmo')))

start = pd.Timestamp(datetime.now().strftime("%Y%m")+"01", tz='Europe/Helsinki')
end = pd.Timestamp((datetime.now() + relativedelta(months=1)).strftime("%Y%m")+"01",tz='Europe/Helsinki')
country_code = 'FI'

ts=client.query_day_ahead_prices(country_code, start=start, end=end)
month = start.strftime("%Y%m")
df=pd.DataFrame({"timestamp":ts.index, "price": ts.values})
wr.s3.to_parquet(df, "s3://linna/entsoe/entsoe_"+ month +".parquet")