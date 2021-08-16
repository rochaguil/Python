fat_dir = pd.read_sql(query_fat_dir,conn)

fat_dir['DATES'] = pd.to_datetime(fat_dir['DATES'], format='%d/%m/%Y')

fat_dir  = fat_dir[['DATES', 'B1_COD','B1_DESC','QUANT']]
produtos = ['A','B']
from statsmodels.tsa.seasonal import seasonal_decompose
import matplotlib.pyplot as plt

fig, axes = plt.subplots(len(produtos),3,figsize=(12,6))
fig.tight_layout()
for i, prod in enumerate(produtos):
    fat_dir_prod = fat_dir[fat_dir['B1_COD'].str.contains(prod)]
    fat_dir_prod = fat_dir_prod .set_index('DATES')
    fat_dir_prod.index.min()
    fat_dir_prod.index.max()
    new_index = (pd.date_range(fat_dir_prod.index.min(), fat_dir_prod.index.max()))
    fat_dir_prod = fat_dir_prod.resample('D').sum()
    fat_dir_prod = fat_dir_prod.reindex(new_index,fill_value=0)
    fat_dir_prod = fat_dir_prod .resample('M').sum()
    ss_decomposition = seasonal_decompose(fat_dir_prod, model='additive',freq=12,)
    axes[0,i].plot(fat_dir_prod)
    axes[0,i].set_title('Original')
    ss_decomposition.trend.plot(ax=axes[1,i])
    axes[1,i].set_title('Trend')
    ss_decomposition.seasonal.plot(ax=axes[2,i])
    axes[2,i].set_title('Seasonality')

