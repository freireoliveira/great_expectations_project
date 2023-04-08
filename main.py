#%%
import pandas as pd
from datetime import datetime as dt
from ydata_profiling import ProfileReport

table = pd.read_csv('Walmart.csv')
table

#%%
raw = table.copy()
raw.to_parquet('dados_brutos/raw.parquet')

#%%
structured = raw.copy()
structured.info()
#%% ausentes
structured.isna().sum()
#%%
duplicateRows = structured[structured.duplicated(keep='last')]
print(duplicateRows)
#%%
structured.dtypes

# %%
structured['Order Date'] = [dt.strptime(x, '%d-%m-%Y') for x in structured['Order Date']]
structured['Ship Date'] = [dt.strptime(x, '%d-%m-%Y') for x in structured['Ship Date']]
structured['Quantity'] = structured['Quantity'].astype('int')

#%%
structured.dtypes

#%%
structured.to_parquet('dados_estruturados/structured.parquet')

#%%
specialized = structured.copy()
marketing = specialized[['Order Date','Country','City','State','Category','Quantity']]
sales = specialized[['Order Date','Category','Sales','Quantity','Profit']]

#%%
categories = list(set(structured['Category']))
print(categories)
for category in categories:
    sales.loc[sales['Category'] == f'{category}'].drop(['Category'], axis=1)\
        .to_parquet(f'dados_especializados/sales/{category.lower()}.parquet')

profitable_categories = sales.groupby(by='Category')['Profit'].sum()
sales_categories = sales.groupby(by='Category')['Sales'].sum()
quantity_categories = sales.groupby(by='Category')['Quantity'].sum()

category_overview = pd.DataFrame(data=[profitable_categories.to_list(), 
                                           sales_categories.to_list(), 
                                           quantity_categories.to_list()],
                                index=['Profit', 'Sales', 'Qantity'],
                                columns=categories).T
category_overview.to_parquet('dados_especializados/sales/overview.parquet')
category_overview

#%%
states = list(set(structured['State']))

quantity_per_state = marketing.groupby(by='State')['Quantity'].sum()
max_date = marketing.groupby(by='State')['Order Date'].apply(list).max()
min_date = marketing.groupby(by='State')['Order Date'].apply(list).min()
deltas = [(i-j).days for i,j in zip(max_date,min_date)]
quantity_per_state_time = [i/j for i,j in zip(quantity_per_state, deltas)]
state_overview = pd.DataFrame(data=quantity_per_state_time,
                              index=states,
                              columns=['Quantity'])
state_overview.to_parquet('dados_especializados/marketing/overview.parquet')
state_overview

