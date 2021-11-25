# Cash Flow Simulation for Power BI Visual
# O código a seguir para criar um dataframe e remover as linhas duplicadas sempre é executado e age como um preâmbulo para o script: 

# dataset = pandas.DataFrame(Date, Faturamento, RECEB_PREV, DESP_PREV, DESP_PREV_FORN, DESP_PREV_DESEMB, DESP_JAN, SALDO PREV)
# dataset = dataset.drop_duplicates()

# Cole ou digite aqui seu código de script:

import matplotlib.pyplot as plt
import datetime
import matplotlib.ticker as ticker

if dataset['SALDO_SIM'][0] > 0:
    saldo_inicial = dataset['SALDO_SIM'][0]
elif datetime.datetime.strptime(dataset['Date'][0],'%Y-%m-%dT00:00:00.0000000') < datetime.datetime.today():
    saldo_inicial = dataset['SALDO PREV'][0] - dataset['MOV_BAIXA_R'].fillna(0)[0] - dataset['MOV_BAIXA_P'].fillna(0)[0]
else:
    saldo_inicial = dataset['SALDO PREV'][0] - dataset['DESP_PREV'].fillna(0)[0] - dataset['SALDO_CAP_FORN'].fillna(0)[0] - dataset['SALDO_CAR'].fillna(0)[0] - dataset['RECEB_PREV'].fillna(0)[0]

prazo_rec = pandas.DataFrame({'Dias': [0,30,60,90,120,150], '% Receb':[0,0.3, 0.3, 0.3,0.05, 0.05]})

dataset_receb = dataset[['DISTR FAT', 'FAT_SIM_REPET','Date']]
dataset_receb['Fat Dia'] = dataset_receb['DISTR FAT'] * dataset_receb['FAT_SIM_REPET']
dataset_receb['Date'] = pandas.to_datetime(dataset_receb['Date'])
dataset_receb.index= dataset_receb['Date']
dataset_receb = dataset_receb.drop(columns='Date')
dataset_receb = dataset_receb[['Fat Dia']]
#dataset = dataset.resample('M').sum()
dataset_receb['key'] = 0
prazo_rec['key'] = 0
dataset_receb = dataset_receb.reset_index().merge(prazo_rec,on='key',how='outer')
dataset_receb['Valor Receb'] = dataset_receb['% Receb'] * dataset_receb['Fat Dia']
dataset_receb['Dias a mais'] = pandas.to_timedelta(dataset_receb['Dias'],'d')
dataset_receb['Data Receb']= dataset_receb['Date'] + dataset_receb['Dias a mais']
dataset_receb = dataset_receb[['Data Receb', 'Valor Receb']]
dataset_receb = dataset_receb.set_index('Data Receb')
dataset_receb = dataset_receb.resample('d').sum()
#dataset_receb['Ano_Mes'] = pandas.to_datetime(dataset_receb['Data Receb']).dt.to_period('M')
#dataset_receb = dataset_receb.groupby('Ano_Mes').sum()

#
dataset.index = pandas.to_datetime(dataset['Date'])
dataset_comb = dataset.join(dataset_receb)

dataset_comb['SALDO SIM'] = saldo_inicial + dataset_comb['DESP_SIM_UNICO'].fillna(0).cumsum() + dataset_comb['DESP_OUTROS_SIM_UNICO'].fillna(0).cumsum() + (dataset_comb['DESP_PREV_SEM_FORN'].fillna(0).cumsum() - dataset_comb['DESP_PREV_OUTROS'].fillna(0).cumsum()) + dataset_comb['Valor Receb'].fillna(0).cumsum() + dataset_comb['SALDO_CAR'].fillna(0).cumsum()
#dataset_comb ['Date'] = pandas.to_datetime(dataset['Date'])
dataset_comb['DEMAIS DESP']  = (dataset_comb['DESP_PREV_SEM_FORN'].fillna(0) - dataset_comb['DESP_PREV_OUTROS'].fillna(0))

dataset_comb.columns
dataset_saldo = dataset_comb[['SALDO SIM','DESP_SIM_UNICO','DESP_OUTROS_SIM_UNICO','DEMAIS DESP','SALDO_CAR','Valor Receb']]
#dataset_saldo['SALDO_CAR'] = dataset_saldo['SALDO_CAR'].fillna(0)
dataset_saldo = dataset_saldo.fillna(0)

import matplotlib.ticker as ticker
import numpy as np
fig, (ax_1, ax_2) = plt.subplots(2,1,figsize=(25,15))
ax_1.plot(dataset_saldo['SALDO SIM'],label='Saldo')
ax_1.bar(height=dataset_saldo['SALDO_CAR'],x=dataset_saldo.index, color='r', label='Carteira a Receber')
ax_1.tick_params(axis='x',rotation=45)
ax_1.tick_params(axis='y' )
scale_y = 1e6
ticks_y = ticker.FuncFormatter(lambda x, pos: '{:.1f}'.format(x/scale_y))
ax_1.yaxis.set_major_formatter(ticks_y)
ax_1.set_yticks([0, 500000, 1000000,1500000,2000000,2500000])
ax_1.grid(linestyle='-')
ax_1.legend(loc='upper left',fontsize=20)

df = dataset_saldo #['SALDO SIM']
data_inicial = df.index.min() - datetime.timedelta(days=30)
df = df.append(pandas.DataFrame({'SALDO SIM': saldo_inicial}, index=[data_inicial]))
df = df.resample('M').agg({'SALDO SIM': lambda x: x.iloc[-1],'DESP_SIM_UNICO': np.sum,'DESP_OUTROS_SIM_UNICO': np.sum,'DEMAIS DESP': np.sum,'SALDO_CAR': np.sum, 'Valor Receb': np.sum, })

#df['SALDO SIM'] = df['SALDO SIM'].apply(lambda x: '{:.1f}'.format(x/1000))
#df['SALDO_CAR'] = df['SALDO_CAR'].apply(lambda x: '{:.1f}'.format(x/1000))
for i in df.columns:
    df[i] = df[i].apply(lambda x: '{:,.1f}'.format(x/1000))
df.index = df.index.strftime('%Y-%m')
#df.transpose().reset_index()
colunas =  ['Saldo','Despesas Forn', 'Despesas Outros', 'Despesas', 'Cart. a Receber', 'Prev. a Receber']
linha = df.transpose().columns.min()
linhas = pandas.Series(df.transpose().columns.astype(str)).replace({linha: 'Saldo inicial'}).values
the_table = ax_2.table(cellText=df.transpose().values,colLabels=linhas,rowLabels=colunas,loc='center')
#the_table = ax_2.table(cellText=df.transpose().values,colLabels=df.transpose().columns,rowLabels=colunas,loc='center')
the_table.auto_set_font_size(False)
the_table.set_fontsize(19)
#ax_2.table(cellText=df.reset_index().values,loc='center')
ax_2.get_xaxis().set_visible(False)
ax_2.get_yaxis().set_visible(False)
the_table.scale(1, 4)
plt.tight_layout()
plt.show()
