import datetime as dt
import requests
import pandas as pd

datainicial = '11-24-2020'
datafinal  = str(dt.datetime.today().month) + '-' + str(dt.datetime.today().day) + '-' + str(dt.datetime.today().year)
diff = dt.datetime.strptime(datafinal, '%m-%d-%Y') - dt.datetime.strptime(datainicial, '%m-%d-%Y')
moeda = 'USD'
url = "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoMoedaPeriodo(moeda=@moeda,dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)?@moeda='{3}'&@dataInicial='{0}'&@dataFinalCotacao='{1}'&$top={2}&$format=json".format(datainicial,datafinal,diff.days *5,moeda)

df = pd.read_json(url)

cotacoes_dolar = pd.DataFrame(columns=['cotacao','dia'])

counter = 0
for j in df['value']:
    if j['tipoBoletim'] == 'Fechamento':
        cotacoes_dolar.loc[counter] = [j['cotacaoVenda'],j['dataHoraCotacao']]
        counter +=1

'''
for i in df['value']:
    if i['tipoBoletim'] == 'Fechamento':
        print(i['cotacaoVenda'],i['dataHoraCotacao'])
'''
    
moeda = 'EUR'
url = "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoMoedaPeriodo(moeda=@moeda,dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)?@moeda='{3}'&@dataInicial='{0}'&@dataFinalCotacao='{1}'&$top={2}&$format=json".format(datainicial,datafinal,diff.days *5,moeda)

df = pd.read_json(url)

cotacoes_euro = pd.DataFrame(columns=['cotacao','dia'])
cotacoes_euro.loc[0] = [1,2]

counter = 0
for j in df['value']:
    if j['tipoBoletim'] == 'Fechamento':
        cotacoes_euro.loc[counter] = [j['cotacaoVenda'],j['dataHoraCotacao']]
        counter+=1

'''
for i in df['value']:
    if i['tipoBoletim'] == 'Fechamento':
        print(i['cotacaoVenda'],i['dataHoraCotacao'])
'''

cotacoes = pd.concat([cotacoes_dolar,cotacoes_euro],ignore_index=False,keys=['dol','eur'],names=['moeda','index']).reset_index()

import datetime as dt
try:
    cotacoes['dia'] = cotacoes['dia'].apply(lambda x: dt.datetime.fromisoformat(x))
    cotacoes['dia'] = cotacoes['dia'].apply(lambda x: x.strftime('%d-%m-%y'))
    cotacoes['cotacao'] = cotacoes['cotacao'].astype(str).str.replace('.',',')
except:
    pass
#cotacoes[['moeda','cotacao','dia']].to_clipboard(index=False,sep='\t',float_format='{0:1.3f}'.format)

import os
os.system("start EXCEL.EXE")
