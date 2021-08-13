# 'dataset' tem os dados de entrada para este script

import pandas as pd
import re

tab = pd.DataFrame(columns=['PO','day','time','fup','status'])


for PO in dataset['N° PO']:
    try:
        if ',' in PO:
            sub_PO = PO.split(',')
        elif '/' in PO:
            sub_PO = PO.split('/')
        else:
            sub_PO = [PO.strip()]
    except:
        pass
    for sub in sub_PO:
        try:
            texto = dataset[dataset['N° PO'] == PO]['HISTÓRICO'].values[0]
            a = pd.Series(re.findall(r'([\d\/]*\d{4}) \d{2}:\d{2} ', texto))
            b = pd.Series(re.findall(r'[\d\/]*\d{4} (\d{2}:\d{2}) ',texto))
            c = pd.Series(re.split(r'[\d\/]*\d{4} \d{2}:\d{2} - ', texto)[1:])
            d = [PO] * len(c)
            status = dataset[dataset['N° PO'] == PO]['STATUS DO FOLLOWUP'].values[0]
            status = [status] * len(c)
            e = pd.DataFrame({'PO': d, 'day': a, 'time': b, 'fup':c, 'status':status,'PO_ref':sub.strip()})
            tab = pd.concat([tab,e])
        except:
            pass
