
venc = pd.read_sql(query_validade_ent, conn)
venc['D1_DTDIGIT'] = venc['D1_DTDIGIT'].apply(lambda x: datetime.datetime.strptime(x, '%d/%m/%Y'))
venc.set_index('D1_DTDIGIT',inplace=True)
venc_prod = venc.groupby('B1_COD').mean()['VALIDADE_MESES']
venc_prod.rename('MÉDIA VALIDADE ENTRADA',inplace=True)

#início P3
try:
    estoque_p3_saldo = pd.DataFrame((tuple(t) for t in estoque_p3),columns=colunas)
    estoque_p3_saldo['SALDO_FINAL'] = estoque_p3_saldo['SALDO'] - estoque_p3_saldo['Z05_QUANT']
except:
    cur.execute(query_p3)
    cur.nextset()
    cur.nextset()
    estoque_p3  = cur.fetchall()
    colunas = [column[0] for column in cur.description]
    estoque_p3_saldo = pd.DataFrame((tuple(t) for t in estoque_p3),columns=colunas)
    estoque_p3_saldo['SALDO_FINAL'] = estoque_p3_saldo['SALDO'] - estoque_p3_saldo['Z05_QUANT']
    
estoque_p3_saldo_linha = estoque_p3_saldo[estoque_p3_saldo['Linha'] == linha]

estoque_p3_saldo_prod_linha  = estoque_p3_saldo_linha[['Produto','SALDO_FINAL']].groupby('Produto').sum('SALDO_FINAL').fillna(0)
estoque_p3_saldo_prod_linha  = estoque_p3_saldo_prod_linha.rename(columns={'SALDO_FINAL': 'P3'})
#fim P3

#
produtos = pd.read_sql(query_produtos, conn)
pedidos = pd.read_sql(query_pedidos, conn)
pedidos_em_aberto = pd.read_sql(query_pedidos_aberto, conn)
estoque = pd.read_sql(query_estoque,conn)
cmv = pd.read_sql(query_cmv, conn)
ult_custo = pd.read_sql(query_ult_custo, conn)

try:
    ult_preco
except:
    ult_preco = pd.read_sql(query_ult_preco, conn)
ult_preco  [ ult_preco ['B1_COD'].str.contains( '1026')] 

ult_preco_prod = ult_preco[ult_preco['CTD_DESC01'].str.contains(linha)].groupby('B1_COD').first('C7_PRECO')['C7_PRECO']
ult_preco_prod = ult_preco_prod.rename('ÚLTIMO PREÇO')



#tempo_fat = pd.read_sql(query_tempo_fat, conn)

produtos.index = produtos['B1_COD']

pedidos = pd.pivot_table(pedidos,columns=['PO', 'C7_EMISSAO'], index='B1_COD',values='C7_QUANT',aggfunc=sum,fill_value=0)
pedidos = pedidos.sort_values('C7_EMISSAO',axis=1)
pedidos_total = pedidos.sum(axis=1)
pedidos_total = pedidos_total.rename("total pedidos")

pedidos_em_aberto = pd.pivot_table(pedidos_em_aberto, columns=['PO','C7_EMISSAO'], index='B1_COD', values='C7_QUANT',aggfunc=sum,fill_value=0)
try:
    pedidos_em_aberto = pedidos_em_aberto.sort_values('C7_EMISSAO',axis=1)
except:
    pass
    #pd.DataFrame(columns={'B1_COD': ['1']}),index={'B1_COD': [0]})

def tempo_validade(x):
    hoje = datetime.datetime(year=datetime.datetime.today().year, month=datetime.datetime.today().month, day=datetime.datetime.today().day)
    status = ''
    if x < hoje:
        status = 'VENCIDO'
    elif x <= hoje + datetime.timedelta(days=30):
        status = '< 30 DIAS'
    elif x <= hoje + datetime.timedelta(days=60):
        status = '30 < 60 DIAS'
    elif x <= hoje + datetime.timedelta(days=90):
        status = '60 < 90 DIAS'    
    elif x <= hoje + datetime.timedelta(days=180):
        status = '90 < 180 DIAS'        
    else:
        status = '> 180 DIAS'
    return status

estoque_prod_valid = estoque[['B1_COD', 'QUANT','B8_DTVALID']]
estoque_prod_valid['B8_DTVALID'] = pd.to_datetime(estoque_prod_valid['B8_DTVALID'],format='%d/%m/%Y')
ordem_val = ['VENCIDO','< 30 DIAS', '30 < 60 DIAS','60 < 90 DIAS','90 < 180 DIAS','> 180 DIAS']
ordem_range = range(0, len(ordem_val))
dict(zip(ordem_val,ordem_range))
estoque_prod_valid['VALIDADE'] = estoque_prod_valid['B8_DTVALID'].apply(lambda x: tempo_validade(x))
estoque_prod_valid['sorter'] = estoque_prod_valid['VALIDADE'].map(dict(zip(ordem_val,ordem_range)))
estoque_prod_valid = estoque_prod_valid.pivot_table(index='B1_COD',columns = ['sorter','VALIDADE'],values='QUANT',aggfunc='sum').fillna(0)
estoque_prod_valid.columns = estoque_prod_valid.columns.droplevel()
estoque_prod_valid['estoque_total'] = estoque_prod_valid.sum(axis=1)
#estoque_prod_valid['VALIDADE'] = estoque_prod_valid['B8_DTVALID'].apply(lambda x: x.to_period('M'))

#estoque = estoque[['B1_COD', 'QUANT']].groupby('B1_COD').sum()
#estoque = estoque.rename(columns={'QUANT': 'ESTOQUE'})

procedimentos = pd.read_sql(query_procedimentos,conn)
procedimentos['DTPROC'] = procedimentos['DTPROC'].str.replace('3021', '2021')
#procedimentos['DTPROC']
procedimentos['DTPROC'] = pd.to_datetime(procedimentos['DTPROC'],format='%d/%m/%Y')
#pd.DataFrame({'a':procedimentos['DTPROC'], 'b': pd.to_datetime(procedimentos['DTPROC'],format='%d/%m/%Y')}).to_clipboard()
procedimentos = procedimentos[procedimentos['DTPROC'] <= datetime.datetime.now()]
#procedimentos['DTPROC'].sort_values().min()
#procedimentos['DTPROC'].dt.strftime('%d-%m-%Y').sort_values().min()

inicio = min(procedimentos['DTPROC']).strftime('%m-%d-%Y')
fim = max(procedimentos['DTPROC']).strftime('%m-%d-%Y')
date_range_proc = pd.Series(pd.date_range(start=inicio, end=fim,freq='M').rename('DTPROC'))
date_range_proc = pd.DataFrame({'DTPROC': date_range_proc})
date_range_proc['Z05_QUANT'] = [0] * len(date_range_proc)

procedimentos = procedimentos[procedimentos.columns[~procedimentos.columns.isin(['D_E_L_E_T_','R_E_C_N_O_','R_E_C_D_E_L_'])]]
procedimentos = pd.concat([procedimentos,date_range_proc]).fillna(0)

procedimentos.index = pd.to_datetime(procedimentos['DTPROC'],format='%d/%m/%Y')


procedimentos_prod = procedimentos.groupby([pd.Grouper(freq='M'), procedimentos['B1_COD'], procedimentos['B1_DESC']]).sum('Z05_QUANT')
procedimentos_prod = procedimentos_prod[['Z05_QUANT']]
procedimentos_prod = procedimentos_prod.reset_index().pivot_table(columns='DTPROC',index='B1_COD',values='Z05_QUANT').fillna(0).iloc[:,:-1].mean(axis=1)
procedimentos_prod  = procedimentos_prod.rename(index='PROCEDIMENTOS')

procedimentos_prod_m6 = procedimentos.groupby([pd.Grouper(freq='M'), procedimentos['B1_COD'], procedimentos['B1_DESC']]).sum('Z05_QUANT')
procedimentos_prod_m6 = procedimentos_prod_m6[['Z05_QUANT']]
procedimentos_prod_m6 = procedimentos_prod_m6.reset_index().pivot_table(columns='DTPROC',index='B1_COD',values='Z05_QUANT').fillna(0)
nrows, ncols = procedimentos_prod_m6.shape
procedimentos_prod_m6 = procedimentos_prod_m6.iloc[:,ncols-7:ncols-1].mean(axis=1)
procedimentos_prod_m6 = procedimentos_prod_m6.rename(index='PROCEDIMENTOS M6')

procedimentos_prod_m6_dp = procedimentos.groupby([pd.Grouper(freq='M'), procedimentos['B1_COD'], procedimentos['B1_DESC']]).sum('Z05_QUANT')
procedimentos_prod_m6_dp = procedimentos_prod_m6_dp[['Z05_QUANT']]
procedimentos_prod_m6_dp = procedimentos_prod_m6_dp.reset_index().pivot_table(columns='DTPROC',index='B1_COD',values='Z05_QUANT').fillna(0)
nrows, ncols = procedimentos_prod_m6_dp.shape
procedimentos_prod_m6_dp = procedimentos_prod_m6_dp.iloc[:,ncols-7:ncols-1].std(axis=1)
procedimentos_prod_m6_dp = procedimentos_prod_m6_dp.rename(index='PROCEDIMENTOS M6 dp')



procedimentos_prod_m12_6 = procedimentos.groupby([pd.Grouper(freq='M'), procedimentos['B1_COD'], procedimentos['B1_DESC']]).sum('Z05_QUANT')
procedimentos_prod_m12_6 = procedimentos_prod_m12_6[['Z05_QUANT']]
procedimentos_prod_m12_6 = procedimentos_prod_m12_6.reset_index().pivot_table(columns='DTPROC',index='B1_COD',values='Z05_QUANT').fillna(0)
nrows, ncols = procedimentos_prod_m12_6.shape
procedimentos_prod_m12_6 = procedimentos_prod_m12_6.iloc[:,ncols-13:ncols-7].mean(axis=1)
procedimentos_prod_m12_6 = procedimentos_prod_m12_6.rename(index='PROCEDIMENTOS M12 - 6')

procedimentos_cliente = procedimentos[['B1_COD','B1_DESC','Z05_CODCLI','Z05_LOJA','A1_NOME', 'A1_NREDUZ', 'A1_EST','Z05_QUANT']]
procedimentos_cliente.index = procedimentos_cliente.index.to_period('M')
procedimentos_cliente['Z05_QUANT']
procedimentos_cliente = procedimentos_cliente.reset_index().groupby(['DTPROC','Z05_CODCLI','Z05_LOJA','A1_NOME', 'A1_NREDUZ', 'A1_EST','B1_COD','B1_DESC']).sum('Z05_QUANT').reset_index()
procedimentos_cliente = procedimentos_cliente.pivot_table(values='Z05_QUANT', index = ['B1_COD','B1_DESC','Z05_CODCLI','Z05_LOJA','A1_NOME', 'A1_NREDUZ', 'A1_EST'], columns='DTPROC').fillna(0)
procedimentos_cliente = procedimentos_cliente[1:]

#procedimentos_cliente.to_clipboard(decimal=',')


procedimentos_cliente_nf = procedimentos[['Z05_CODCLI','Z05_LOJA','A1_NOME', 'A1_NREDUZ', 'A1_EST','Z05_DOC','Z05_QUANT']]

procedimentos_cliente_nf.index = procedimentos_cliente_nf.index.to_period('M')

inicio = min(procedimentos_cliente_nf.index).strftime('%m-%d-%Y')
fim = max(procedimentos_cliente_nf.index).strftime('%m-%d-%Y')
date_range = pd.Series(pd.date_range(start=inicio,end=fim,freq='M')).rename('DTPROC')
date_range.index = date_range
date_range.index = date_range.index.to_period('M')
procedimentos_cliente_nf = pd.concat([date_range,procedimentos_cliente_nf]).fillna(0)

procedimentos_cliente_nf  = procedimentos_cliente_nf.reset_index().groupby(['DTPROC','Z05_CODCLI','Z05_LOJA','A1_NOME', 'A1_NREDUZ', 'A1_EST','Z05_DOC']).sum('Z05_QUANT').reset_index()
procedimentos_cliente_nf = procedimentos_cliente_nf.pivot_table(columns='DTPROC', index=['Z05_CODCLI','Z05_LOJA','A1_NOME', 'A1_NREDUZ', 'A1_EST'],values='Z05_DOC',aggfunc='count').fillna(0)
procedimentos_cliente_nf = procedimentos_cliente_nf[1:]
#procedimentos_cliente_nf.to_clipboard(decimal=',')

procedimentos_cliente_media = procedimentos_cliente.pivot_table(values=procedimentos_cliente, index = ['B1_COD','Z05_CODCLI','Z05_LOJA','A1_NOME','A1_NREDUZ','A1_EST']).fillna(0).mean(axis=1)
procedimentos_cliente_media = procedimentos_cliente_media.rename('Procedimentos')

fat_dir = pd.read_sql(query_fat_dir,conn)

fat_dir['DATES'] = pd.to_datetime(fat_dir['DATES'], format='%d/%m/%Y')

inicio = min(fat_dir['DATES']).strftime('%m-%d-%Y')
fim = max(fat_dir['DATES']).strftime('%m-%d-%Y')
date_range = pd.Series(pd.date_range(start=inicio,end=fim,freq='M')).rename('DATES')
fat_dir = pd.concat([fat_dir, date_range]).fillna(0)
fat_dir['MONTH'] = fat_dir['DATES'].apply(lambda x: x.to_period('M') if type(x) == pd.Timestamp else pd.Timestamp(year=2020,month=1,day=1).to_period('M')  if type(x) == int else 0)
#fat_dir['MONTH'] = fat_dir['DATES'].dt.to_period('M')

fat_dir = fat_dir.fillna('')

fat_dir_cliente = fat_dir[['B1_COD', 'B1_DESC','CLIFOR_COD', 'CLIFOR_LOJ','NOME', 'NREDUZ', 'ESTADO','MONTH','QUANT']].groupby(['B1_COD','CLIFOR_COD', 'CLIFOR_LOJ','NOME', 'NREDUZ', 'ESTADO','MONTH']).sum('QUANT')
try:
    fat_dir_cliente  = fat_dir_cliente.pivot_table(columns= 'MONTH',index=['B1_COD','CLIFOR_COD', 'CLIFOR_LOJ','NOME', 'NREDUZ', 'ESTADO'],values='QUANT').fillna(0)
    fat_dir_cliente = fat_dir_cliente[1:]
except:
    pass

try:
    fat_dir_mes = fat_dir[['B1_COD','CLIFOR_COD', 'CLIFOR_LOJ','MONTH','QUANT']].groupby(['B1_COD','CLIFOR_COD', 'CLIFOR_LOJ','MONTH']).sum('QUANT')
    fat_dir_mes = fat_dir_mes.pivot_table(index= ['B1_COD','CLIFOR_COD', 'CLIFOR_LOJ'], columns='MONTH', values='QUANT').fillna(0).mean(axis=1)
    fat_dir_mes = fat_dir_mes.rename('Fat Direto')
except:
    pass

try:
    fat_dir_mes_prod = fat_dir[['B1_COD','MONTH','QUANT']].groupby(['B1_COD','MONTH']).sum('QUANT')
    fat_dir_mes_prod = fat_dir_mes_prod.pivot_table(index= 'B1_COD', columns='MONTH', values='QUANT').fillna(0).iloc[:,:-1].mean(axis=1)
    fat_dir_mes_prod = fat_dir_mes_prod.rename('fat_dir')
    
    fat_dir_mes_prod_m6 = fat_dir[['B1_COD','MONTH','QUANT']].groupby(['B1_COD','MONTH']).sum('QUANT')
    fat_dir_mes_prod_m6 = fat_dir_mes_prod_m6.pivot_table(index= 'B1_COD', columns='MONTH', values='QUANT').fillna(0).iloc[:,:-1]
    fat_dir_mes_prod_m6 = fat_dir_mes_prod_m6.iloc[:,ncols-7:ncols-1].mean(axis=1)
    fat_dir_mes_prod_m6 = fat_dir_mes_prod_m6 .rename(index='FAT DIRETO M6')
    
    fat_dir_mes_prod_m6_dp = fat_dir[['B1_COD','MONTH','QUANT']].groupby(['B1_COD','MONTH']).sum('QUANT')
    fat_dir_mes_prod_m6_dp = fat_dir_mes_prod_m6_dp.pivot_table(index= 'B1_COD', columns='MONTH', values='QUANT').fillna(0).iloc[:,:-1]
    fat_dir_mes_prod_m6_dp = fat_dir_mes_prod_m6_dp.iloc[:,ncols-7:ncols-1].std(axis=1)
    fat_dir_mes_prod_m6_dp = fat_dir_mes_prod_m6_dp.rename(index='FAT DIRETO M6 dp')
except:
    pass

try:
    fat_dir_mes_prod_dp = fat_dir[['B1_COD','MONTH','QUANT']].groupby(['B1_COD','MONTH']).sum('QUANT')
    fat_dir_mes_prod_dp = fat_dir_mes_prod_dp.pivot_table(index= 'B1_COD', columns='MONTH', values='QUANT').fillna(0).iloc[:,:-1].std(axis=1)
    fat_dir_mes_prod_dp = fat_dir_mes_prod_dp.rename('fat_dir_dp')
except:
    pass



remessas = pd.read_sql(query_remessas,conn)
remessas['DATES'] = pd.to_datetime(remessas['DATES'], format='%d/%m/%Y')


inicio = min(remessas['DATES']).strftime('%m-%d-%Y')
fim = max(remessas['DATES']).strftime('%m-%d-%Y')
date_range = pd.Series(pd.date_range(start=inicio,end=fim,freq='M')).rename('DATES')
date_range  = pd.DataFrame({'DATES': date_range})
date_range['QUANT'] = [0] * len(date_range)
#date_range.index = date_range
#date_range.index = date_range.index.to_period('M')
remessas.index
date_range.index
remessas = pd.concat([date_range,remessas]).fillna('')
remessas['MONTH'] = remessas['DATES'].dt.to_period('M')
remessas_mes = remessas 
remessas_mes['MONTH'] = remessas_mes['DATES'].dt.to_period('M')
#remessas_mes = remessas_mes[remessas_mes['MONTH'] >= '2020-07']
#remessas_mes = remessas_mes.drop(columns='MONTH')
remessas_mes = remessas_mes[['B1_COD','B1_DESC','CLIFOR_COD', 'CLIFOR_LOJ','NOME','NREDUZ','ESTADO','MONTH','QUANT']].groupby(['B1_COD','B1_DESC','CLIFOR_COD', 'CLIFOR_LOJ','NOME','NREDUZ','ESTADO','MONTH']).sum('QUANT')

remessas_mes = remessas_mes.pivot_table(index= ['B1_COD','B1_DESC','CLIFOR_COD', 'CLIFOR_LOJ','NOME','NREDUZ','ESTADO'], columns='MONTH', values='QUANT').fillna(0)
remessas_mes = remessas_mes[1:]
#remessas_mes = remessas_mes.fillna(0).mean(axis=1)
#remessas_mes = remessas_mes.rename('Remessas')
#remessas_mes .to_clipboard(decimal=',')

remessas_prod = remessas[['B1_COD','MONTH','QUANT']].groupby(['B1_COD','MONTH']).sum('QUANT')
remessas_prod = remessas_prod.pivot_table(index= 'B1_COD', columns='MONTH', values='QUANT').fillna(0).iloc[:,:-1].mean(axis=1)
remessas_prod = remessas_prod.rename('Remessas')

remessas_prod_m6 = remessas[['B1_COD','MONTH','QUANT']].groupby(['B1_COD','MONTH']).sum('QUANT')
remessas_prod_m6 = remessas_prod_m6.pivot_table(index= 'B1_COD', columns='MONTH', values='QUANT').fillna(0)
nrows, ncols = remessas_prod_m6.shape
remessas_prod_m6 = remessas_prod_m6.iloc[:,ncols-7:ncols-1].mean(axis=1)
remessas_prod_m6 = remessas_prod_m6.rename('Remessas M6')

remessas_prod_m6_dp = remessas[['B1_COD','MONTH','QUANT']].groupby(['B1_COD','MONTH']).sum('QUANT')
remessas_prod_m6_dp = remessas_prod_m6_dp.pivot_table(index= 'B1_COD', columns='MONTH', values='QUANT').fillna(0)
nrows, ncols = remessas_prod_m6_dp.shape
remessas_prod_m6_dp = remessas_prod_m6_dp.iloc[:,ncols-7:ncols-1].std(axis=1)
remessas_prod_m6_dp = remessas_prod_m6_dp.rename('Remessas M6 dp')

remessas_prod_m12_6 = remessas[['B1_COD','MONTH','QUANT']].groupby(['B1_COD','MONTH']).sum('QUANT')
remessas_prod_m12_6 = remessas_prod_m12_6.pivot_table(index= 'B1_COD', columns='MONTH', values='QUANT').fillna(0)
nrows, ncols = remessas_prod_m12_6.shape
remessas_prod_m12_6 = remessas_prod_m12_6.iloc[:,ncols-13:ncols-7].mean(axis=1)
remessas_prod_m12_6 = remessas_prod_m12_6.rename('Remessas M12 - M6')

remessas_mes_nf = remessas[['CLIFOR_COD', 'CLIFOR_LOJ','NOME','NREDUZ','ESTADO','MONTH','QUANT','DOC']].groupby(['CLIFOR_COD', 'CLIFOR_LOJ','NOME','NREDUZ','ESTADO','MONTH','DOC']).sum('QUANT')
remessas_mes_nf = remessas_mes_nf.reset_index().pivot_table(index= ['CLIFOR_COD', 'CLIFOR_LOJ','NOME','NREDUZ','ESTADO'], values='DOC', columns='MONTH', aggfunc='count').fillna(0)
remessas_mes_nf = remessas_mes_nf[1:]
#remessas_mes_nf.to_clipboard(decimal=',')

#tempo_fat_q.to_clipboard(decimal=',')
tempo_fat_q = pd.read_sql(query_tempo_fat, conn)
#tempo_fat = tempo_fat_q.dropna()
tempo_fat = pd.read_sql(query_tempo_fat, conn)
tempo_fat = tempo_fat[tempo_fat['CTD_DESC01'].str.contains(linha,na=False)]
tempo_fat['C6_DATFAT'] = tempo_fat['C6_DATFAT'].str.replace('  /  /    ','01/01/2001')
tempo_fat['C6_DATFAT'] = pd.to_datetime(tempo_fat['C6_DATFAT'], format='%d/%m/%Y')
tempo_fat['Z05_DTPROC'] = tempo_fat['Z05_DTPROC'].str.replace('  /  /    ','01/01/2001')
tempo_fat['Z05_DTPROC'] = tempo_fat['Z05_DTPROC'].str.replace('3021', '2021')
tempo_fat['Z05_DTPROC'] = pd.to_datetime(tempo_fat['Z05_DTPROC'], format='%d/%m/%Y')
tempo_fat['proc_vs_fat'] = (tempo_fat['C6_DATFAT'] - tempo_fat['Z05_DTPROC']).dt.days

tempo_fat_nf = tempo_fat[tempo_fat['C6_DATFAT'] >= datetime.datetime(2021,1,1)]
tempo_fat_nf.index = tempo_fat_nf['B1_COD']
tempo_fat_prod = tempo_fat_nf['proc_vs_fat'].groupby('B1_COD').mean()
tempo_fat_prod = tempo_fat_prod.rename('dias_proc_vs_fat')

cmv_prod = cmv.groupby('B1_COD').first()

ult_custo_prod = ult_custo.groupby('B1_COD').first()
ult_custo_prod = ult_custo_prod.rename(columns={'CUSTO_UNIT':'ÚLTIMO CUSTO'})

estoque_p3_saldo_prod_linha.index.rename('B1_COD',inplace=True)

tabela = pd.concat([produtos, pedidos, pedidos_total, pedidos_em_aberto, venc_prod, estoque_prod_valid, estoque_p3_saldo_prod_linha,
                    procedimentos_prod_m6, procedimentos_prod_m6_dp,
                    fat_dir_mes_prod_m6, fat_dir_mes_prod_m6_dp,
                    remessas_prod_m6, remessas_prod_m6_dp,
                    tempo_fat_prod,
                    cmv_prod, ult_preco_prod, ult_custo_prod
                    ],axis=1).fillna(0)
tabela['Custo Estoque'] = tabela['estoque_total'] * tabela['ÚLTIMO CUSTO']
tabela['Custo Estoque P3'] = tabela['P3'] * tabela['ÚLTIMO CUSTO']
tabela['Remessas + Fat. Direto'] = tabela['FAT DIRETO M6'] + tabela['Remessas M6']

formato_cond = {'type':'3_color_scale',
                'criteria': '%',
                'value': 0,
                'min_color': "#f8696b",
                'mid_color': '#ffeb84',
                'max_color': "#63be7b"}

with pd.ExcelWriter('output_{}.xlsx'.format(linha)) as writer:  
    tabela.to_excel(writer, sheet_name='Geral', index=False)
    worksheet = writer.sheets['Geral']
    (max_row, max_col) = tabela.shape
    
    #estoque_cols = len(estoque_prod_valid.columns)
    
    #tabela.columns[tabela.columns.str.contains('< 30 DIAS')]
    #tabela.columns.get_loc('< 30 DIAS')    
    counter = len(pedidos_em_aberto)
    try:
        formato = writer.book.add_format({'bg_color': 'red'})
        worksheet.set_column(tabela.columns.get_loc('< 30 DIAS'),tabela.columns.get_loc('< 30 DIAS'), None, formato)
        counter += 1
    except:
        pass
    try:
        formato = writer.book.add_format({'bg_color': 'yellow'})
        worksheet.set_column(tabela.columns.get_loc('30 < 60 DIAS'),tabela.columns.get_loc('60 < 90 DIAS'), None, formato)
        counter += 1
    except:
        pass
    try:
        formato = writer.book.add_format({'bg_color': 'yellow'})
        worksheet.set_column(tabela.columns.get_loc('60 < 90 DIAS'),tabela.columns.get_loc('60 < 90 DIAS'), None, formato)
        counter += 1
    except:
        pass
    try:
        formato = writer.book.add_format({'bg_color': '#92D050'})
        worksheet.set_column(tabela.columns.get_loc('60 < 90 DIAS'),tabela.columns.get_loc('> 180 DIAS'), None, formato)
        counter += 1
    except:
        pass
    try:
        formato = writer.book.add_format({'bg_color': '#92D050'})
        worksheet.set_column(tabela.columns.get_loc('90 < 180 DIAS'),tabela.columns.get_loc('> 180 DIAS'), None, formato)
        counter += 1
    except:
        pass
    try:
        formato = writer.book.add_format({'bg_color': '#92D050'})
        worksheet.set_column(tabela.columns.get_loc('> 180 DIAS'),tabela.columns.get_loc('> 180 DIAS'), None, formato)
        counter += 1
    except:
        pass
    worksheet.set_column(4,max_col-17-counter,None, None, {'hidden':1})
    worksheet.write(0,max_col, 'Duração em dias')
    worksheet.write(0,max_col+1,'Data Stock Out')
    worksheet.write(0,max_col+2,'Duração Desejada')
    worksheet.write(0,max_col+3,'Recomendação Compra')
    worksheet.write(0,max_col+4,'Lead Time')
    worksheet.write(0,max_col+5,'Data Recomendada Compra')
    worksheet.write(0,max_col+6,'Valor Pedido')
    for row in range(2,max_row):
        #formula = '='
        col_estoque = xlsxwriter .utility.xl_col_to_name(tabela.columns.get_indexer(target=['estoque_total'])[0])        
        col_remessa_fat = xlsxwriter.utility.xl_col_to_name(tabela.columns.get_indexer(target=['Remessas + Fat. Direto'])[0])
        worksheet.write_formula(row -1 ,max_col , '=IFERROR({estoque}{row}/({demanda}{row}/30),0)'.format(estoque=col_estoque,demanda=col_remessa_fat,row=row))
        
        worksheet.write(row-1, max_col + 1, 'Data Stock Out')
        col_duracao = xlsxwriter.utility.xl_col_to_name(tabela.columns.get_indexer(target=['Remessas + Fat. Direto'])[0] + 1)
        col_duracao_desej = xlsxwriter.utility.xl_col_to_name(tabela.columns.get_indexer(target=['Remessas + Fat. Direto'])[0] + 3)
        
        col_lead_time = xlsxwriter.utility.xl_col_to_name(tabela.columns.get_indexer(target=['Remessas + Fat. Direto'])[0] + 5)
        
        col_stock_out = xlsxwriter.utility.xl_col_to_name(tabela.columns.get_indexer(target=['Remessas + Fat. Direto'])[0] + 2)
        worksheet.write_formula(row-1, max_col + 1, '=TODAY()+{duracao}{row}'.format(duracao=col_duracao, row=row), writer.book.add_format({'num_format':'dd/mm/yy'}))
        worksheet.write_formula(row-1, max_col + 3, '=ROUND(IF({duracao}{row}>{duracao_desej}{row},0,({duracao_desej}{row}-{duracao}{row})/30*{demanda}{row}),0)'.format(demanda=col_remessa_fat, row=row,duracao=col_duracao,duracao_desej=col_duracao_desej))
        
        worksheet.write_formula(row-1,max_col + 5, '=IF({lead_time}{row}>0,{duracao}{row}-{lead_time}{row},"")'.format(lead_time=col_lead_time,row=row,duracao=col_stock_out), writer.book.add_format({'num_format':'dd/mm/yy'}))
        
        col_ult_preco = xlsxwriter.utility.xl_col_to_name(tabela.columns.get_indexer(target=['ÚLTIMO PREÇO'])[0])
        col_pedido = xlsxwriter.utility.xl_col_to_name(tabela.columns.get_indexer(target=['Remessas + Fat. Direto'])[0] + 4)
        worksheet.write_formula(row-1,max_col + 6, '={pedido}{row} * {ult_preco}{row}'.format(row=row,ult_preco= col_ult_preco, pedido=col_pedido))
    worksheet.set_column(0, max_col +6,12)
    worksheet.autofilter(0,0,max_row,max_col+6)
    worksheet.set_column(max_col+2,max_col+2,None,writer.book.add_format({'bg_color': 'yellow'}))
    worksheet.set_column(max_col+4,max_col+4,None,writer.book.add_format({'bg_color': 'yellow'}))
    
    
    #remessas_mes_nf ok
    remessas_mes_nf.to_excel(writer,sheet_name='Nr Remessas')
    worksheet = writer.sheets['Nr Remessas']
    (max_row, max_col) = remessas_mes_nf.shape
    index_size = len(remessas_mes_nf.index[0])
    worksheet.conditional_format(1,index_size,max_row,max_col+index_size,formato_cond)
    worksheet.set_column(0,max_col+index_size - 1)
    worksheet.autofilter(0,0,max_row,max_col+index_size -1)
    
    #procedimentos_cliente_nf ok
    #procedimentos_cliente_nf.to_clipboard(decimal=',')
    procedimentos_cliente_nf.to_excel(writer,sheet_name='Nr Procedimentos')
    worksheet = writer.sheets['Nr Procedimentos']
    (max_row, max_col) = procedimentos_cliente_nf.shape
    index_size = len(procedimentos_cliente_nf.index[0])
    worksheet.conditional_format(1,index_size,max_row, max_col+index_size,formato_cond)    
    worksheet.autofilter(0,0,max_row, max_col+index_size -1)

    #remessas_mes ok
    remessas_mes.to_excel(writer,sheet_name='Prod Remessas',merge_cells=False)
    worksheet = writer.sheets['Prod Remessas']
    (max_row,max_col) = remessas_mes.shape
    index_size = len(remessas_mes.index[0])
    worksheet.conditional_format(1,index_size,max_row,max_col+index_size,formato_cond)
    worksheet.autofilter(0,0,max_row,max_col+index_size-1)
    
    #procedimentos_cliente ok
    procedimentos_cliente.to_excel(writer, sheet_name='Prod Procedimentos', merge_cells=False)
    worksheet = writer.sheets['Prod Procedimentos']
    (max_row,max_col) = procedimentos_cliente.shape
    index_size = len(procedimentos_cliente.index[0])
    worksheet.conditional_format(1,index_size,max_row,max_col+index_size,formato_cond)
    worksheet.autofilter(0,0,max_row,max_col+index_size-1)
    
    #fat_dir
    fat_dir_cliente.to_excel(writer, sheet_name='Fat Direto', merge_cells=False)
    worksheet = writer.sheets['Fat Direto']
    (max_row,max_col) = fat_dir_cliente.shape
    index_size = len(fat_dir_cliente.index[0])
    worksheet.conditional_format(1,index_size, max_row, max_col+index_size,formato_cond)
    worksheet.autofilter(0,0,max_row,max_col+index_size-1)
    
    estoque_p3_saldo_linha.to_excel(writer,sheet_name='Saldo P3', index=False)
    worksheet = writer.sheets['Saldo P3']
    (max_row, max_col) = estoque_p3_saldo_linha.shape
    worksheet.autofilter(0,0,max_row,max_col-1)
       
    tempo_fat.to_excel(writer, sheet_name='Tempo Faturamento',index=False)
    worksheet = writer.sheets['Tempo Faturamento']
    (max_row, max_col) = tempo_fat.shape
    worksheet.autofilter(0,0,max_row,max_col)
