#!/usr/bin/python
import urllib
import json
import tokens
import datetime
import time
import pickle
import pandas as pd

start_time = time.time()

edTokens = tokens.getTokens()

def getDecimals(amount, precision):
	i = precision - len(amount)
	if i > 0:
		k=0
		while k<i:
			amount = "0"+ amount
			k=k+1

	length = len(amount) 
	subst = length  - precision
	ret = amount[0:subst] + "," + amount[subst:7]
	if ret[0:1] ==",":
		ret = "0"+ret
		
	if ret[-1:]==',':
		ret= ret[:-1]
	return float(ret.replace(',','.'))

def get_df_from_file(file_name):
	
	df = pd.read_pickle(file_name)

	df = df.drop(columns=['address', 'logIndex', 'transactionIndex'])

	df['blockNumber'] = df['blockNumber'].apply(lambda x: int(x,16))
	#df['gasPrice'] = df['gasPrice'].apply(lambda x: int(x,16))
	#df['gasUsed'] = df['gasUsed'].apply(lambda x: int(x,16))
	df['timeStamp'] = df['timeStamp'].apply(lambda x: int(x,16))
	df = df.rename({'timeStamp':'timeStampUnix'},axis='columns')
	df['timeStampString'] = df['timeStampUnix'].apply(lambda x: datetime.datetime.utcfromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S'))
	df['data'] = df['data'].apply(lambda x: x[2:])

	tokenGet = df['data'].apply(lambda x: '0x' + x[24:64])
	amountGetRaw = df['data'].apply(lambda x: x[65:128])
	tokenGive = df['data'].apply(lambda x: '0x' + x[152:192])
	amountGiveRaw = df['data'].apply(lambda x: x[193:256])
	maker = df['data'].apply(lambda x: '0x' + x[280:320])
	taker = df['data'].apply(lambda x: '0x' + x[344:384])

	def catch(func, t, dec=False, ret=0):
		try:
			return func(t)
		except Exception as e:
			if dec:
				return 1
			elif ret!=0:
				return ret
			else:
				return t

	tokenGetName =pd.Series([catch(lambda x: edTokens[x].name, t) for t in tokenGet])
	tokenGetDecimal =pd.Series([catch(lambda x: edTokens[x].decimal, t, dec=True) for t in tokenGet])

	tokenGiveName =pd.Series([catch(lambda x: edTokens[x].name, t) for t in tokenGive])
	tokenGiveDecimal =pd.Series([catch(lambda x: edTokens[x].decimal, t, dec=True) for t in tokenGive])

	amountGive = pd.Series([getDecimals(str(int(x,16)), y) for x, y in zip(amountGiveRaw, tokenGiveDecimal)])
	amountGet = pd.Series([getDecimals(str(int(x,16)), y) for x, y in zip(amountGetRaw, tokenGetDecimal)])

	df['buyer'] = maker.where([x != 'ETH' for x in tokenGetName], taker)
	df['seller'] = maker.where([x == 'ETH' for x in tokenGetName], taker)
	
	priceGetETH = pd.Series([catch(lambda z: z[0]/z[1], (x,y), ret=x) for x,y in zip(amountGet, amountGive)])
	priceGiveETH = pd.Series([catch(lambda z: z[0]/z[1], (x,y), ret=x) for x,y in zip(amountGive, amountGet)])
	df['price'] = priceGetETH.where([x == 'ETH' for x in tokenGetName], priceGiveETH)

	df['amountETH'] = amountGet.where([x == 'ETH' for x in tokenGetName], amountGive)
	df['amountALT'] = amountGet.where([x != 'ETH' for x in tokenGetName], amountGive)

	df['token_name'] = tokenGetName.where([x != 'ETH' for x in tokenGetName], tokenGiveName)
	df['token_decimal'] = tokenGetDecimal.where([x != 'ETH' for x in tokenGetName], tokenGiveDecimal)

	fee = df['amountETH']*.0003
	df['seller_fee'] = fee.where([s == t for s, t in zip(df['seller'],taker)], 0.)
	df['buyer_fee'] = fee.where([s != t for s, t in zip(df['seller'],taker)], 0.)

	'''
	for x in range(len(tokenGiveName)):
		if tokenGetName[x]==tokenGiveName[x]:
			print 'Both ETH'
			print tokenGetName[x],tokenGiveName[x]
			print df['amountETH'][x],df['amountALT'][x]
			print df['transactionHash'][x]
	'''

	df = df.drop(columns=['data', 'gasPrice', 'gasUsed'])

	return df


first_block = 5346500
last_block = 5356500
#5356353 

#first_block = 3250000
#last_block = 4760000
blocks_per_file = 10000

file_name = 'data/trades_'+str(first_block)+'_'+str(first_block+blocks_per_file)+'.pkl'
df = get_df_from_file(file_name)

for b in range(first_block+blocks_per_file, last_block, blocks_per_file):
	#print 'blocks '+str(b)+'--'+str(b+blocks_per_file)

	file_name = 'data/trades_'+str(b)+'_'+str(b+blocks_per_file)+'.pkl'

	df_block = get_df_from_file(file_name)

	df = pd.concat([df, df_block])

#file_name = 'data/processed_trades.pkl'
#file_name = 'data/recent_trades.pkl'
#df.to_pickle(file_name)

print '\nSaved to '+file_name+'\n'

print("--- %s seconds ---" % (time.time() - start_time))