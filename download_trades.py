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

def getTradesInBlockRange(url):

	trade_list = []

	page = json.loads(urllib.urlopen(url).read())
	result = page['result']
	#print '\t'+str(len(result))+' trades found'

	if len(result) >= 999:
		print '\n*** ALERT: p_static_iteration too large ***\n'
		return -1, trade_list

	#loop all possible transaction in block range
	for i in result:
		del i['topics']
		trade_list.append(i)

	return len(result), trade_list
	 
def getTrades(p_fromBlock, p_toBlock):

	trade_list = []

	p_static_iteration = 75
	
	p_url = "https://api.etherscan.io/api?module=logs&action=getLogs&fromBlock=$1&toBlock="\
					"$2&address=0x8d12a197cb00d4747a1fe03395095ce2a5cc6819&topic0="\
					"0x6effdda786735d5033bfad5f53e5131abcced9e52be6c507b62d639685fbed6d&apikey=PWYCPV9Y82AE9HC9YVMQ7RGJZ7TS3E15MB"
	
	fromBlock = p_fromBlock - p_static_iteration -1
	toBlock   = p_fromBlock
	total_trades=0
	while True:
		if toBlock >= p_toBlock:
			break
		fromBlock = fromBlock + p_static_iteration + 1
		toBlock   = fromBlock + p_static_iteration
		if toBlock > p_toBlock:
			toBlock = p_toBlock
		
		#print "reading blocks from " + str(fromBlock) + " to " + str(toBlock)
		call = p_url.replace("$1", str(fromBlock)).replace("$2", str(toBlock))
		res = getTradesInBlockRange(call)

		if res[0] == -1:
			fromBlock = fromBlock - p_static_iteration - 1
			toBlock   = fromBlock - p_static_iteration
			p_static_iteration = p_static_iteration/2
			print 'setting p_static_iteration to '+str(p_static_iteration)
		else:
			total_trades+=res[0]
			trade_list = trade_list + res[1]

	return trade_list

#getTrades(3150000, 5350000)

def generate(from_block, to_block):

	trade_list = getTrades(from_block, to_block)
	df = pd.DataFrame(trade_list)

	file_name = 'data/trades_'+str(from_block)+'_'+str(to_block)+'.pkl'
	df.to_pickle(file_name)

	'''
	with open(file_name, 'wb') as f:
		pickle.dump(df, f)
		print 'Saved to '+file_name+'\n'

	with open('trade_raw.pkl', 'rb') as f:
		loaded_trade_raw = pickle.load(f)
	'''

chunk_size = 10000
chunks = 1
start_chunk = 5346500

# end_chunk = 4763000

start = start_chunk
end = start_chunk+chunk_size
for c in range(chunks):
	print("--- %s seconds ---" % (time.time() - start_time))
	print 'Chunk '+str(c)+' of '+str(chunks)
	print "reading blocks from " + str(start) + " to " + str(end)
	generate(start, end)
	start += chunk_size
	end += chunk_size

print("--- %s seconds ---" % (time.time() - start_time))