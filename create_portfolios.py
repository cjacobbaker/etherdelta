#!/usr/bin/python
import urllib
import json
import tokens
import datetime
import time
import pickle
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import copy

start_time = time.time()

tokens_list = list(pd.read_pickle('data/tokens_by_frequency.pkl'))[:600]
tn = len(tokens_list)

pe = pd.read_pickle('data/prices/df_eth_usd.pkl')[['time','price']]

# partitions for days
tmin = 1486731600
tmax = 1513731600
days = range(tmin, tmax, 86400)

dn = len(days)

def get_token_index(token):
	return tokens_list.index(token)

def get_day(timestamp):
	for di in range(dn-1):
		if timestamp < days[di+1] and timestamp >= days[di]:
			return di

def get_day_length(cur_day, time_list):
	return sum([ (x < cur_day+86400) and (x >= cur_day) for x in time_list ])

def get_ofinterest(sell_cutoff = 10, buy_cutoff = 10):
	df = pd.read_pickle('data/df_reduced.pkl')
	sellers = set(df.seller.unique())
	buyers = set(df.buyer.unique())
	accounts = sellers.union(buyers)
	of_interest = sellers.intersection(buyers)

	svc = df['seller'].value_counts(sort=False)
	bvc = df['buyer'].value_counts(sort=False)

	of_interest = [x for x in of_interest if (svc[x]>=sell_cutoff and bvc[x]>=buy_cutoff)]
	
	df = df[df['token_name'].isin(tokens_list)]

	print 'Total Accounts: ', len(accounts)
	print 'Traders of Interest: ', len(of_interest)

	return of_interest, df

def get_price_token(token, timestamp):
	round_time = timestamp - (timestamp%3600)
	tl = token.lower()
	file_name = 'data/prices/df_'+tl+'_eth.pkl'
	df_p = pd.read_pickle(file_name)
	pt = df_p[['time', 'price']]
	price = pt[pt['time']==round_time]['price'].iloc[0] * get_price_eth(timestamp)
	if np.isnan(price):
		return 0.
	return price

def get_hlp_token(token, timestamp):
	round_time = timestamp - (timestamp%3600)

	tl = token.lower()
	file_name = 'data/prices/df_'+tl+'_eth.pkl'
	if file_name == 'data/prices/df_xmas_eth.pkl':
		return 0., 0., 0.
	df_p = pd.read_pickle(file_name)
	hlpt = df_p[['time', 'price', 'high', 'low']]

	price_eth = get_price_eth(timestamp)

	price = hlpt[hlpt['time']==round_time]['price'].iloc[0] * price_eth
	high = hlpt[hlpt['time']==round_time]['high'].iloc[0] * price_eth
	low = hlpt[hlpt['time']==round_time]['low'].iloc[0] * price_eth
	if np.isnan(price) or np.isnan(high) or np.isnan(low):
		return 0., 0., 0.

	return high, low, price

def get_price_eth(timestamp):
	round_time = timestamp - (timestamp%3600)
	price = pe[pe['time']==round_time]['price'].iloc[0]
	if np.isnan(price):
		return 0.
	return price

def get_paper_gain_loss(port, timestamp, tokens):
	# iterate over tokens
	paper_gain = 0.
	paper_loss = 0.

	for t in tokens:
		if port[t][0]==0:
			continue

		# market prices
		mh, ml, mp = get_hlp_token(t, timestamp)
		# average purchase price
		app = port[t][1]

		if mh>app and ml>app:
			paper_gain += (mp-app) * port[t][0]
		elif mh<app and ml<app:
			paper_loss += (mp-app) * port[t][0]

	return paper_gain, paper_loss

def get_token_ranks(port, timestamp, tokens):

	paper_profits = [0. for i in range(len(tokens))]

	for i in range(len(tokens)):
		mp = get_price_token(tokens[i], timestamp)
		app = port[tokens[i]][1]

		paper_profits[i] = (mp-app) * port[tokens[i]][0]

	paper_profits = np.array(paper_profits)

	hm_index, h_index = list(paper_profits.argsort()[-2:])
	l_index, lm_index = list(paper_profits.argsort()[:2])

	high = copy.copy(tokens[h_index])
	hmid = copy.copy(tokens[hm_index])
	lmid = copy.copy(tokens[lm_index])
	low = copy.copy(tokens[l_index])

	not_mid = [high, hmid, lmid, low]
	mid = [x for x in tokens if x not in not_mid]

	return high, hmid, mid, lmid, low

def traders_sells_buys(sc=200, bc=200):

	traders, df = get_ofinterest(sell_cutoff=sc, buy_cutoff=bc)

	sells = [ df[df['seller']==t] for t in traders ]

	print 'finished sells'
	print("--- %s seconds ---" % (time.time() - start_time))

	buys = [ df[df['buyer']==t] for t in traders ]

	print 'finished sells'
	print("--- %s seconds ---" % (time.time() - start_time))

	with open('data/traders_sells_buys.pkl', 'wb') as f:
		pickle.dump([traders, sells, buys], f)

	print 'Saved to data/traders_sells_buys.pkl'	

	return traders, sells, buys

def create_and_save_results(num_traders=0):

	with open('data/traders_sells_buys.pkl', 'rb') as f:
		traders, sells, buys = pickle.load(f)

	print 'We have ',len(traders),' traders, and will look at the first ',num_traders

	all_token_best = []
	all_token_worst = []
	all_token_own = []

	all_token_sold = []

	all_rg = []
	all_rl = []
	all_pg = []
	all_pl = []

	high_sold = []
	mh_sold = []
	mid_sold = []
	mid_total = []
	ml_sold = []
	low_sold = []

	if num_traders == 0:
		num_traders = len(traders)

	for ti in range(num_traders):

		print 'Starting trader ',ti,'/',num_traders

		t = traders[ti]
		s = sells[ti]
		b = buys[ti]

		# Get times for all trades or sells
		time_list = sorted(list(s['timeStampUnix']) + list(b['timeStampUnix']))

		# For each day, we need to have
		# - Within each of the five ranks, the percentage of tokens sold
		# - For each token, an indicator for whether it was extreme (best or worst)
		# - For each token, an indicator for whether it was sold
		# - Total real_gains, total real_losses, total paper_gains, total paper_losses

		##########

		day_token_best = np.zeros((dn,tn))
		day_token_worst = np.zeros((dn,tn))
		day_token_own = np.zeros((dn,tn))

		##########

		day_token_sold = np.zeros((dn,tn))

		##########

		day_rg = np.zeros(dn)
		day_rl = np.zeros(dn)
		day_pg = np.zeros(dn)
		day_pl = np.zeros(dn)

		##########

		# Get list of all tokens ever owned
		tokens = list( set( list(s['token_name']) + list(b['token_name']) ).intersection(set(tokens_list)) )

		port = [dict([(str(toke), [0.,0.]) for toke in tokens]) for x in time_list] # [number, avg. price]

		# Go over every day
		for di in range(dn-1):

			if di%50== 0:
				print '\tStarting day ',di,'/',dn

			cur_day_time = days[di]
			
			#Find the range of timestamps in time_list for this day
			bi = [t for t in range(len(time_list)) if time_list[t] >= days[di]]
			ei = [t for t in range(len(time_list)) if time_list[t] < days[di+1]]
			if bi==[] or ei==[]:
				continue

			day_begin_i = min(bi)   # Thesese are indices
			day_end_i = max(ei)
			day_length_i = day_end_i - day_begin_i

			unsold_tokens = [x for x in tokens if x in tokens_list]

			real_gain_day = 0.
			real_loss_day = 0.

			#Now iterate over every trade that took place on this day
			for it in range(day_begin_i, day_end_i+1):

				cur_time = time_list[it]
				cur_eth_price = get_price_eth(cur_time)

				# Seller
				if cur_time in list(s['timeStampUnix']):

					trade = s[s['timeStampUnix']==cur_time]
					toke = trade['token_name'].iloc[0]
					if (toke not in tokens_list) or (toke not in tokens):
						continue
					if toke in unsold_tokens:
						unsold_tokens.remove(toke)

					# update number owned; price stays same
					if it>0:
						port[it][toke][0] = max(0., port[it-1][toke][0]-trade['amountALT'].iloc[0])
						port[it][toke][1] = port[it-1][toke][1]

					# everything else remains the same
					if it>0:
						other_tokens = copy.copy(tokens)
						other_tokens.remove(toke)
						for ot in other_tokens:
							port[it][ot] = port[it-1][ot]

					#### see if it was worth it ####
					# what you got from selling

					if isinstance(trade['price'].iloc[0], tuple):
						p_stupid = 0.
					else:
						p_stupid = trade['price'].iloc[0]

					revenue = ( p_stupid * min([trade['amountALT'].iloc[0], port[it-1][toke][0]]) - trade['seller_fee'].iloc[0] ) * cur_eth_price
					# avg. purchase price:
					avp = port[it][toke][1]
					# total payment avg. for the stuff sold
					expen = avp * min(trade['amountALT'].iloc[0], port[it-1][toke][0])

					#### REAL GAINS AND LOSSES

					real_gain_day += max(0., revenue - expen)
					real_loss_day += min(0., revenue - expen)

				# Buyer
				else:
					trade = b[b['timeStampUnix']==cur_time]
					toke = trade['token_name'].iloc[0]
					if (toke not in tokens_list) or (toke not in tokens):
						continue

					# set number owned
					if it==0:
						port[it][toke][0] = trade['amountALT'].iloc[0]
					else:
						port[it][toke][0] += port[it-1][toke][0] + trade['amountALT'].iloc[0]

					# set avg. price
					if it==0:
						port[it][toke][1] = cur_eth_price *\
											(trade['amountETH'].iloc[0] + trade['buyer_fee'].iloc[0])/trade['amountALT'].iloc[0]
					else:
						xt1 = port[it-1][toke][0]
						pt1 = port[it-1][toke][1]
						xt = trade['amountALT'].iloc[0]
						pt = trade['price'].iloc[0]
						if isinstance(pt, tuple):
							pt = 0.
						pt = cur_eth_price * pt
						if (xt1+xt) > 0:
							port[it][toke][1] = (xt1*pt1+xt*pt+cur_eth_price*trade['buyer_fee'].iloc[0])/(xt1+xt)
						else:
							port[it][toke][1] = 0.

					# everything else remains the same
					if it>0:
						other_tokens = copy.copy(tokens)
						other_tokens.remove(toke)
						for ot in other_tokens:
							port[it][ot] = port[it-1][ot]

			# We have now updated the portfolio fothe the whole day, on to analysis

			sold_tokens = list([x for x in tokens if x not in unsold_tokens])

			#### DISPOSITION EFFECT: Real vs. Paper gains and losses

			paper_gain_day, paper_loss_day = get_paper_gain_loss(port[day_begin_i], cur_day_time, unsold_tokens)

			day_rg[di] = real_gain_day
			day_rl[di] = real_loss_day
			day_pg[di] = paper_gain_day
			day_pl[di] = paper_loss_day

			#### RANK EFFECT

			for toke in sold_tokens:
				day_token_sold[di, get_token_index(toke)] = 1

			# only want to look at tokens that are owned

			owned_tokens = list([x for x in tokens if port[day_begin_i][x][0]>0])
			#temp = copy.copy(owned_tokens)
			#owned_tokens = list([x for x in temp if (x in tokens_list) and (x != 'XMAS')])

			if len(owned_tokens) >= 5:

				high, hmid, mid, lmid, low = get_token_ranks(port[day_begin_i], cur_day_time, owned_tokens)




				if port[day_begin_i][high][0] != port[day_end_i][high][0]:
					high_sold.append(1.)
				else:
					high_sold.append(0.)

				if port[day_begin_i][hmid][0] != port[day_end_i][hmid][0]:
					mh_sold.append(1.)
				else:
					mh_sold.append(0.)

				mid_sold.append(float(sum([ (port[day_begin_i][m][0] != port[day_end_i][m][0]) for m in mid])))
				mid_total.append(float(len(mid)))

				if port[day_begin_i][lmid][0] != port[day_end_i][lmid][0]:
					ml_sold.append(1.)
				else:
					ml_sold.append(0.)

				if port[day_begin_i][low][0] != port[day_end_i][low][0]:
					low_sold.append(1.)
				else:
					low_sold.append(0.)

				day_token_best[di, get_token_index(high)] = 1
				day_token_worst[di, get_token_index(low)] = 1

				def eval(port, tkn):
					if tkn not in port[day_begin_i].keys():
						return 0
					elif port[day_begin_i][tkn][0]<=0:
						return 0
					return 1

				day_token_own[di,:] = [eval(port,tkn) for tkn in tokens_list]

			else:
				for i in range(tn):
					day_token_best[di, i] = 0
					day_token_worst[di, i] = 0
					day_token_own[di, i] = 0

		# We have now completed individual trader ti, and this is his important data

		all_token_best.append(day_token_best)
		all_token_worst.append(day_token_worst)
		all_token_own.append(day_token_own)

		all_token_sold.append(day_token_sold)

		all_rg.append(day_rg)
		all_rl.append(day_rl)
		all_pg.append(day_pg)
		all_pl.append(day_pl)

		# print np.sum(day_rg)/np.sum(day_rg+day_pg)
		# print np.sum(day_rl)/np.sum(day_rl+day_pl)


	# Save it all for future use

	with open('data/results/all_token_best.pkl', 'wb') as f:
		pickle.dump(all_token_best, f)
	print 'Saved to data/results/all_token_best.pkl'

	with open('data/results/all_token_worst.pkl', 'wb') as f:
		pickle.dump(all_token_worst, f)
	print 'Saved to data/results/all_token_worst.pkl'	

	with open('data/results/all_token_own.pkl', 'wb') as f:
		pickle.dump(all_token_own, f)
	print 'Saved to data/results/all_token_own.pkl'	

	with open('data/results/all_token_sold .pkl', 'wb') as f:
		pickle.dump(all_token_sold , f)
	print 'Saved to data/results/all_token_sold .pkl'	

	with open('data/results/all_rg.pkl', 'wb') as f:
		pickle.dump(all_rg, f)
	print 'Saved to data/results/all_rg.pkl'	

	with open('data/results/all_rl .pkl', 'wb') as f:
		pickle.dump(all_rl , f)
	print 'Saved to data/results/all_rl .pkl'	

	with open('data/results/all_pg.pkl', 'wb') as f:
		pickle.dump(all_pg, f)
	print 'Saved to data/results/all_pg.pkl'	

	with open('data/results/all_pl.pkl', 'wb') as f:
		pickle.dump(all_pl, f)
	print 'Saved to data/results/all_pl.pkl'

	with open('data/results/sold_ind.pkl', 'wb') as f:
		pickle.dump([high_sold,mh_sold,mid_sold,ml_sold,low_sold,mid_total], f)
	print 'Saved to data/results/sold_ind.pkl'

#traders_sells_buys(sc=200, bc=200)

create_and_save_results(num_traders=200)



print("--- %s seconds ---" % (time.time() - start_time))

