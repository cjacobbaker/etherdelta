#!/usr/bin/python
import time
import pickle
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np


start_time = time.time()


########### Disposition Effect ###################

def disposition():

	# These are all lists of arrays, where each array contain gains/losses for a single individual over each day.

	with open('data/results/all_rg.pkl', 'rb') as f:
		all_rg = pickle.load(f)

	with open('data/results/all_rl .pkl', 'rb') as f:
		all_rl  = pickle.load(f)


	with open('data/results/all_pg.pkl', 'rb') as f:
		all_pg = pickle.load(f)


	with open('data/results/all_pl.pkl', 'rb') as f:
		all_pl = pickle.load(f)

	'''
	for i in [68,8,25,33]:
		del all_rg[i]
		del all_rl[i]
		del all_pg[i]
		del all_pl[i]
	'''

	start = 0
	end = 313

	trg = np.sum([x[start:end] for x in all_rg])
	trl = np.sum([x[start:end] for x in all_rl])
	tpg = np.sum([x[start:end] for x in all_pg])
	tpl = np.sum([x[start:end] for x in all_pl])

	pgr = trg/(trg+tpg)
	plr = trl/(trl+tpl)

	print 'proportion of gains realized',pgr
	print 'proportion of losses realized',plr

	#print min([min(x) for x in all_rl])
	#print np.argmin([min(x) for x in all_rl])
	#print trg
	#print trl
	#print tpg
	#print tpl

	
disposition()

################### Rank Effect #################

def rank():

	with open('data/results/sold_ind.pkl', 'rb') as f:
		high_sold,mh_sold,mid_sold,ml_sold,low_sold,mid_total = pickle.load(f)

	with open('data/results/all_token_best.pkl', 'rb') as f:
		all_token_best = pickle.load(f)

	with open('data/results/all_token_worst.pkl', 'rb') as f:
		all_token_worst = pickle.load(f)


	with open('data/results/all_token_own.pkl', 'rb') as f:
		all_token_own = pickle.load(f)


	with open('data/results/all_token_sold .pkl', 'rb') as f:
		all_token_sold  = pickle.load(f)

	tn = len(all_token_best)
	dn = all_token_best[0].shape[0]
	kn = all_token_best[0].shape[1]

	# Simple Results

	# best = num_best_sold / (num_best_sold + num_best_unsold)

	# best token for day d and trader t is

	rank_prop = [0., 0., 0., 0., 0.]
	rank_prop[0] = sum(high_sold)/len(high_sold)
	rank_prop[1] = sum(mh_sold)/len(mh_sold)
	rank_prop[2] = float(sum(mid_sold))/float(sum(mid_total))
	rank_prop[3] = sum(ml_sold)/len(ml_sold)
	rank_prop[4] = sum(low_sold)/len(low_sold)

	print 'sell-rate by rank',rank_prop

	pd.DataFrame(rank_prop).plot(kind='bar', color='green')

	plt.legend().set_visible(False)
	locs, labels = plt.xticks()
	xt_label = ['Highest', 'Second Highest', 'Middle', 'Second Lowest', 'Lowest']
	plt.xticks(locs, xt_label, rotation =10)
	plt.grid(axis='y')
	plt.ylabel('Probability of Sale')

	plt.savefig('../Report/images/rank_hist.pdf')

	# Find all individuals who best ranked j,d
	# Find all individuals who did not best rank j,d
	# If not some of each, skp this j,d
	# Otherwise, get the num of the best-rankers who sold and the number of non-best-rankers who sold
	# Repeat, and take the average
	
	best_diff = 0.
	pairs = 0.

	for d in range(dn):
		for j in range(kn):
			besters = [i for i in range(tn) if all_token_best[i][d,j]==1]
			notters = [i for i in range(tn) if (all_token_best[i][d,j]!=1 and all_token_own[i][d,j]==1)]

			if len(besters)==0 or len(notters)==0:
				continue

			solders = [i for i in range(tn) if all_token_sold[i][d,j]==1]

			sell_best = len(set(besters).intersection(set(solders)))
			sell_not_best = len(set(notters).intersection(set(solders)))

			best_diff += (sell_best - sell_not_best)
			pairs += len(besters)*len(notters)

	print 'best - not best',best_diff/pairs

	worst_diff = 0.
	pairs = 0.

	for d in range(dn):
		for j in range(kn):
			worsters = [i for i in range(tn) if all_token_worst[i][d,j]==1]
			notters = [i for i in range(tn) if (all_token_worst[i][d,j]!=1 and all_token_own[i][d,j]==1)]

			if len(worsters)==0 or len(notters)==0:
				continue

			solders = [i for i in range(tn) if all_token_sold[i][d,j]==1]

			sell_worst = len(set(worsters).intersection(set(solders)))
			sell_not_worst = len(set(notters).intersection(set(solders)))

			worst_diff += (sell_worst - sell_not_worst)
			pairs += len(worsters)*len(notters)

	print 'worst - not worst',worst_diff/pairs

	#print np.sum(all_token_best[0])
	#print all_token_own[0]
	#print all_token_sold[0]

rank()


print("--- %s seconds ---" % (time.time() - start_time))