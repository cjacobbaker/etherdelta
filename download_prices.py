#!/usr/bin/python
import urllib
import json
import tokens
import datetime
import time
import pickle
import pandas as pd

start_time = time.time()

ts1 = '1513731600'
ts2 = '1506531600'
ts3 = '1499331600'
ts4 = '1492131600'
# start = 1486731600

l123 = '2000'
l4 = '1500'

def reduce_main_df():
	file_name = 'data/processed_trades.pkl'
	df = pd.read_pickle(file_name)

	df_reduced = df[df['token_decimal']!=1]
	df_reduced = df_reduced[df_reduced['token_name']!='ETH']
	df_reduced = df_reduced[df_reduced['token_name']!='COSSOLD']
	df_reduced = df_reduced[df_reduced['token_name']!='IBTCOLD']
	df_reduced = df_reduced[df_reduced['token_name']!='HATOLD']
	df_reduced = df_reduced[df_reduced['token_name']!='FUCK_OLD']
	df_reduced = df_reduced[df_reduced['token_name']!='MKROLD']
	df_reduced = df_reduced[df_reduced['token_name']!='BITCOINEREUM']
	df_reduced = df_reduced[df_reduced['token_name']!='ELYTE']
	df_reduced = df_reduced[df_reduced['token_name']!='EETHER']
	df_reduced = df_reduced[df_reduced['token_name']!='LTCRED']
	df_reduced = df_reduced[df_reduced['token_name']!='PNY']
	df_reduced = df_reduced[df_reduced['token_name']!='ROCK']
	df_reduced = df_reduced[df_reduced['token_name']!='eBTCS']
	df_reduced = df_reduced[df_reduced['token_name']!='GNTW']
	df_reduced = df_reduced[df_reduced['token_name']!='KITTEN']
	df_reduced = df_reduced[df_reduced['token_name']!='COT']
	df_reduced = df_reduced[df_reduced['token_name']!='BITINDIA']
	df_reduced = df_reduced[df_reduced['token_name']!='BOLD']
	df_reduced = df_reduced[df_reduced['token_name']!='FLLWOLD']
	df_reduced = df_reduced[df_reduced['token_name']!='CSOLD']
	df_reduced = df_reduced[df_reduced['token_name']!='LCWP']
	df_reduced = df_reduced[df_reduced['token_name']!='EBYTE']
	df_reduced = df_reduced[df_reduced['token_name']!='BTCL']
	df_reduced = df_reduced[df_reduced['token_name']!='EQC']
	df_reduced = df_reduced[df_reduced['token_name']!='DVN']
	df_reduced = df_reduced[df_reduced['token_name']!='EPOSY']

	df = df.drop(columns=['token_decimal'])

	df_reduced.to_pickle('data/df_reduced.pkl')
	print 'Saved to data/df_reduced.pkl'

	vcn = pd.DataFrame(df_reduced['token_name'].value_counts(normalize=True))
	vcn.reset_index(level=0, inplace=True)
	vcn['index'].to_pickle('data/tokens_by_frequency.pkl')
	print 'Saved to data/tokens_by_frequency.pkl'

def get_price_histohour(fsym, tsym='ETH'):
	call = 'https://min-api.cryptocompare.com/data/histohour?fsym=$1&tsym=$2&limit=$3&toTs=$4'
	call = call.replace('$1',fsym).replace('$2',tsym)

	call_1 = call.replace('$3',l123).replace('$4',ts1)
	call_2 = call.replace('$3',l123).replace('$4',ts2)
	call_3 = call.replace('$3',l123).replace('$4',ts3)
	call_4 = call.replace('$3',l4).replace('$4',ts4)

	p1 = json.loads(urllib.urlopen(call_1).read())
	p2 = json.loads(urllib.urlopen(call_2).read())
	p3 = json.loads(urllib.urlopen(call_3).read())
	p4 = json.loads(urllib.urlopen(call_4).read())

	data = p4['Data'][:-1]
	data = data + p3['Data'][:-1]
	data = data + p2['Data'][:-1]
	data = data + p1['Data']

	# volumeto is the amount fof fsym, volumefrom is the amount of tsym

	df_res = pd.DataFrame.from_dict(data)

	df_res['price'] = df_res['volumeto']/df_res['volumefrom']

	if fsym=='ETH' and tsym=='USD':
		df_res['volume_ETH'] = df_res['volumefrom']
		df_res['volume_USD'] = df_res['volumeto']
	elif tsym=='ETH':
		df_eth_usd = pd.read_pickle('data/prices/df_eth_usd.pkl')
		df_res['volume_ETH'] = df_res['volumeto']
		df_res['volume_USD'] = df_res['volume_ETH'].multiply(df_eth_usd['price'])
		df_res['volume_token'] = df_res['volumefrom']
	else:
		print 'Someting wrong with fsym or tsym'

	df_res = df_res.drop(columns=['volumefrom', 'volumeto'])

	return df_res

def download_eth_usd():

	file_name = 'data/prices/df_eth_usd.pkl'
	df_eth_usd = get_price_histohour('ETH', 'USD')
	df_eth_usd.to_pickle(file_name)
	print 'Saved to ',file_name

def get_price_file_list(i=10):
	res = []
	tokens_by_frequency = pd.read_pickle('data/tokens_by_frequency.pkl')
	for t in tokens_by_frequency[:i]:
		tl = t.lower()
		file_name = 'data/prices/df_'+tl+'_eth.pkl'
		res.append(file_name)
	return res

def download_token_eth(token_list):

	for t in token_list:

		tl = t.lower()
		file_name = 'data/prices/df_'+tl+'_eth.pkl'

		df_t = get_price_histohour(t)
		df_t.to_pickle(file_name)
		print 'Saved to ',file_name



tokens_by_frequency = pd.read_pickle('data/tokens_by_frequency.pkl')
print tokens_by_frequency[tokens_by_frequency=='EGOLD'].index[0]
#print tokens_by_frequency[602]
#print tokens_by_frequency[603]

download_token_eth(tokens_by_frequency[175:])

#reduce_main_df()















'''
df_reduced = pd.read_pickle('data/df_reduced.pkl')

vcn = pd.DataFrame(df_reduced['token_name'].value_counts(normalize=True))
vc = pd.DataFrame(df_reduced['token_name'].value_counts())
vcn.reset_index(level=0, inplace=True)
vc.reset_index(level=0, inplace=True)
vcn['CDF'] = [sum(vcn['token_name'][:x]) for x in range(len(vcn['token_name']))]

vcn['index'].to_pickle('data/tokens_by_frequency.pkl')
#print vcn[vcn['CDF']<.90]
#print vc[vc['token_name']>500]

'''











'''
alt_tokens = ['PLU','1ST','ARC','TIME','NXC','HKG','SWT','MKROLD','VSL','ICN','DGD',\
				'SNGLS','GNTW','EDG','MLN','EMV','RHOC','XAUR','RLC','TRST','WINGS','LUN',\
				'TAAS','TKN','HMQ','DICE','ANT','GUP','BAT','VERI','BCAP','VSMOLD','MGO',\
				'PTOY','MYST','ETH','CFI','SONM','GNO','BNT','GOOD','ICE','STORJ','FUNOLD',\
				'MBRS','RARE','E4ROW','ADT','SNT','NMR','MCAP','ADX','EOS','DCN','LNK','PLBT',\
				'PAY','XRL','NET','PPT','NEWB','OMG','BNB','SAN','FUCKOLD','BCD','BTH','MTL',\
				'PLR','FUN','CVC','MSP','CAT','SKIN','BRAT','IXT','PRO','MCD','CDT','SHOUC',\
				'TIX','NXXOLD','BET','DEX','OAX','BAS','TFL','DDF','STRC','DNT','MAYN','STX',\
				'BQX','CREDO','SND','LRC','FYN','ECN','CTR','CLRT','NIH','NDC','DENT','GUNS',\
				'DEL','YC','KTN','WTT','REP','LUCK','OHNI','ZRX','MyB','MCO','BLX','BENJA',\
				'IDEA','ADST','IFT','KZN','FLIK','RIYA','PT','DALC','TNT','TOV','VEN','OPT',\
				'FC','REX','SUB','MAYY','POE','QAU','EMT','SNC','HVN','ATT','FUCK_OLD','MPRM',\
				'AIRA','EPOSY','ETBS','ELIX','WTC','CCT','TRX','SQRL','BITS','BOPTOLD','WILD',\
				'AE','AVT','MTH','MDA','SAI','AUA','DLT','BOP','BXC','CASH','TPT','PST','BTM',\
				'SWFTC','FLIP','PGL','HBT','PPT2','GXC','R','FAM','JET','IND','JADE','BULX',\
				'KING','PRS','DOM','FAP','SCL','MRV','MANA','ETHB','POS','YESTERDAY','TKR',\
				'EBET','CTCOLD','1LIFE','ORME','SGG','SIFT','ATS','ARENA','BTE','ALIS','COB',\
				'PIX','BMC','DAY','VOISE','ROC','LINK','CNX','RSPR','GGS','ALPC','ATM','TFT',\
				'GOOC','IQT','HOWL','XAI','BTN','CSNO','VIBE','WIC','ICOS','RVT','XNN','KNC',\
				'RPL','MCI','SDRN','ZSC','COSSOLD','UMC','TRV','KIN','SPNK','XHY','LLT','SALT',\
				'TEU','SHT','UGT','SHIT','ITT','INXT','CLD','DTC','LDM','BCO','BMT','FNL','FRD',\
				'POW','SWP','KICK','EXN','NTC','CDX','NUGD','PLY','ONEK','PME','VIB','RLX','ELTC3',\
				'EXRP2','KSS','DET','REAL','ELTC2','RENT','IWT','EBCSH','EBCC','SLIP','HGT',\
				'RUB','ARXAI','BKB','ELITE','EZEC2','NIMFA','XPA','PPP','MOL','MVC','EVX','EXRN',\
				'AIR','ECASH','NCH','LLA','AION','ENG','MOAC','EETH','CND','MDT','EGOLD','ETG',\
				'EBTGOLD','ELTC','RCC','EVC','SPARTA','ETHG','ACC','ART','BTCM','KEY','IBTCOLD',\
				'EUSD','10MT','ITS','BCPT','CM','STU','FIT','LUM','EDOGE','EDO','ENO','IETH',\
				'POOL','EREAL','CAG','ETHP','AST','WRC','HDG','BTH2','DRT','KRT','LMT','TIOTOUR',\
				'TIE','STH','ALIEF','OTN','CARE','BLUE','EURB','SHLD','7YPE','REQ','POLL',\
				'GEN','ATMT','FUDD','ETV','ETHD','DBETOLD','EXMR','BTCRED','NULS','MOD','BITC',\
				'QVT','BBD','EBIT','JS','GFL','LTG','ETL','AMB','BNN','ARTE','ESMS','CODE',\
				'PUMP','DEEP','SOAR','EAGLE','PRG','MXX','LA','RCN','NTWK','MLK','SUB1X',\
				'BB','FYP','IBTG','ADUOLD','DRP2','BUS','BQ','GMT','BTCS','0ED','READ',\
				'BITCOINEREUM','ERC20','AMO','CTT','HATOLD','MSC','NEBO','CZT','CTX','XBL',\
				'LIFE','TMP','1BIT','CCP','DOVU','LAP','JDI','GIM','CL','SOCIAL','ASTRO',\
				'EGASOLD','10MTI','MPESA','YEL','EBTG','FUEL','ZAP','BTCE','SMART','NEOG',\
				'HAO','GRID','ELUNCH','SAT','IPY','POWR','EBTC','SNIP','ENJ','ELTCOIN','ZCG',\
				'GTA','ATL','FBR','WETOLD','CCB','LCASH','WLK','TDT','BTCQ','ALCO','TWIT',\
				'SCX','PAYX','NIO','DGPT','AGO','CSL','WNDOLD','TRIA','DATA','LGR','BLN',\
				'ARN','CRTM','XUC','EBCH','GRX','BTSE','UKG','GBTS','IGN','T8C','CHUCK',\
				'DEEM','MVT','BCOIN','BOTA','CFD','HYTV','GRMD','ETHCEN','CSOLD','SWM',\
				'RDN','BTC2X','BITINDIA','SPOON','TPL','EPY','ARCT','GNEISS','DRP','EETHER',\
				'QBE','HST','UFR','BTCP','DATL','LIRA','PLC','GEC','KITTEN','MNTP',\
				'UETL','DJC','RIPT','DCL','VIU','DBET','DVD','STCN','HELP','BRC','ETHX',\
				'BBT','ABSOLD','NAO','PNKOLD','DHG','INDIOLD','TSS','FBL','SSS',\
				'PRL','EALP','DEER','JWT','MINT','GVT','LCC','EBYTE','FDM','ENTRC',\
				'ONG','RC','ETBT','PRIX','TRASH','SNI','EGAS','LOC','H2O','B2XOLD',\
				'SGR','BTHE','ETHC','DNA','RPIL','KMR','SISA','ROCK','HUB','BZX','MGX',\
				'COV','CLASH','B2B','MLD','MTX','CLASSY','SPANK','QSP','1KT','STAKE',\
				'BGIFT','MEDI','CANADA','B2X','ZDR','ALTS','ssn','HQX','BALI','PBL',\
				'CDRT','BOLD','DIVX','HDLT','PNY','VEE','FLIXX','AMM','ELYTE','LCT',\
				'WAND','PCC','WISH','GOAL','AI','eBTCS','SMS','UAHOLD','WND','WET',\
				'WBA','RAC','DVN','INT','DRPU','COT','LEND','XMRG','KEN','CCO','CRYPHER',\
				'SNOV','DRGN','ERO','ASTR','ULT','PFR','PKT','BCDC','TBT','REBL','LCWP',\
				'ALLY','PRFT','LTCRED','ABS','ADU','STAR','INDI','UAHPAY','BULLISH','NXX',\
				'SKR','BON','PXS','FDX','STORM','BTCL','XSC','XMAS','SCT','1WO','LNC','SHNZ',\
				'CMT','DTR','PXT','BCR','WABI','ACE','LEV','SXDT','GREED','DAT','SXUT',\
				'CRED','HKN','CAPT','EQC','AIX','FLLWOLD','BNTY','PAL','NEU','ICX','SEXY',\
				'ETHPR','eGO','DAI','DMT','EQL']
print len(alt_tokens)
'''

print("--- %s seconds ---" % (time.time() - start_time))