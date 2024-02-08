import requests
import sqlite3
import json
import os
import pymarc
import io
import re
import time
from urllib.parse import unquote
import datetime
import xml.etree.ElementTree as ET
import random
from collections import Counter


from pathlib import Path

# load all the wikibaseintegrator modules
from wikibaseintegrator import WikibaseIntegrator
from wikibaseintegrator.wbi_config import config as wbi_config
from wikibaseintegrator import wbi_login
from wikibaseintegrator import datatypes
from wikibaseintegrator.models import Reference, References
from wikibaseintegrator.models.qualifiers import Qualifiers


# the credentials are stored in a .gitignore'ed file called creds.json, look at the creds_example.json to see how to add your OAUTH2 info
try:
	creds = json.load(open('creds.json'))
	consumer_token = creds['consumer_token']
	consumer_secret = creds['consumer_secret']
	report_output = creds['report_output']
except Exception as e:
	print("Could not load/parse the creds.json file that holds the consumer_token and consumer_secret for Wikidata")
	print(e)
	os._exit(0)


try:
	login_instance = wbi_login.OAuth2(consumer_token=consumer_token, consumer_secret=consumer_secret)
except Exception as e: 
	print('Failed to log in using the credentials provided:', e)
	os._exit(0)

# the user agent we are using for these operations in wikibaseintegrator and requests
wbi_config['USER_AGENT'] = 'LCNNBot/1.0 (https://www.wikidata.org/wiki/User:LCNNBot)'
headers={"user-agent":'LCNNBot/1.0 (https://www.wikidata.org/wiki/User:LCNNBot)'}

# initate wikibaseintegrator / log in
# wbi = WikibaseIntegrator(login=login_instance,is_bot=True)
wbi = WikibaseIntegrator(login=login_instance,is_bot=True)

#today as a a string
today_string = datetime.datetime.today().strftime('%Y-%m-%d')
start_time_string = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')





# some functions we are using
# ----

# make / load the sqlite3 db
def connect_to_database():
	# look to see if it can find the id database
	db_connection = None
	if Path("ids.sqlite3").is_file():
		db_connection = sqlite3.connect("ids.sqlite3")
	else:
		db_connection = sqlite3.connect("ids.sqlite3")
		db_connection.execute("CREATE TABLE ids(key, lccn, timestamp)")

	return db_connection

# extract the qid from the MARC field, expecting a string
def extract_wikidata(field):
	reg_results = re.findall(r'wikidata\.org/.*/Q[0-9]+',str(field))
	if len(reg_results) == 1:
		return reg_results[0].split('/')[-1]
	else:
		return False

# extract the viaf from the MARC field, expecting a string
def extract_viaf(field):
	reg_results = re.findall(r'viaf/[0-9]+',str(field))
	if len(reg_results) == 1:
		return reg_results[0].split('/')[-1]
	else:
		return False

# delete records that are older than a month
def prune(db):

	one_month_ago = int(time.time()) - 2_592_000


	cur = db.cursor()

	cur.execute(f"SELECT * FROM ids WHERE timestamp < {one_month_ago}")
	result = cur.fetchall()
	print("Deleting",len(result), 'old records.')

	cur.execute(f"DELETE FROM ids WHERE timestamp < {one_month_ago}")
	db.commit()



# pass the event log and the where to output the reports
def build_report(events,output_dir):

	# count the occurances and store it as dict
	c = Counter()
	for item in events:
		c[item["action"]] += 1
	c = dict(c)


	# setup the XML lib
	ET.register_namespace('log',"info:lc/lds-id/log")
	ET.register_namespace('mets',"http://www.loc.gov/METS/")

	end_time_string = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')


	mets_collection = ET.Element("{http://www.loc.gov/METS/}collection")
	mets = ET.SubElement(mets_collection,"{http://www.loc.gov/METS/}mets")
	mets.set('OBJID','/loads/lccnbot/'+today_string+'.xml')

	metsHdr = ET.SubElement(mets,"{http://www.loc.gov/METS/}metsHdr")
	metsHdr.set('LASTMODDATE',start_time_string)

	dmdSec = ET.SubElement(mets,"{http://www.loc.gov/METS/}dmdSec")
	dmdSec.set('ID','logxml')

	mdWrap = ET.SubElement(dmdSec,"{http://www.loc.gov/METS/}mdWrap")
	mdWrap.set('MDTYPE','OTHER')

	xmlData = ET.SubElement(mdWrap,"{http://www.loc.gov/METS/}xmlData")


	log_root = ET.SubElement(xmlData,"{info:lc/lds-id/log}load")

	log_root.attrib['source'] = 'LccnBot'
	log_root.attrib['start'] = start_time_string
	log_root.attrib['end'] = end_time_string

	available_actions = ['MULTI_LCCN_IN_WIKI',	'NEED_REVIEW',	'VIAF_SUGGESTION',	'ADD_P244',	'NAMED_AS_CHANGE',	'NAMED_AS_ADDED']

	for action in available_actions:
		if action not in c:
			c[action] = 0

	log_load_type = ET.SubElement(log_root, "{info:lc/lds-id/log}loadType")
	log_load_type.text = "every day"
	log_msg = ET.SubElement(log_root, "{info:lc/lds-id/log}msg")
	log_msg.text = f"LccnBot. New P244: {c['ADD_P244']}, Named_as added: {c['NAMED_AS_ADDED']}, Named_as changed: {c['NAMED_AS_CHANGE']}, Need Review: {c['NEED_REVIEW']}, Multiple Qid for one LCCN: {c['MULTI_LCCN_IN_WIKI']}, VIAF Suggestion: {c['VIAF_SUGGESTION']} "

	log_log_details = ET.SubElement(log_root, "{info:lc/lds-id/log}logDetails")

	for action in available_actions:
		for e in events:			
			if e['action'] == action:
				


				log_details_item = ET.SubElement(log_log_details, "{info:lc/lds-id/log}logDetail")

				log_details_item.set('lccn',e['lccn'])
				log_details_item.set('qid',e['qid'])
				log_details_item.set('action',e['action'])
				log_details_item.set('old',e['old'])
				log_details_item.set('new',e['new'])




	Path(f"{output_dir}/").mkdir(parents=True, exist_ok=True)
	ET.ElementTree(mets_collection).write(f"{output_dir}/{today_string}.xml", encoding='utf8')
	mets.set('OBJID','/loads/lccnbot/latest.xml')
	ET.ElementTree(mets_collection).write(f"{output_dir}/latest.xml", encoding='utf8')





# ----


db = connect_to_database()
db_crsr = db.cursor()
log = []
log_writes = []


full_page_complete_count = 0	# keeps track of how many API response pages have already been marked as finished in the DB

# go back 50 pages by default
# for use_page_number in range(3286,3500):
for use_page_number in range(1,50):


	page_complete = True
	feedurl = f"https://id.loc.gov/authorities/names/activitystreams/feed/{use_page_number}.json?nocache{random.random()}"
	print("PAGE", use_page_number, feedurl)
	lccns_to_check_wikidata_count = []

	data = requests.get(feedurl)
	try:
		data = json.loads(data.text)
	except:
		print("JSON decode error:",data.text)
		print("Sleeping 60 sec")
		time.sleep(60)
		print("Trying again...")
		data = requests.get(feedurl)
		data = json.loads(data.text)


	to_check = []

	# build a dict for each item to check 
	for rec in data['orderedItems']:
		lccn = rec['object']['id'].split("/")[-1]
		uri = rec['object']['id']
		url = uri.replace("http://","https://")
		date_pub = rec['published']
		date_update = rec['object']['update']
		marcurl = rec['object']['id'] + '.marcxml.xml'
		marcurl = marcurl.replace("http://","https://")
		db_id = f"{lccn}-{date_pub}-{date_update}"

		db_crsr.execute(f"SELECT * FROM ids WHERE key = '{db_id}'")
		result = db_crsr.fetchall()
		
		# if we have that ID in the DB it means it was checked/worked in a previoud run, so skip it
		if len(result) == 0:
			# otherwise not found, build a dict for it
			page_complete = False
			to_check.append({'lccn':lccn,'db_id':db_id,'marcurl':marcurl, 'uri': uri, 'url':url})
		else:
			# we already checked this one, keep moving
			print("skipping",lccn)
			continue


	# grab each XML blob and see if it has a wikidata URL in it, if so then pull it out
	for l in to_check:

		# load the record from id.loc.gov
		try:
			xml = requests.get(l['marcurl'],headers=headers)
			xmltext = xml.text

			# with open("/Volumes/BibGlum/marc_lccn_wikibot/"+l['lccn']+'.xml','w') as tmpmarc:
			# 	tmpmarc.write(xmltext)


			# time.sleep(1)

		except Exception as e: 
			print('Faild to download the XML from id.loc.gov:', l['lccn'], e)
			continue

		# try to parse it by making a stringIO object and putting the XML into it
		try:
			with io.StringIO() as f:
				f.write(xmltext)
				f.seek(0)
				# parse it, its returns a list of records, but we only have one, so take the 0 index
				record = pymarc.marcxml.parse_xml_to_array(f)[0]
		except Exception as e: 
			print('Faild to parse the XML from id.loc.gov:', l['lccn'], e)
			continue

		wiki_id = False
		viaf_id = False

		# check for wikidata in the fields
		for field in record.get_fields():
			if 'wikidata.org' in str(field):
				# look in 024
				if '=024' in str(field):
					wiki_id = extract_wikidata(field)
					if wiki_id != False:
						print("Found wiki id in 024:", str(field))
						

				if '=670' in str(field):
					if 'u' in field:
						wiki_id = extract_wikidata(field)
						if wiki_id != False:
							print("Found wiki id in 670$u:", str(field['u']), "wiki_id:",wiki_id)
							
						
			if 'viaf.org' in str(field):
				# look in 024
				if '=024' in str(field):
					viaf_id = extract_viaf(field)						

		# if things change or they start showing up in different part of the records keep track of it
		if wiki_id == False and 'wikidata' in str(record):
			print("wikidata pattern miss", record)


		if wiki_id == False:
			# add it to the DB as not needing our attention again
			sql = ''' INSERT INTO ids(key, lccn, timestamp)
					  VALUES(?,?,?) '''

			db_crsr.execute(sql, ( l['db_id'], l['lccn'], int(time.time())))
			db.commit()

			# no wikidata but did it have a VIAF?
			if viaf_id != False:	
				viaf_links={}
				try:
					viaf_req = requests.get(f"https://viaf.org/viaf/{viaf_id}/justlinks.json",headers=headers)
					viaf_links = viaf_req.json()
				except:
					print("Viaf commmuication ERROR")
					print(viaf_req.text)

				if isinstance(viaf_links, dict):

					if 'WKP' in viaf_links and 'LC' in viaf_links:

						# it has a viaf and wikidata ID, check wikidata and see if it already has a P244 if so skip the suggestion
						viaf_qid = viaf_links['WKP'][0]
						try:
							wiki_item = wbi.item.get(entity_id=viaf_qid)
						except Exception as e: 
							print('Faild to find Wikidata item:', viaf_qid, l['lccn'], e)

							# it might be a token timeout issue
							try:
								login_instance = wbi_login.OAuth2(consumer_token=consumer_token, consumer_secret=consumer_secret)
								wbi = WikibaseIntegrator(login=login_instance,is_bot=True)
								wiki_item = wbi.item.get(entity_id=viaf_qid)
							except Exception as e: 

								continue
						p244 = []

						try:
							p244 = wiki_item.claims.get('P244')
						except:
							pass

						if len(p244) == 0:
							log_writes.append({'lccn':l['lccn'],'qid':",".join(viaf_links['WKP']),'action':"VIAF_SUGGESTION","old":viaf_id,'new':''})

		else:


			# we will likely need to know the pref label, so grab that from id.loc.gov, this is easier than tying to recontruct it ourselves
			try:
				pref_req = requests.head(l['url'])
				l['pref'] = unquote(pref_req.headers['X-PrefLabel-Encoded'])

			except Exception as e: 
				print('Faild to grab the pref label from id.loc.gov:', l['lccn'], e)
				continue

			# grab the wikidata item
			try:
				wiki_item = wbi.item.get(entity_id=wiki_id)
			except Exception as e: 
				print('Faild to find Wikidata item:', wiki_id, l['lccn'], e)
				try:
					login_instance = wbi_login.OAuth2(consumer_token=consumer_token, consumer_secret=consumer_secret)
					wbi = WikibaseIntegrator(login=login_instance,is_bot=True)
					wiki_item = wbi.item.get(entity_id=wiki_id)
				except Exception as e: 

					continue

				

			try:
				p244 = wiki_item.claims.get('P244')
			except:
				p244 = []


			if len(p244) > 0:
				# it already has a p244 claim
				has_lccn = False
				for c in p244:
					# loop through all of them, is it the same one we would be trying to add?
					if c.mainsnak.datavalue['value'].lower() == l['lccn'].lower():
						has_lccn = True
						# already has the same P244 lccn
						# does it already have a named as
						if len(c.qualifiers.get('P1810')) > 0:

							# print("Already has subaged named as", l, wiki_id)
							# print(f"www.wikidata.org/entity/{wiki_id}")

							# it might be a different named_subject_as value, if so we need to update it with a new value
							for q in c.qualifiers.get('P1810'):
								if q.datavalue['value'].strip() != l['pref']:
									old_value = q.datavalue['value']
									q.datavalue['value'] = l['pref']
									try:
										wiki_item.write(summary='Updating the subject named as to LCCN authorized heading value')
										log_writes.append({'lccn':l['lccn'],'qid':wiki_id,'action':"NAMED_AS_CHANGE","old":str(old_value),'new':l['pref']})

									except:
										log_writes.append({'lccn':l['lccn'],'qid':wiki_id,'action':"NEED_REVIEW","old":"",'new':"Error changing named_as."})

									break

							# update database marking it done
							sql = ''' INSERT INTO ids(key, lccn, timestamp)
							VALUES(?,?,?) '''
							db_crsr.execute(sql, ( l['db_id'], l['lccn'], int(time.time())))
							db.commit()							


						else:
							# print(f"www.wikidata.org/entity/{wiki_id}")
							# # doesn't have named_as yet
							# print(l)
							# print(c.get_json())
							# print(c.qualifiers)
							# print(json.dumps(c.get_json(),indent=2))
							c.qualifiers.add(datatypes.String(prop_nr='P1810', value=l['pref']))
							# print("AFTER------")
							# print(json.dumps(c.get_json(),indent=2))

							try:
								wiki_item.write(summary='Add authorized heading for P244 Library of Congress LCCN subject named as')
								log_writes.append({'lccn':l['lccn'],'qid':wiki_id,'action':"NAMED_AS_ADDED","old":"",'new':l['pref']})

							except:
								log_writes.append({'lccn':l['lccn'],'qid':wiki_id,'action':"NEED_REVIEW","old":"",'new':"Error adding named_as."})

							# update database marking it done
							sql = ''' INSERT INTO ids(key, lccn, timestamp)
							VALUES(?,?,?) '''
							db_crsr.execute(sql, ( l['db_id'], l['lccn'], int(time.time())))
							db.commit()		


					else:

						# it has a different LCCN than what we are trying to add, so we need to also add ours as well and leave the old one alone
						# we will mark it in the report (TODO) to flag it for something that needs review
						# handled in has_lccn below
						pass

				if has_lccn == False:
					# this doesn't seem to happen, i need to find an example before coding it, so just add it to the log file now
					print("Has a LCCN but not the one we are trying to add!")
					log_writes.append({'lccn':l['lccn'],'qid':wiki_id,'action':"NEED_REVIEW","old":"",'new':"LCCN already exist but not the one we are trying to add."})
					# update database marking it done
					sql = ''' INSERT INTO ids(key, lccn, timestamp)
							  VALUES(?,?,?) '''
					db_crsr.execute(sql, ( l['db_id'], l['lccn'], int(time.time())))
					db.commit()		


			else:


				# print("Doesn't have P244 at all yet")
				# print(l)
				# print(f"www.wikidata.org/entity/{wiki_id}")



				claim_qualifiers = Qualifiers()
				claim_qualifiers.add(datatypes.String(prop_nr='P1810', value=l['pref']))

				claim_references = References()  # Create a group of references
				claim_reference1 = Reference()

				# stated in and date retrieved, Q18912790 = LC authority file
				claim_reference1.add(datatypes.Item(prop_nr='P248', value='Q18912790'))
				claim_reference1.add(datatypes.Time(prop_nr='P813', time="+" + datetime.datetime.now().replace(microsecond=0).isoformat().split("T")[0]+'T00:00:00Z'))

				claim_references.add(claim_reference1)

				lccn_id_claim = datatypes.ExternalID(value=l['lccn'], prop_nr='P244', qualifiers=claim_qualifiers, references=claim_references)



				wiki_item.claims.add(lccn_id_claim)
				

				try:
					wiki_item.write(summary='Add P244 Library of Congress LCCN External Identifier')
					log_writes.append({'lccn':l['lccn'],'qid':wiki_id,'action':"ADD_P244","old":'','new':''})

				except:
					log_writes.append({'lccn':l['lccn'],'qid':wiki_id,'action':"NEED_REVIEW","old":"",'new':"Script Error when adding P244."})

				break


				lccns_to_check_wikidata_count.append(l['lccn'])
				# update database marking it done
				sql = ''' INSERT INTO ids(key, lccn, timestamp)
				VALUES(?,?,?) '''
				db_crsr.execute(sql, ( l['db_id'], l['lccn'], int(time.time())))
				db.commit()		

				# also ask 


	# give wikidata sparql endpoint a few seconds to catch up before we query
	time.sleep(5)


	# for each lccn write a sparql that looks for 
	# the P244 and if it returns > 1 then make a log entry for it
	for lccn in lccns_to_check_wikidata_count:

		headers_wiki = {
			'Accept' : 'application/json',
			'User-Agent': 'LCNNBot/1.0 (https://www.wikidata.org/wiki/User:LCNNBot)'
		}
		sparql = f"""
		  SELECT *
		  WHERE {{
			?item wdt:P244 "{lccn}" .
		  }}
		"""
		params = {
			'query' : sparql
		}

		r = requests.get("https://query.wikidata.org/sparql", params=params, headers=headers_wiki)
		data = r.json()
		multi_qid = []
		if len(data['results']['bindings']) > 0:
			for result in data['results']['bindings']:
				multi_qid.append(result['item']['value'].split('/')[-1])


		
		if len(multi_qid) > 1:

			log_writes.append({'lccn':lccn,'qid':",".join(multi_qid),'action':"MULTI_LCCN_IN_WIKI","old":"",'new':"LCCN Used in multiple Qids"})


	lccns_to_check_wikidata_count = []

	# with open('tmp.log','w') as logout:
	# 	for l in log_writes:
	# 		logout.write(f'<logevent date="{datetime.datetime.now()}" lccn="{l["lccn"]}" qid="{l["qid"]}" action="{l["action"]}" old="{l["old"]}" new="{l["new"]}">{datetime.datetime.now()} / {l["action"]} - {l["lccn"]} - {l["qid"]}: old:{l["old"]} new: {l["new"]} </logevent>\n')
	# 		json.dump(log_writes,open("events.json",'w'),indent=2)
	build_report(log_writes,report_output)





prune(db)












