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
except:
	print("Could not load/parse the creds.json file that holds the consumer_token and consumer_secret for Wikidata")
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
wbi = WikibaseIntegrator(login=login_instance,is_bot=True)


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

# delete records that are older than a month
def prune(db):

	one_month_ago = int(time.time()) - 2_592_000


	cur = db.cursor()

	cur.execute(f"SELECT * FROM ids WHERE timestamp < {one_month_ago}")
	result = cur.fetchall()
	print("Deleting",len(result), 'old records.')

	cur.execute(f"DELETE FROM ids WHERE timestamp < {one_month_ago}")
	db.commit()


# ----


db = connect_to_database()
db_crsr = db.cursor()
log = []
log_writes = []


full_page_complete_count = 0	# keeps track of how many API response pages have already been marked as finished in the DB

# go back 50 pages by default
for use_page_number in range(1,50):

	page_complete = True
	feedurl = f"https://id.loc.gov/authorities/names/activitystreams/feed/{use_page_number}.json"
	print("PAGE", use_page_number, feedurl)

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
		# check for wikidata in the fields
		for field in record.get_fields():
			if 'wikidata.org' in str(field):
				# look in 024
				if '=024' in str(field):
					wiki_id = extract_wikidata(field)
					if wiki_id != False:
						print("Found wiki id in 024:", str(field))
						break

				if '=670' in str(field):
					if 'u' in field:
						wiki_id = extract_wikidata(field)
						if wiki_id != False:
							print("Found wiki id in 670$u:", str(field['u']), "wiki_id:",wiki_id)
							break
						

		# if things change or they start showing up in different part of the records keep track of it
		if wiki_id == False and 'wikidata' in str(record):
			print("wikidata pattern miss", record)


		if wiki_id == False:
			# add it to the DB as not needing our attention again
		    sql = ''' INSERT INTO ids(key, lccn, timestamp)
		              VALUES(?,?,?) '''

		    db_crsr.execute(sql, ( l['db_id'], l['lccn'], int(time.time())))
		    db.commit()

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
							for q in c.qualifiers:
								if q.datavalue['value'].strip() != l['pref']:
									log_writes.append({'lccn':l['lccn'],'qid':wiki_id,'action':"NAMED_AS_CHANGE","old":str(q.datavalue['value']),'new':l['pref']})
									q.datavalue['value'] = l['pref']
									wiki_item.write(summary='Updating the subject named as to LCCN authorized heading value')
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

							c.qualifiers.add(datatypes.String(prop_nr='P1810', value=l['pref']))
							wiki_item.write(summary='Add authorized heading for P244 Library of Congress LCCN subject named as')
							log_writes.append({'lccn':l['lccn'],'qid':wiki_id,'action':"NAMED_AS_ADDED","old":"",'new':l['pref']})
							
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
				
				wiki_item.write(summary='Add P244 Library of Congress LCCN External Identifier')

				log_writes.append({'lccn':l['lccn'],'qid':wiki_id,'action':"ADD_P244","old":'','new':''})

				# update database marking it done
				sql = ''' INSERT INTO ids(key, lccn, timestamp)
				VALUES(?,?,?) '''
				db_crsr.execute(sql, ( l['db_id'], l['lccn'], int(time.time())))
				db.commit()		




for l in log_writes:
	print(l)


prune(db)












