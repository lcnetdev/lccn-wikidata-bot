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

from wikibaseintegrator import WikibaseIntegrator
from wikibaseintegrator.wbi_config import config as wbi_config
from wikibaseintegrator import wbi_login
from wikibaseintegrator import datatypes

from wikibaseintegrator.models import Reference, References
from wikibaseintegrator.models.qualifiers import Qualifiers


wbi = WikibaseIntegrator()

try:
	creds = json.load(open('creds.json'))
	consumer_token = creds['consumer_token']
	consumer_secret = creds['consumer_secret']
except:
	print("Could not load/parse the creds.json file that holds the consumer_token and consumer_secret for Wikidata")
	os._exit(0)


try:
	login_instance = wbi_login.OAuth2(consumer_token=consumer_token, consumer_secret=consumer_secret)
except Exception as e: # work on python 3.x
    print('Failed to log in using the credentials provided:', e)
    os._exit(0)


wbi_config['USER_AGENT'] = 'LCNNBot/1.0 (https://www.wikidata.org/wiki/User:LCNNBot)'
headers={"user-agent":'LCNNBot/1.0 (https://www.wikidata.org/wiki/User:LCNNBot)'}

wbi = WikibaseIntegrator(login=login_instance)


# my_first_wikidata_item = wbi.item.get(entity_id='Q5')

# # print(my_first_wikidata_item.get_json())
# print(login_instance)

# ----


def connect_to_database():
	# look to see if it can find the id database
	db_connection = None
	if Path("ids.sqlite3").is_file():
		db_connection = sqlite3.connect("ids.sqlite3")
	else:
		db_connection = sqlite3.connect("ids.sqlite3")
		db_connection.execute("CREATE TABLE ids(key, lccn, timestamp)")

	return db_connection

def extract_wikidata(field):
	reg_results = re.findall(r'wikidata\.org/.*/Q[0-9]+',str(field))
	if len(reg_results) == 1:
		return reg_results[0].split('/')[-1]
	else:
		return False

# ----


db = connect_to_database()
db_crsr = db.cursor()
log = []

full_page_complete_count = 0	# keeps track of how many API response pages have already been marked as finished in the DB

for use_page_number in range(1,25):

	page_complete = True
	feedurl = f"https://id.loc.gov/authorities/names/activitystreams/feed/{use_page_number}.json"
	print("PAGE", use_page_number, feedurl)

	data = requests.get(feedurl)
	data = json.loads(data.text)
	to_check = []

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
		
		if len(result) == 0:

			page_complete = False
			to_check.append({'lccn':lccn,'db_id':db_id,'marcurl':marcurl, 'uri': uri, 'url':url})
		else:
			# we already checked this one, keep moving
			print("skipping",lccn)
			continue


	# grab each XML blob and see if it has a wikidata URL in it, if so then pull it out
	for l in to_check:


		try:
			xml = requests.get(l['marcurl'],headers=headers)
			xmltext = xml.text

		except Exception as e: 
			print('Faild to download the XML from id.loc.gov:', l['lccn'], e)
			continue

		# try to parse it
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
					print("Found wiki id in 024:", str(field))
						
				if '=670' in str(field):
					if 'u' in field:
						wiki_id = extract_wikidata(field)
						print("Found wiki id in 670$u:", str(field['u']))
						

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


			# we will likely need to know the pref label, so grab that from ID
			try:
				pref_req = requests.head(l['url'])
				l['pref'] = unquote(pref_req.headers['X-PrefLabel-Encoded'])

			except Exception as e: 
				print('Faild to grab the pref label from id.loc.gov:', l['lccn'], e)
				continue

			# grab the item
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
				has_lccn = False
				for c in p244:

					if c.mainsnak.datavalue['value'].lower() == l['lccn'].lower():
						has_lccn = True
						# already has the same P244 lccn
						if len(c.qualifiers.get('P1810')) > 0:
							pass
							# already has named_as
							# skip
						else:
							# print(f"www.wikidata.org/entity/{wiki_id}")
							# # doesn't have named_as yet
							# print(l)
							# print(c.get_json())

							c.qualifiers.add(datatypes.String(prop_nr='P1810', value=l['pref']))
							# wiki_item.write(summary='Add authorized heading for P244 Library of Congress LCCN subject named as')
							# xxx=xxx

				# has_lccn

			else:


				print("Doesn't have P244 at all yet")
				print(l)
				print(f"www.wikidata.org/entity/{wiki_id}")



				claim_qualifiers = Qualifiers()
				claim_qualifiers.add(datatypes.String(prop_nr='P1810', value=l['pref']))

				claim_references = References()  # Create a group of references

				claim_reference1 = Reference()
				claim_reference1.add(datatypes.Item(prop_nr='P248', value='Q18912790'))

				claim_reference2 = Reference()
				claim_reference2.add(datatypes.Time(prop_nr='P813', time="+" + datetime.datetime.now().replace(microsecond=0).isoformat().split("T")[0]+'T00:00:00Z'))

				claim_references.add(claim_reference1)
				claim_references.add(claim_reference2)

				

				lccn_id_claim = datatypes.ExternalID(value=l['lccn'], prop_nr='P244', qualifiers=claim_qualifiers, references=claim_references)
				print(lccn_id_claim)

				wiki_item.claims.add(lccn_id_claim)
				wiki_item.write(summary='Add P244 Library of Congress LCCN External Identifier')

				xxxx=xxxxx


				pass


			
			





		print(wiki_id)





	# break















