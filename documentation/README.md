![program_flow](./diagrams/program_flow.svg)

#### 1.

The activity feed at id.loc.gov for NACO names starts at https://id.loc.gov/authorities/names/activitystreams/feed/1.json

When a NACO record is created/updated ("Updated / Add") it will show up in this feed. For each LCCN updated there are links to the MARC record.

#### 2.

A local file/sqlite db of previously checked IDs. A unique Id will be the `LCCN` + `date-update`+`date-published` for example `no2022065764-2023-10-18-2023-10-18` most records will have the same update and published date but we can handle multiple updates published on a single day or vice versa. The script will download the feed pages sequentially until it hits a full page of unique ids that have already be completed. The unique ids from that group which have not been marked as completed before will be the LCCNs for that run of the script.

#### 3.

We are checking for the the Wikidata Q ID in two places the `024` in various subfields:

```
  <marcxml:datafield tag="024" ind1="7" ind2=" " xmlns:streams="info:lc/streams#">
    <marcxml:subfield code="a">https://www.wikidata.org/wiki/Q359694</marcxml:subfield>
    <marcxml:subfield code="2">uri</marcxml:subfield>
  </marcxml:datafield>
  ---
  <marcxml:datafield tag="024" ind1="8" ind2=" " xmlns:streams="info:lc/streams#">
    <marcxml:subfield code="1">http://www.wikidata.org/entity/Q737768</marcxml:subfield>
  </marcxml:datafield>
```

Basically anywhere in a 024 that has a Q ID using regular expressions:  `r'wikidata\.org/.*/Q[0-9]+'` or just `r'Q[0-9]+'`

Or in the `670 $u` field

```
  <marcxml:datafield tag="670" ind1=" " ind2=" " xmlns:streams="info:lc/streams#">
    <marcxml:subfield code="a">Wikidata via Wikipedia, August 19, 2019</marcxml:subfield>
    <marcxml:subfield code="b">(instance of: human.....</marcxml:subfield>
    <marcxml:subfield code="u">http://www.wikidata.org/entity/Q25338552</marcxml:subfield>
  </marcxml:datafield>
```



#### 4.

For each ask Wikidata for the json data export for the entity for example: https://www.wikidata.org/wiki/Special:EntityData/Q25338552.json

The properties we are working with in this process are 

**P244** - Library of Congress authority ID - https://www.wikidata.org/wiki/Property:P244

**P1810** - subject named as (qualifier) - https://www.wikidata.org/wiki/Property:P1810

**P248** - stated in (reference) - https://www.wikidata.org/wiki/Property:P248

**P813** - retrieved (reference) - https://www.wikidata.org/wiki/Property:P813

#### 5.

If the Wikidata ID from the MARC record has the same LCCN in the P244 already then the script will check to see if it has a P1810 subject named as if it does then it will check if it is the same if does not have one or is different update the P1810 subject named as to the one from the MARC `100`. This will allow for updating the authorized heading into the P1810 for all records that flow through NACO that are on Wikidata and will allow for updates when the name changes because of life dates, etc.

If the LCCN is not the same it will add it, but not replace any existing data, if there are then two LCCNs in the P244 it will flag that in the report as a problem to be looked at by a person.

#### 6.

When creating the P244 the script will also add the P1810 subject named as using the MARC `100` value, the P248 stated in with the value of https://www.wikidata.org/wiki/Q18912790 and the P813 retrieved with the current date.

A report will be created that will be on a public accessible URL (probably at id.loc.gov) that will document the activities for the run of the script.



