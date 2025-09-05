import urllib.parse
import xml.etree.ElementTree as ET
import json
from typing import Optional, Dict, Any, Tuple, List
import requests


def convert_point_to_degree_string(point_string):
    """
    Converts a point string in the format 'Point(lat,lon)' to a degree string.

    Args:
        point_string (str): The input string, e.g., 'Point(34.1234, -118.5678)'.

    Returns:
        str: The formatted degree string, e.g., '34.1234° N, 118.5678° W'.
    """
    # Remove 'Point(' and ')' and split the coordinates
    coords_str = point_string.replace('Point(', '').replace(')', '')
    if ',' in coords_str:
        longitude_str, latitude_str = coords_str.split(',')
    elif ' ' in coords_str:
        longitude_str, latitude_str = coords_str.split(' ')

    # Convert to float
    latitude = float(latitude_str.strip())
    longitude = float(longitude_str.strip())

    # Determine hemisphere and format
    lat_hemisphere = 'N' if latitude >= 0 else 'S'
    lon_hemisphere = 'E' if longitude >= 0 else 'W'

    # Use absolute values for display and format with degree symbol
    formatted_latitude = f"{abs(latitude):.4f}° {lat_hemisphere}"
    formatted_longitude = f"{abs(longitude):.4f}° {lon_hemisphere}"

    return f"{formatted_latitude}, {formatted_longitude}"

def get_loc_preflabel(url: str) -> Optional[str]:
    """
    Request a Library of Congress URL without following redirects and extract
    the x-preflabel-encoded header value.
    
    Args:
        url: The LOC URL to request (e.g., "https://id.loc.gov/authorities/no2025089213")
        
    Returns:
        The decoded preflabel string if found, None otherwise
    """
    try:
        # Make request without following redirects
        headers = {'User-Agent': 'wikidata user: thisismattmiller / data quality script'}
        response = requests.get(url, headers=headers, allow_redirects=False)
        
        # Get the x-preflabel-encoded header
        preflabel_encoded = response.headers.get('x-preflabel-encoded')
        
        if preflabel_encoded:
            # URL decode the value
            decoded_label = urllib.parse.unquote(preflabel_encoded)
            return decoded_label
        else:
            print(f"No x-preflabel-encoded header found for {url}")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"Error fetching {url}: {e}")
        return None


def parse_loc_xml(url_or_xml: str) -> Tuple[str, Dict[str, Any]]:
    """
    Parse Library of Congress XML data from a URL or XML string.
    
    Args:
        url_or_xml: Either a URL endpoint to fetch XML from, or an XML string
        
    Returns:
        A tuple containing:
        - Text format: Key-value pairs as "Key: Value\n" string (multiple values on separate lines)
        - JSON format: Dictionary of key-value pairs (lists for multiple values)
    """
    # Check if input is a URL or XML string
    if url_or_xml.startswith('http://') or url_or_xml.startswith('https://'):
        # Fetch XML from URL
        try:
            headers = {
                'User-Agent': 'wikidata user: thisismattmiller / data quality script',
                'Accept': 'application/rdf+xml'
            }
            response = requests.get(url_or_xml, headers=headers)
            response.raise_for_status()
            xml_content = response.text
        except requests.exceptions.RequestException as e:
            print(f"Error fetching XML from {url_or_xml}: {e}")
            return "", {}
    else:
        xml_content = url_or_xml
    
    # Parse XML
    try:
        root = ET.fromstring(xml_content)
    except Exception as e:
        print(f"Error parsing XML: {e}")
        return "", {}
    
    # Define namespaces
    namespaces = {
        'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
        'rdfs': 'http://www.w3.org/2000/01/rdf-schema#',
        'madsrdf': 'http://www.loc.gov/mads/rdf/v1#',
        'identifiers': 'http://id.loc.gov/vocabulary/identifiers/',
        'ri': 'http://id.loc.gov/ontologies/RecordInfo#',
        'bflc': 'http://id.loc.gov/ontologies/bflc/',
        'dcterms': 'http://purl.org/dc/terms/',
        'owl': 'http://www.w3.org/2002/07/owl#'
    }
    
    # Store results - using lists to handle multiple values
    result = {}
    
    # Helper function to add values to result
    def add_to_result(key: str, value: Any):
        if value:
            if key not in result:
                result[key] = []
            if isinstance(value, list):
                result[key].extend(value)
            else:
                result[key].append(value)
    
    # Find the main entity (PersonalName, CorporateName, Topic, etc.)
    main_entity = None
    for child in root:
        if any(entity_type in child.tag for entity_type in ['PersonalName', 'CorporateName', 'Topic', 'Geographic', 'Title']):
            main_entity = child
            break
    
    if main_entity is None:
        return "", {}
    
    # Extract entity type
    entity_type = main_entity.tag.split('}')[-1] if '}' in main_entity.tag else main_entity.tag
    add_to_result('Entity Type', entity_type)
    
    # Extract URI
    uri = main_entity.get('{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about')
    if uri:
        add_to_result('URI', uri)
    
    # Extract ALL authoritative labels (including different languages/scripts)
    for auth_label in main_entity.findall('.//madsrdf:authoritativeLabel', namespaces):
        if auth_label.text:
            lang = auth_label.get('{http://www.w3.org/XML/1998/namespace}lang')
            if lang:
                add_to_result(f'Authoritative Label ({lang})', auth_label.text)
            else:
                add_to_result('Authoritative Label', auth_label.text)
    
    # Extract ALL identifiers
    for lccn in main_entity.findall('.//identifiers:lccn', namespaces):
        if lccn.text:
            add_to_result('LCCN', lccn.text)
    
    for local_id in main_entity.findall('.//identifiers:local', namespaces):
        if local_id.text:
            add_to_result('Local ID', local_id.text)
    
    # Extract ALL MARC keys
    for marc_key in main_entity.findall('.//bflc:marcKey', namespaces):
        if marc_key.text:
            lang = marc_key.get('{http://www.w3.org/XML/1998/namespace}lang')
            if lang:
                add_to_result(f'MARC Key ({lang})', marc_key.text)
            else:
                add_to_result('MARC Key', marc_key.text)
    
    # Extract RWO (Real World Object) information
    for rwo in main_entity.findall('.//madsrdf:RWO', namespaces):
        rwo_uri = rwo.get('{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about')
        if rwo_uri:
            add_to_result('RWO URI', rwo_uri)
        
        # Extract types
        for rdf_type in rwo.findall('.//rdf:type', namespaces):
            type_ref = rdf_type.get('{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource')
            if type_ref:
                add_to_result('RDF Type', type_ref)
        
        # Extract labels
        for label in rwo.findall('.//rdfs:label', namespaces):
            if label.text:
                add_to_result('RWO Label', label.text)
        
        # Birth/Death dates
        for birth_date in rwo.findall('.//madsrdf:birthDate', namespaces):
            if birth_date.text:
                add_to_result('Birth Date', birth_date.text)
        
        for death_date in rwo.findall('.//madsrdf:deathDate', namespaces):
            if death_date.text:
                add_to_result('Death Date', death_date.text)
        
        # Birth/Death places
        for birth_place in rwo.findall('.//madsrdf:birthPlace', namespaces):
            place_ref = birth_place.get('{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource')
            if place_ref:
                if 'id.loc.gov/authorities' in place_ref:
                    place_label = get_loc_preflabel(place_ref)
                    if place_label:
                        add_to_result('Birth Place', f"{place_label} ({place_ref})")
                    else:
                        add_to_result('Birth Place', place_ref)
                else:
                    add_to_result('Birth Place', place_ref)
            else:
                # Check for nested Geographic element
                geo = birth_place.find('.//madsrdf:Geographic', namespaces)
                if geo is not None:
                    geo_label = geo.find('.//rdfs:label', namespaces)
                    if geo_label is not None and geo_label.text:
                        add_to_result('Birth Place', geo_label.text)
        
        # Extract ALL occupations
        for occ_elem in rwo.findall('.//madsrdf:occupation', namespaces):
            # Check for nested occupation
            nested_occ = occ_elem.find('.//madsrdf:occupation', namespaces)
            if nested_occ is not None:
                occ_ref = nested_occ.get('{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource')
            else:
                occ_ref = occ_elem.get('{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource')
            
            if occ_ref:
                if 'id.loc.gov/authorities' in occ_ref:
                    occ_label = get_loc_preflabel(occ_ref)
                    if occ_label:
                        add_to_result('Occupation', f"{occ_label} ({occ_ref})")
                    else:
                        add_to_result('Occupation', occ_ref)
                else:
                    add_to_result('Occupation', occ_ref)
        
        # Extract ALL associated locales
        for locale in rwo.findall('.//madsrdf:associatedLocale', namespaces):
            locale_ref = locale.get('{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource')
            if locale_ref:
                if 'id.loc.gov/authorities' in locale_ref:
                    locale_label = get_loc_preflabel(locale_ref)
                    if locale_label:
                        add_to_result('Associated Locale', f"{locale_label} ({locale_ref})")
                    else:
                        add_to_result('Associated Locale', locale_ref)
                else:
                    add_to_result('Associated Locale', locale_ref)
        
        # Extract fields of activity
        for field in rwo.findall('.//madsrdf:fieldOfActivity', namespaces):
            # Check for nested fieldOfActivity
            nested_field = field.find('.//madsrdf:fieldOfActivity', namespaces)
            if nested_field is not None:
                field_ref = nested_field.get('{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource')
            else:
                field_ref = field.get('{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource')
            
            if field_ref:
                if 'id.loc.gov/authorities' in field_ref:
                    field_label = get_loc_preflabel(field_ref)
                    if field_label:
                        add_to_result('Field of Activity', f"{field_label} ({field_ref})")
                    else:
                        add_to_result('Field of Activity', field_ref)
                else:
                    add_to_result('Field of Activity', field_ref)
        
        # Extract entity descriptors
        for descriptor in rwo.findall('.//madsrdf:entityDescriptor', namespaces):
            desc_ref = descriptor.get('{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource')
            if desc_ref:
                if 'id.loc.gov/authorities' in desc_ref:
                    desc_label = get_loc_preflabel(desc_ref)
                    if desc_label:
                        add_to_result('Entity Descriptor', f"{desc_label} ({desc_ref})")
                    else:
                        add_to_result('Entity Descriptor', desc_ref)
                else:
                    add_to_result('Entity Descriptor', desc_ref)
        
        # Extract associated languages
        for lang in rwo.findall('.//madsrdf:associatedLanguage', namespaces):
            lang_ref = lang.get('{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource')
            if lang_ref:
                add_to_result('Associated Language', lang_ref)
    
    # Extract ALL variant labels
    for variant in main_entity.findall('.//madsrdf:hasVariant', namespaces):
        for var_name in variant.findall('.//madsrdf:PersonalName', namespaces) + \
                       variant.findall('.//madsrdf:CorporateName', namespaces) + \
                       variant.findall('.//madsrdf:Topic', namespaces):
            var_label = var_name.find('.//madsrdf:variantLabel', namespaces)
            if var_label is not None and var_label.text:
                lang = var_label.get('{http://www.w3.org/XML/1998/namespace}lang')
                if lang:
                    add_to_result(f'Variant Name ({lang})', var_label.text)
                else:
                    add_to_result('Variant Name', var_label.text)
            
            # Also get MARC keys for variants
            for var_marc in var_name.findall('.//bflc:marcKey', namespaces):
                if var_marc.text:
                    lang = var_marc.get('{http://www.w3.org/XML/1998/namespace}lang')
                    if lang:
                        add_to_result(f'Variant MARC Key ({lang})', var_marc.text)
                    else:
                        add_to_result('Variant MARC Key', var_marc.text)
    
    # Extract ALL external authorities - EXCLUDING direct URI resources from identifiesRWO and hasCloseExternalAuthority
    # We skip identifiesRWO and hasCloseExternalAuthority elements that have rdf:resource attributes pointing to URIs
    # These are handled differently and should not be included in the output
    
    # Extract exact external authorities
    for exact_auth in main_entity.findall('.//madsrdf:hasExactExternalAuthority', namespaces):
        exact_ref = exact_auth.get('{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource')
        if exact_ref:
            add_to_result('Exact External Authority', exact_ref)
    
    # Extract ALL sources/citations
    for source in main_entity.findall('.//madsrdf:hasSource/madsrdf:Source', namespaces):
        citation_status = source.find('.//madsrdf:citationStatus', namespaces)
        citation_source = source.find('.//madsrdf:citationSource', namespaces)
        citation_note = source.find('.//madsrdf:citationNote', namespaces)
        
        source_text = []
        if citation_status is not None and citation_status.text:
            source_text.append(f"[{citation_status.text}]")
        if citation_source is not None:
            if citation_source.text:
                text = citation_source.text.strip()
                # Skip if text is a Wikidata identifier or URL
                if not (text.startswith('Q') and text[1:].isdigit() or 
                        'wikidata.org' in text):
                    source_text.append(text)
            # Check for resource attribute (URLs)
            source_ref = citation_source.get('{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource')
            if source_ref:
                # Skip Wikidata URLs
                if 'wikidata.org' not in source_ref:
                    source_text.append(f"({source_ref})")
        if citation_note is not None and citation_note.text:
            note_text = citation_note.text.strip()
            # Skip if note contains Wikidata references
            if not ('wikidata.org' in note_text or 
                    (note_text.startswith('Q') and len(note_text) > 1 and note_text[1:].isdigit())):
                source_text.append(f"Note: {note_text}")
        
        if source_text:
            add_to_result('Source', ' '.join(source_text))
    
    # Extract editorial notes
    for note in main_entity.findall('.//madsrdf:editorialNote', namespaces):
        if note.text:
            add_to_result('Editorial Note', note.text)
    
    # Extract classification
    for classification in main_entity.findall('.//madsrdf:classification', namespaces):
        class_ref = classification.get('{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource')
        if class_ref:
            add_to_result('Classification', class_ref)
    
    # Extract related authorities
    for related in main_entity.findall('.//madsrdf:hasRelatedAuthority', namespaces):
        related_ref = related.get('{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource')
        if related_ref:
            if 'id.loc.gov/authorities' in related_ref:
                related_label = get_loc_preflabel(related_ref)
                if related_label:
                    add_to_result('Related Authority', f"{related_label} ({related_ref})")
                else:
                    add_to_result('Related Authority', related_ref)
            else:
                add_to_result('Related Authority', related_ref)
    
    # Extract reciprocal authorities
    for reciprocal in main_entity.findall('.//madsrdf:hasReciprocalAuthority', namespaces):
        reciprocal_ref = reciprocal.get('{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource')
        if reciprocal_ref:
            if 'id.loc.gov/authorities' in reciprocal_ref:
                reciprocal_label = get_loc_preflabel(reciprocal_ref)
                if reciprocal_label:
                    add_to_result('Reciprocal Authority', f"{reciprocal_label} ({reciprocal_ref})")
                else:
                    add_to_result('Reciprocal Authority', reciprocal_ref)
            else:
                add_to_result('Reciprocal Authority', reciprocal_ref)
    
    # Extract see references
    for see_ref in main_entity.findall('.//madsrdf:see', namespaces):
        see_uri = see_ref.get('{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource')
        if see_uri:
            if 'id.loc.gov/authorities' in see_uri:
                see_label = get_loc_preflabel(see_uri)
                if see_label:
                    add_to_result('See Reference', f"{see_label} ({see_uri})")
                else:
                    add_to_result('See Reference', see_uri)
            else:
                add_to_result('See Reference', see_uri)
    
    # Extract collection memberships
    for collection in main_entity.findall('.//madsrdf:isMemberOfMADSCollection', namespaces):
        coll_ref = collection.get('{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource')
        if coll_ref:
            add_to_result('Collection Membership', coll_ref)
    
    # Extract scheme memberships
    for scheme in main_entity.findall('.//madsrdf:isMemberOfMADSScheme', namespaces):
        scheme_ref = scheme.get('{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource')
        if scheme_ref:
            add_to_result('Scheme Membership', scheme_ref)
    
    # Extract ALL record information
    for record_info in main_entity.findall('.//ri:RecordInfo', namespaces):
        change_date = record_info.find('.//ri:recordChangeDate', namespaces)
        status = record_info.find('.//ri:recordStatus', namespaces)
        source = record_info.find('.//ri:recordContentSource', namespaces)
        catalog_lang = record_info.find('.//ri:languageOfCataloging', namespaces)
        
        record_text = []
        if status is not None and status.text:
            record_text.append(f"Status: {status.text}")
        if change_date is not None and change_date.text:
            record_text.append(f"Date: {change_date.text[:10]}")
        if source is not None:
            source_ref = source.get('{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource')
            if source_ref:
                record_text.append(f"Source: {source_ref}")
        if catalog_lang is not None:
            lang_ref = catalog_lang.get('{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource')
            if lang_ref:
                record_text.append(f"Language: {lang_ref}")
        
        if record_text:
            add_to_result('Record Info', ' | '.join(record_text))
    
    # Format as text - handle multiple values per key
    text_output = []
    for key, values in result.items():
        if isinstance(values, list):
            for value in values:
                text_output.append(f"{key}: {value}")
        else:
            text_output.append(f"{key}: {values}")
    
    text_format = '\n'.join(text_output)
    
    # For JSON format, simplify single-item lists
    json_result = {}
    for key, values in result.items():
        if isinstance(values, list) and len(values) == 1:
            json_result[key] = values[0]
        else:
            json_result[key] = values
    
    # filter out any line in text_format that has "MARC Key" in it
    text_format = '\n'.join(line for line in text_format.split('\n') if "MARC Key" not in line)
    # make sure each line in text_format is unique
    text_format = '\n'.join(sorted(set(text_format.split('\n'))))

    return text_format, json_result


def return_wikidata(qid: str) -> Optional[Dict[str, Any]]:
    """
    Build an entity comparison prompt by fetching data from Wikidata for a given QID.
    
    This function queries Wikidata to get all properties and values for an entity,
    excluding external IDs, and formats them for comparison purposes.
    
    Args:
        qid: The Wikidata QID (e.g., 'Q3703' or 'Q123456')
        
    Returns:
        A dictionary containing:
        - 'data': List of dictionaries with 'p' (property) and 'o' (object) keys
        - 'prompt': Formatted string with property-value pairs
        Returns None if there's an error fetching data
    """
    # First, fetch labels, aliases, and descriptions
    labels_dict, _ = get_wikidata_labels_aliases_descriptions(qid)
    
    # Initialize with labels, aliases, and descriptions at the start
    binding_data = []
    prompt = ''
    
    if labels_dict:
        # Add labels first
        for lang, label in labels_dict.get('labels', {}).items():
            binding_data.append({
                'p': f'label ({lang})',
                'o': label
            })
            prompt += f"label ({lang}): {label}\n"
        
        # Add descriptions second
        for lang, desc in labels_dict.get('descriptions', {}).items():
            binding_data.append({
                'p': f'description ({lang})',
                'o': desc
            })
            prompt += f"description ({lang}): {desc}\n"
        
        # Add aliases third
        for lang, alias_list in labels_dict.get('aliases', {}).items():
            for alias in alias_list:
                binding_data.append({
                    'p': f'alias ({lang})',
                    'o': alias
                })
                prompt += f"alias ({lang}): {alias}\n"

        for site, site_data in labels_dict.get('sitelinks', {}).items():
            if 'url' in site_data:
                binding_data.append({
                    'p': f'site link ({site})',
                    'o': f"{site_data.get('title', '')} [{site_data.get('url', '')}]"
                })
                prompt += f"site link ({site}): {site_data.get('title', '')} [{site_data.get('url', '')}]\n"



    # SPARQL query to get entity properties and values
    sparql = f"""SELECT ?wdLabel ?ps_Label ?wdpqLabel ?pq_Label{{
        VALUES (?entity) {{(wd:{qid})}}

        ?entity ?p ?statement .
        ?statement ?ps ?ps_ .
        
        ?wd wikibase:claim ?p.
        ?wd wikibase:statementProperty ?ps.
        ?wd wikibase:propertyType ?dataType .

        OPTIONAL {{
          ?statement ?pq ?pq_ .
          ?wdpq wikibase:qualifier ?pq .

        }}
        FILTER (?dataType != wikibase:ExternalId)
        SERVICE wikibase:label {{ 
          bd:serviceParam wikibase:language "en, [AUTO_LANGUAGE]" .            
        }}
      }} ORDER BY ?wd ?statement ?ps_"""
    # print(sparql)
    # Wikidata SPARQL endpoint
    sparql_url = "https://query.wikidata.org/sparql"


    try:
        # Prepare the request
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': 'application/sparql-results+json',
            'User-Agent': 'wikidata user: thisismattmiller / data quality script'
        }
        data = {'query': sparql}
        
        # Execute the request
        response = requests.post(sparql_url, data=data, headers=headers)
        response.raise_for_status()
        sparql_data = response.json()
        
        # Process the results
        if 'results' in sparql_data and 'bindings' in sparql_data['results']:
            for binding in sparql_data['results']['bindings']:
                # Extract property and value
                if 'wdLabel' in binding and 'ps_Label' in binding:
                    p_value = binding['wdLabel']['value']
                    o_value = binding['ps_Label']['value']
                    
                    # Add to binding data
                    binding_data.append({
                        'p': p_value,
                        'o': o_value
                    })

                    # Check if o_value is a coordinate point
                    if o_value.startswith("Point(") == True:
                        o_value_converted = convert_point_to_degree_string(o_value)
                        binding_data.append({
                            'p': p_value,
                            'o': o_value_converted
                        })
                        statement = f"{p_value}: {o_value_converted}"
                        prompt += f"{statement}\n"


                    # Build statement string
                    statement = f"{p_value}: {o_value}"
                    
                    # Add qualifier if present
                    if 'wdpqLabel' in binding and 'pq_Label' in binding:
                        statement += f" ({binding['wdpqLabel']['value']}: {binding['pq_Label']['value']})"
                    
                    prompt += f"{statement}\n"
        
        return {
            'data': binding_data,
            'prompt': prompt
        }
        
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error fetching from Wikidata: {e}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"Request Error fetching from Wikidata: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON response: {e}")
        return None
    except Exception as e:
        print(f"Error fetching from Wikidata: {e}")
        return None


def get_loc_subject_of_works(lccn: str, max_pages: int = None) -> List[str]:
    """
    Fetch all works where the given LCCN is the subject.
    
    Args:
        lccn: The LCCN identifier (e.g., "n79021164")
        max_pages: Maximum number of pages to fetch (None for all pages)
        
    Returns:
        List of work labels
    """
    labels = []
    page = 0
    total_pages = 1  # Will be updated from first response
    
    # Construct base URL
    base_url = f"https://id.loc.gov/resources/works/relationships/subjectof/"
    
    try:
        while page < total_pages:
            # Construct URL with parameters
            params = {
                'label': f'http://id.loc.gov/authorities/names/{lccn}',
                'page': page
            }
            
            # Fetch the data
            headers = {'User-Agent': 'wikidata user: thisismattmiller / data quality script'}
            response = requests.get(base_url, params=params, headers=headers)
            response.raise_for_status()
            data = response.json()
            
            # Update total pages from first response
            if page == 0 and 'summary' in data:
                total_pages = data['summary'].get('totalPages', 1)
                if max_pages and total_pages > max_pages:
                    total_pages = max_pages
            
            # Extract labels from results
            if 'results' in data:
                for result in data['results']:
                    if 'label' in result:
                        labels.append(result['label'])
            
            # Move to next page
            page += 1
            
            # Break if no more pages
            if 'summary' in data and page >= data['summary'].get('totalPages', 0):
                break
                
    except requests.exceptions.RequestException as e:
        print(f"Error fetching subject of works for {lccn}: {e}")
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON response for {lccn}: {e}")
    except Exception as e:
        print(f"Unexpected error fetching subject of works for {lccn}: {e}")
    
    return labels


def get_loc_contributor_to_works(lccn: str, max_pages: int = None) -> List[str]:
    """
    Fetch all works where the given LCCN is a contributor.
    
    Args:
        lccn: The LCCN identifier (e.g., "n79021164")
        max_pages: Maximum number of pages to fetch (None for all pages)
        
    Returns:
        List of work labels
    """
    labels = []
    page = 0
    total_pages = 1  # Will be updated from first response
    
    # Construct base URL
    base_url = f"https://id.loc.gov/resources/works/relationships/contributorto/"
    
    try:
        while page < total_pages:
            # Construct URL with parameters
            params = {
                'label': f'http://id.loc.gov/authorities/names/{lccn}',
                'page': page
            }
            
            # Fetch the data
            headers = {'User-Agent': 'wikidata user: thisismattmiller / data quality script'}
            response = requests.get(base_url, params=params, headers=headers)
            response.raise_for_status()
            data = response.json()

            # Update total pages from first response
            if page == 0 and 'summary' in data:
                total_pages = data['summary'].get('totalPages', 1)
                if max_pages and total_pages > max_pages:
                    total_pages = max_pages
            
            # Extract labels from results
            if 'results' in data:
                for result in data['results']:
                    if 'label' in result:
                        labels.append(result['label'])
            
            # Move to next page
            page += 1
            
            # Break if no more pages
            if 'summary' in data and page >= data['summary'].get('totalPages', 0):
                break
                
    except requests.exceptions.RequestException as e:
        print(f"Error fetching contributor to works for {lccn}: {e}")
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON response for {lccn}: {e}")
    except Exception as e:
        print(f"Unexpected error fetching contributor to works for {lccn}: {e}")
    
    return labels


def get_wikidata_labels_aliases_descriptions(qid: str) -> Tuple[Dict[str, Any], str]:
    """
    Fetch Wikidata JSON for a QID and extract labels, aliases, and descriptions in all languages.
    
    Args:
        qid: The Wikidata QID (e.g., 'Q42')
        
    Returns:
        A tuple containing:
        - Dictionary with 'labels', 'aliases', and 'descriptions' keys, each containing language-keyed data
        - Multi-line text string formatted as "Label (lang): value"
    """
    # Construct URL for Wikidata entity JSON
    url = f"https://www.wikidata.org/wiki/Special:EntityData/{qid}.json"
    
    try:
        # Fetch the JSON data
        headers = {'User-Agent': 'wikidata user: thisismattmiller / data quality script'}
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        # Check if entity exists in response
        if 'entities' not in data or qid not in data['entities']:
            print(f"Entity {qid} not found in response")
            return {}, ""
        
        entity = data['entities'][qid]
        
        # Initialize result dictionary
        result = {
            'labels': {},
            'aliases': {},
            'descriptions': {},
            'sitelinks': {}
        }
        
        # Initialize text output list
        text_lines = []
        
        # Extract labels
        if 'labels' in entity:
            for lang, label_data in entity['labels'].items():
                if 'value' in label_data:
                    result['labels'][lang] = label_data['value']
                    text_lines.append(f"Label ({lang}): {label_data['value']}")
        
        # Extract descriptions
        if 'descriptions' in entity:
            for lang, desc_data in entity['descriptions'].items():
                if 'value' in desc_data:
                    result['descriptions'][lang] = desc_data['value']
                    text_lines.append(f"Description ({lang}): {desc_data['value']}")
        
        # Extract aliases
        if 'aliases' in entity:
            for lang, alias_list in entity['aliases'].items():
                if isinstance(alias_list, list):
                    # Store all aliases for this language
                    result['aliases'][lang] = []
                    for alias_data in alias_list:
                        if 'value' in alias_data:
                            result['aliases'][lang].append(alias_data['value'])
                            text_lines.append(f"Alias ({lang}): {alias_data['value']}")
        
        # Extract site links
        if 'sitelinks' in entity:
            for site, link_data in entity['sitelinks'].items():

                result['sitelinks'][site] = link_data
                text_lines.append(f"Site Link ({site}): {link_data.get('title', '')} [{link_data.get('url', '')}] ")



        # Sort text lines for consistent output
        text_lines.sort()
        text_output = '\n'.join(text_lines)
        
        return result, text_output
        
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error fetching Wikidata for {qid}: {e}")
        return {}, ""
    except requests.exceptions.RequestException as e:
        print(f"Request Error fetching Wikidata for {qid}: {e}")
        return {}, ""
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON response for {qid}: {e}")
        return {}, ""
    except Exception as e:
        print(f"Error fetching Wikidata for {qid}: {e}")
        return {}, ""


def build_lc_data(lccn):


    url = f"https://id.loc.gov/authorities/{lccn}.madsrdf.rdf"

    text_output, json_output = parse_loc_xml(url)

    contributor_to = get_loc_contributor_to_works(lccn,max_pages=5)
    subject_of = get_loc_subject_of_works(lccn,max_pages=5)

    if contributor_to  == None:
        contributor_to = []
    if subject_of == None:
        subject_of = []

    contributor_to = sorted(set(contributor_to))
    subject_of = sorted(set(subject_of))

    json_output['contributor_to'] = contributor_to
    json_output['subject_of'] = subject_of  

    for c in contributor_to:
        text_output += f"\nContributor to: {c}"

    for s in subject_of:
        text_output += f"\nSubject of: {s}"




    return text_output, json_output


def get_lccn_label_and_type(lccn: str) -> Optional[Dict[str, str]]:
    """
    Fetch MADS RDF JSON data for an LCCN and extract type and label.
    
    Args:
        lccn: The LCCN identifier (e.g., "n2009024078")
        
    Returns:
        Dictionary with keys 'type' and 'label', or None if error
    """
    # Construct URL for MADS RDF JSON
    url = f"https://id.loc.gov/authorities/{lccn}.madsrdf.json"
    
    try:
        # Fetch the JSON data
        headers = {'User-Agent': 'LccnBot'}
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        # Parse JSON response
        json_data = response.json()
        
        # Find the dictionary with the @id containing the LCCN
        target_graph = None
        for item in json_data:
            if '@id' in item:
                # Check if this @id contains the LCCN
                if lccn in item['@id']:
                    # Also check that it's the main authority record (not a variant or other element)
                    if 'http://id.loc.gov/authorities' in item['@id'] and item['@id'].endswith(lccn):
                        target_graph = item
                        break
        
        if not target_graph:
            print(f"Could not find main graph for LCCN {lccn}")
            return None
        
        # Extract @type value
        type_value = None
        if '@type' in target_graph:
            types = target_graph['@type']
            # Filter for MADS types (PersonalName, CorporateName, Topic, etc.)
            mads_types = [t for t in types if 'http://www.loc.gov/mads/rdf/v1#' in t]
            if mads_types:
                # Get the first MADS type that's not just "Authority"
                for mads_type in mads_types:
                    if not mads_type.endswith('#Authority'):
                        type_value = mads_type.split('#')[-1]  # Extract just the type name
                        break
        
        # Extract authoritative label
        label_value = None
        auth_label_key = 'http://www.loc.gov/mads/rdf/v1#authoritativeLabel'
        if auth_label_key in target_graph:
            labels = target_graph[auth_label_key]
            if labels and len(labels) > 0:
                # Get the first label's value
                first_label = labels[0]
                if '@value' in first_label:
                    label_value = first_label['@value']
        
        return {
            'type': type_value if type_value else 'Unknown',
            'label': label_value if label_value else 'No label found'
        }
        
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error fetching MADS JSON for {lccn}: {e}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"Request Error fetching MADS JSON for {lccn}: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON response for {lccn}: {e}")
        return None
    except Exception as e:
        print(f"Error fetching MADS JSON for {lccn}: {e}")
        return None


def query_wikidata_for_label_and_instanceOf(qid: str) -> Optional[Dict[str, Any]]:
    """
    Query Wikidata for a specific QID and return its label and instance of values.
    
    Args:
        qid: The Wikidata QID (e.g., 'Q30' or 'Q42'). Can include or exclude the 'Q' prefix.
        
    Returns:
        Dictionary with keys 'qid', 'label', and 'instance', or None if error
    """
    # Ensure QID has the Q prefix
    if not qid.startswith('Q'):
        qid = 'Q' + qid
    
    # SPARQL query
    sparql_query = f"""SELECT ?item ?itemLabel (GROUP_CONCAT(?instanceOfLabel; SEPARATOR=", ") AS ?instanceOf)
WHERE {{
  VALUES ?item {{ wd:{qid} }}
   SERVICE wikibase:label {{
    bd:serviceParam wikibase:language "en" .
   }}
  ?item wdt:P31 ?instanceOf .
  ?instanceOf rdfs:label ?instanceOfLabel .
  FILTER (LANG(?instanceOfLabel) = "en")
}}
GROUP BY ?item ?itemLabel"""
    
    # Wikidata SPARQL endpoint
    sparql_url = "https://query.wikidata.org/sparql"
    
    # Headers with custom user agent
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept': 'application/sparql-results+json',
        'User-Agent': 'LccnBot'
    }
    
    try:
        # Prepare and execute the request
        data = {'query': sparql_query}
        response = requests.post(sparql_url, data=data, headers=headers)
        response.raise_for_status()
        
        # Parse JSON response
        json_data = response.json()
        
        # Extract results
        if 'results' in json_data and 'bindings' in json_data['results']:
            bindings = json_data['results']['bindings']
            
            if bindings and len(bindings) > 0:
                binding = bindings[0]  # Should only be one result for a single QID
                
                # Extract QID from URI
                item_uri = binding.get('item', {}).get('value', '')
                extracted_qid = item_uri.split('/')[-1] if item_uri else qid
                
                # Extract label
                label = binding.get('itemLabel', {}).get('value', '')
                
                # Extract instance of values
                instance_of = binding.get('instanceOf', {}).get('value', '')
                
                return {
                    'qid': extracted_qid,
                    'label': label,
                    'instance': instance_of
                }
            else:
                # No results found - entity might exist but has no P31 (instance of) property
                print(f"No instance of (P31) values found for {qid}")
                return {
                    'qid': qid,
                    'label': 'No label found',
                    'instance': 'No instance of values'
                }
                
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error querying Wikidata for {qid}: {e}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"Request Error querying Wikidata for {qid}: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON response for {qid}: {e}")
        return None
    except Exception as e:
        print(f"Error querying Wikidata for {qid}: {e}")
        return None


if __name__ == "__main__":
    pass

    # lc_text, lc_json = build_lc_data("n79021164")
    # print(lc_text)
    # # Test the functions
    # test_url = "https://id.loc.gov/authorities/names/n2016030655"
    
    # print("Testing get_loc_preflabel:")
    # label = get_loc_preflabel(test_url)
    # if label:
    #     print(f"  Preflabel: {label}")
    # else:
    #     print("  Failed to get preflabel")
    
    # print("\nTesting parse_loc_xml:")
    # text_output, json_output = parse_loc_xml(test_url + ".madsrdf.rdf")
    
    # print("\nText format:")
    # print(text_output)
    
    # print("\nJSON format:")
    # print(json.dumps(json_output, indent=2))
    
    # print("\nTesting return_wikidata:")
    # # Test with Mark Twain's QID
    # result = return_wikidata("Q42")
    # print(result['prompt'])

    # test = get_lccn_label_and_type("n79021164")

    # print(test) 
    # test = query_wikidata_for_label_and_instanceOf("Q16494911")
    # print(test)
    # if result:
    #     print(f"  Found {len(result['data'])} properties")
    #     print("\nFirst 5 properties:")
    #     for item in result['data']:
    #         print(f"    {item['p']}: {item['o']}")
    #     print("\nPrompt preview (first 500 chars):")
    #     print(result['prompt'])
    # else:
    #     print("  Failed to fetch entity data")
    
    # print("\nTesting get_wikidata_labels_aliases_descriptions:")
    # Test with Douglas Adams' QID
    # dict_result, text_result = get_wikidata_labels_aliases_descriptions("Q42")

    # print(text_result)
    # if dict_result:
    #     print(f"  Found {len(dict_result['labels'])} labels")
    #     print(f"  Found {len(dict_result['descriptions'])} descriptions")
    #     print(f"  Found {len(dict_result['aliases'])} languages with aliases")
        
    #     # Show sample output
    #     print("\nSample labels:")
    #     for lang in ['en', 'fr', 'de', 'es', 'ja']:
    #         if lang in dict_result['labels']:
    #             print(f"    {lang}: {dict_result['labels'][lang]}")
        
    #     print("\nSample descriptions:")
    #     for lang in ['en', 'fr', 'de']:
    #         if lang in dict_result['descriptions']:
    #             print(f"    {lang}: {dict_result['descriptions'][lang]}")
        
    #     print("\nText output preview (first 500 chars):")
    #     print(text_result[:500])
    # else:
    #     print("  Failed to fetch Wikidata labels/aliases/descriptions")



