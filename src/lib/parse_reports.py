import os
import json
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from typing import Dict, List, Any
import requests
from html.parser import HTMLParser
from .parse_data_sources import query_wikidata_for_label_and_instanceOf, get_lccn_label_and_type

# Global headers for all requests
headers = {"user-agent": 'LCNNBot/1.0 (https://www.wikidata.org/wiki/User:LCNNBot)'}


def parse_constraint_report_html(html_content: str) -> List[Dict[str, Any]]:
    """
    Parse the constraint report HTML table and extract violations.
    
    Args:
        html_content: HTML content from a Wikidata constraint report page
        
    Returns:
        List of dictionaries with keys: status, property_name, property_P_number, message, constraint
    """
    
    class ConstraintTableParser(HTMLParser):
        def __init__(self):
            super().__init__()
            self.in_table = False
            self.in_tbody = False
            self.in_row = False
            self.in_cell = False
            self.current_row = []
            self.current_cell_content = []
            self.rows = []
            self.in_property_link = False
            self.current_property_p = None
            
        def handle_starttag(self, tag, attrs):
            attrs_dict = dict(attrs)
            
            if tag == 'table' and 'class' in attrs_dict and 'wikitable sortable' in attrs_dict['class']:
                self.in_table = True
            elif self.in_table and tag == 'tbody':
                self.in_tbody = True
            elif self.in_tbody and tag == 'tr':
                self.in_row = True
                self.current_row = []
            elif self.in_row and tag == 'td':
                self.in_cell = True
                self.current_cell_content = []
                self.current_property_p = None
            elif self.in_cell and tag == 'a':
                # Check if this is a property link
                if 'href' in attrs_dict:
                    href = attrs_dict['href']
                    # Match patterns like /wiki/Q664896#P214 or /wiki/Property:P214#P2302
                    if '#P' in href:
                        # Extract P number from the href
                        match = re.search(r'#(P\d+)', href)
                        if match:
                            self.current_property_p = match.group(1)
                            self.in_property_link = True
                    elif '/wiki/Property:P' in href:
                        match = re.search(r'Property:(P\d+)', href)
                        if match and not self.current_property_p:
                            self.current_property_p = match.group(1)
                            
        def handle_endtag(self, tag):
            if tag == 'table' and self.in_table:
                self.in_table = False
                self.in_tbody = False
            elif tag == 'tbody' and self.in_tbody:
                self.in_tbody = False
            elif tag == 'tr' and self.in_row:
                self.in_row = False
                if self.current_row:
                    self.rows.append(self.current_row)
            elif tag == 'td' and self.in_cell:
                self.in_cell = False
                # Clean up the cell content
                cell_text = ' '.join(self.current_cell_content).strip()
                # Store both the text and any P number we found
                self.current_row.append({
                    'text': cell_text,
                    'p_number': self.current_property_p
                })
            elif tag == 'a' and self.in_property_link:
                self.in_property_link = False
                
        def handle_data(self, data):
            if self.in_cell:
                # Clean up the data
                cleaned = data.strip()
                if cleaned:
                    self.current_cell_content.append(cleaned)
    
    # Parse the HTML
    parser = ConstraintTableParser()
    parser.feed(html_content)
    
    # Convert parsed rows to the desired format
    results = []
    for row in parser.rows:
        if len(row) >= 4:  # We expect at least 4 columns
            result = {
                'status': row[0]['text'] if row[0] else '',
                'property_name': row[1]['text'] if row[1] else '',
                'property_P_number': row[1]['p_number'] if row[1] and row[1]['p_number'] else '',
                'message': row[2]['text'] if row[2] else '',
                'constraint': row[3]['text'] if row[3] else ''
            }
            results.append(result)
    
    return results


def fetch_wikidata_with_login(username: str, password: str, url: str) -> str:
    """
    Log into Wikidata and fetch a URL using the authenticated session.
    
    Args:
        username: Wikidata username
        password: Wikidata password
        url: URL to fetch after logging in
        
    Returns:
        HTML content of the requested URL
    """
    # Create a session to maintain cookies
    session = requests.Session()
    
    # Get login token
    login_token_url = "https://www.wikidata.org/w/api.php"
    token_params = {
        "action": "query",
        "meta": "tokens",
        "type": "login",
        "format": "json"
    }

    token_response = session.get(login_token_url, params=token_params, headers=headers)
    print(token_response.text)
    print(token_response)
    token_data = token_response.json()
    login_token = token_data["query"]["tokens"]["logintoken"]
    
    # Perform login
    login_params = {
        "action": "login",
        "lgname": username,
        "lgpassword": password,
        "lgtoken": login_token,
        "format": "json"
    }

    login_response = session.post(login_token_url, data=login_params, headers=headers)
    print("login_response", login_response.text)
    login_data = login_response.json()

    if login_data["login"]["result"] != "Success":
        return {"result":False, 'html':"Login Error"}
        # raise Exception(f"Login failed: {login_data['login'].get('reason', 'Unknown error')}")
    
    # Fetch the requested URL with the authenticated session
    try:
        response = session.get(url, headers=headers)
        response.raise_for_status()


        return {"result":True, 'html':response.text}


    except requests.exceptions.RequestException as e:
        print(f"Error fetching {url}: {e}")
        return {"result":False, 'html':"Error fetching URL"}


def parse_wikidata_violations(url_or_content: str) -> Dict[str, Any]:
    """
    Extract data from Wikidata constraint violations report URL or raw content.
    
    Args:
        url_or_content: Either a URL to fetch the report from, or the raw report content
        
    Returns:
        Dictionary with each section as a key containing parsed violations
    """
    # Check if the input is a URL (starts with http:// or https://)
    if url_or_content.startswith('http://') or url_or_content.startswith('https://'):
        # It's a URL, fetch the content using requests
        response = requests.get(url_or_content, headers=headers, verify=False)
        response.raise_for_status()
        content = response.text
    else:
        # It's raw content, use it directly
        content = url_or_content
    # Parse the header information

    text_by_lines = content.splitlines()
    active_section = None
    results = {
        'metadata': {},
        'sections': {}
    }

    # match a line that starts and ends with "=="
    section_header_regex = re.compile(r'^==\s*(.*?)\s*==$')
    # match values that look like this [[Q###]]
    qids = re.compile(r'\[\[(Q\d+)\]\]')
    # match lccn urls that look like this: https://id.loc.gov/authorities/n80017681
    lccn_urls = re.compile(r'(https://id\.loc\.gov/authorities/[a-z]+\d+)')
    # match property values that look like this [[Property:P31]]
    property_values = re.compile(r'\[\[Property:(P\d+)\]\]')

    for line in text_by_lines:
        line = line.strip()
        if not line:
            continue


        # if the line matches {{Constraint violations report|date=2025-08-10T11:59:01Z|item count=1691671}} then extract the date
        metadata_match = re.match(r'^\{\{Constraint violations report\|date=(.*?)\|item count=(\d+)\}\}$', line)
        if metadata_match:
            date_str = metadata_match.group(1)
            item_count = int(metadata_match.group(2))
            results['metadata'] = {
                # add the date as a standard yyyy-mm-dd
                'date': datetime.fromisoformat(date_str.replace('Z', '+00:00')).date().isoformat(),
                'item_count': item_count
            }

        header_match = section_header_regex.match(line)
        if header_match:
            active_section = header_match.group(1).strip()
            results['sections'][active_section] = {'violations': []}
            continue

        # pull out any qids from the line and put them all into one list
        qid_matches = qids.findall(line)
        # do the same for lccn urls
        lccn_matches = lccn_urls.findall(line)
        # remove the httpurl from all the lccn_matches and just have the end number
        lccn_matches = [match.split('/')[-1] for match in lccn_matches]
        # and for property values
        property_matches = property_values.findall(line)

        if active_section != None and (qid_matches or lccn_matches or property_matches):
            a_violation = {
                'qids': qid_matches,
                'lccns': lccn_matches,
                'properties': property_matches
            }
            results['sections'][active_section]['violations'].append(a_violation)


    if len(text_by_lines) < 10:
        results['metadata']['warning'] = 'Report is too short to be useful, there is likely an error'
        

    return results


# Test function
def test_parse_violations():
    """Test the parsing function with the provided URL."""
    url = "https://www.wikidata.org/w/index.php?title=Wikidata:Database_reports/Constraint_violations/P244&action=raw"
    
    try:
        result = parse_wikidata_violations(url)
        json.dump(result,open('violations_parsed.json', 'w'), indent=2)
        # Print summary of parsed data
        print("=== Parsing Results ===")
        print(f"Metadata: {result.get('metadata', {})}")
        print(f"\nSections found: {len(result) - 1}")  # -1 for metadata
        
        for section_name, section_data in result.items():
            if section_name != 'metadata':
                print(f"\n{section_name}:")
                if isinstance(section_data, dict):
                    if 'violations_count' in section_data:
                        print(f"  Violations count: {section_data['violations_count']}")
                    if 'exceptions' in section_data:
                        print(f"  Exceptions: {section_data['exceptions']}")
                    if 'violations' in section_data:
                        print(f"  Number of violations: {len(section_data['violations'])}")
                        if section_data['violations'] and len(section_data['violations']) > 0:
                            print(f"  Sample violation: {section_data['violations'][0]}")
                    if 'records_skipped' in section_data:
                        print(f"  Records skipped: {section_data['records_skipped']}")
        
        return result
        
    except Exception as e:
        print(f"Error: {e}")
        return None


def fetch_lccn_bot_reports(days_back: int = 14) -> List[Dict[str, Any]]:
    """
    Fetch and parse LCCN bot XML reports for the last N days.
    
    Args:
        days_back: Number of days to go back (default 14 for 2 weeks)
        
    Returns:
        List of dictionaries containing date, lccn, and qid for each logDetail
    """
    results = []
    
    # Start from today
    current_date = datetime.now() #- timedelta(days=1)
    
    for _ in range(days_back):
        date_str = current_date.strftime("%Y-%m-%d")
        url = f"https://id.loc.gov/loads/lccnbot/{date_str}.xml"
        
        try:
            # Fetch the XML content using requests
            response = requests.get(url, headers=headers, verify=False)
            response.raise_for_status()
            xml_content = response.content
            
            # Parse the XML
            root = ET.fromstring(xml_content)
            
            # Define namespace
            namespaces = {'log': 'info:lc/lds-id/log'}
            
            # Find all logDetail elements
            log_details = root.findall('.//log:logDetail', namespaces)
            
            for detail in log_details:
                lccn = detail.get('lccn')
                qid = detail.get('qid')
                action = detail.get('action')
                
                if lccn and qid:
                    results.append({
                        'date': date_str,
                        'lccn': lccn,
                        'qid': qid,
                        'action': action
                    })
            
            print(f"Successfully processed {date_str}: found {len([r for r in results if r['date'] == date_str])} entries")
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                print(f"No report found for {date_str} (404)")
            else:
                print(f"Error fetching {date_str}: {e}")
        except Exception as e:
            print(f"Error processing {date_str}: {e}")
        
        # Move to previous day
        current_date -= timedelta(days=1)
    
    return results


def test_fetch_lccn_reports():
    """Test fetching LCCN bot reports for the last 2 weeks."""
    print("=== Fetching LCCN Bot Reports ===")
    reports = fetch_lccn_bot_reports(14)
    
    print(f"\nTotal entries collected: {len(reports)}")
    
    # Group by date for summary
    dates = {}
    for report in reports:
        date = report['date']
        if date not in dates:
            dates[date] = 0
        dates[date] += 1
    
    print("\nEntries per date:")
    for date in sorted(dates.keys(), reverse=True):
        print(f"  {date}: {dates[date]} entries")
    
    # Save to JSON file
    with open('lccn_bot_reports.json', 'w') as f:
        json.dump(reports, f, indent=2)
    print(f"\nReports saved to lccn_bot_reports.json")
    
    return reports


def cross_reference_violations_with_reports(violations_url_or_content: str = None, days_back: int = 14) -> Dict[str, Any]:
    """
    Cross-reference LCCN bot reports with Wikidata constraint violations.
    
    Args:
        violations_url_or_content: URL to fetch the report from, or raw report content (if None, uses default P244 URL)
        days_back: Number of days to fetch LCCN bot reports
        
    Returns:
        Dictionary with cross-referenced data
    """
    # Use default URL if not provided
    if violations_url_or_content is None:
        violations_url_or_content = "https://www.wikidata.org/w/index.php?title=Wikidata:Database_reports/Constraint_violations/P244&action=raw"
    
    print("Fetching/parsing Wikidata constraint violations...")
    violations = parse_wikidata_violations(violations_url_or_content)
    
    print("\nFetching LCCN bot reports...")
    lccn_reports = fetch_lccn_bot_reports(days_back)
    
    # Track matches
    matches = []
    
    print(f"\nCross-referencing {len(lccn_reports)} LCCN bot entries with violations report...")
    
    # Process each LCCN bot report entry
    for report in lccn_reports:
        lccn = report['lccn']
        qid = report['qid']
        
        # Track which sections contain this lccn or qid
        found_in_sections = []
        
        # Check each section in violations
        for section_name, section_data in violations.items():
            if section_name == 'metadata':
                continue
            
            lccn_found = False
            qid_found = False
            
            # Check based on section structure
            if isinstance(section_data, dict) and 'violations' in section_data:
                violations_list = section_data['violations']
                
                # Different sections have different structures
                if section_name in ['Format', 'Single value']:
                    # Structure: {'item': 'Qxxx', 'lccn_ids': ['lccn1', 'lccn2']}
                    for violation in violations_list:
                        if isinstance(violation, dict):
                            if 'item' in violation and violation['item'] == qid:
                                qid_found = True
                            if 'lccn_ids' in violation and lccn in violation['lccn_ids']:
                                lccn_found = True
                                
                elif section_name == 'Unique value':
                    # Structure: {'lccn_id': 'xxx', 'items': ['Q1', 'Q2']}
                    for violation in violations_list:
                        if isinstance(violation, dict):
                            if 'lccn_id' in violation and violation['lccn_id'] == lccn:
                                lccn_found = True
                            if 'items' in violation and qid in violation['items']:
                                qid_found = True
                                
                elif section_name in ['Conflicts with {{P|31}}', 'Conflicts with {{P|1144}}']:
                    # Structure: list of Q numbers ['Q123', 'Q456']
                    if qid in violations_list:
                        qid_found = True
                        
                elif section_name == 'Scope':
                    # Structure: {'item': 'Qxxx', 'property': 'Pxxx', 'value': ...}
                    for violation in violations_list:
                        if isinstance(violation, dict):
                            if 'item' in violation and violation['item'] == qid:
                                qid_found = True
                                
                elif section_name == 'Allowed qualifiers':
                    # Structure: {'item': 'Qxxx', 'property': 'Pxxx'}
                    for violation in violations_list:
                        if isinstance(violation, dict):
                            if 'item' in violation and violation['item'] == qid:
                                qid_found = True
                                
                elif section_name == "Label in 'en' language":
                    # Structure: list of Q numbers ['Q123', 'Q456']
                    if qid in violations_list:
                        qid_found = True
                        
                elif section_name == 'Entity types':
                    # Similar to Scope
                    for violation in violations_list:
                        if isinstance(violation, dict):
                            if 'item' in violation and violation['item'] == qid:
                                qid_found = True
            
            if lccn_found or qid_found:
                found_in_sections.append({
                    'section': section_name,
                    'lccn_found': lccn_found,
                    'qid_found': qid_found
                })
        
        # If found in any section, add to matches
        if found_in_sections:
            matches.append({
                'date': report['date'],
                'lccn': lccn,
                'qid': qid,
                'action': report['action'],
                'found_in_sections': found_in_sections
            })
    
    # Create summary
    summary = {
        'total_lccn_reports': len(lccn_reports),
        'total_matches': len(matches),
        'matches_by_section': {},
        'matches_by_action': {},
        'matches': matches
    }
    
    # Count matches by section
    for match in matches:
        for section_info in match['found_in_sections']:
            section = section_info['section']
            if section not in summary['matches_by_section']:
                summary['matches_by_section'][section] = 0
            summary['matches_by_section'][section] += 1
    
    # Count matches by action
    for match in matches:
        action = match['action']
        if action not in summary['matches_by_action']:
            summary['matches_by_action'][action] = 0
        summary['matches_by_action'][action] += 1
    
    return summary


def test_cross_reference():
    """Test the cross-referencing function."""
    print("=== Cross-Referencing LCCN Reports with Violations ===\n")
    
    results = cross_reference_violations_with_reports()  # Will use default URL
    
    # Print summary
    print(f"\n=== Cross-Reference Results ===")
    print(f"Total LCCN bot reports analyzed: {results['total_lccn_reports']}")
    print(f"Total matches found: {results['total_matches']}")
    
    if results['matches_by_section']:
        print(f"\nMatches by violation section:")
        for section, count in sorted(results['matches_by_section'].items(), key=lambda x: x[1], reverse=True):
            print(f"  {section}: {count}")
    
    if results['matches_by_action']:
        print(f"\nMatches by LCCN bot action:")
        for action, count in sorted(results['matches_by_action'].items(), key=lambda x: x[1], reverse=True):
            print(f"  {action}: {count}")
    
    # Show sample matches
    if results['matches']:
        print(f"\nSample matches (first 5):")
        for match in results['matches'][:5]:
            print(f"\n  Date: {match['date']}")
            print(f"  LCCN: {match['lccn']}, QID: {match['qid']}")
            print(f"  Action: {match['action']}")
            print(f"  Found in sections:")
            for section_info in match['found_in_sections']:
                found_items = []
                if section_info['lccn_found']:
                    found_items.append('LCCN')
                if section_info['qid_found']:
                    found_items.append('QID')
                print(f"    - {section_info['section']} ({', '.join(found_items)})")
    
    # Save results to file
    output_file = 'cross_reference_results.json'
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)
    print(f"\nDetailed results saved to {output_file}")
    
    return results


def process_lccn_bot_xml(xml_string: str) -> str:
    """
    Process LCCN bot XML report, run functions on each logDetail, and return modified XML.
    
    Args:
        xml_string: XML string containing LCCN bot report
        
    Returns:
        Modified XML string with processed logDetail elements
    """

    # see if we have the env variable AUTO_EVAL_URL
    auto_eval_url = os.environ.get("AUTO_EVAL_URL", "")

    # Parse the XML string
    root = ET.fromstring(xml_string)
    
    # Define namespaces
    namespaces = {
        'log': 'info:lc/lds-id/log',
        'mets': 'http://www.loc.gov/METS/'
    }
    
    # Register namespaces for output
    for prefix, uri in namespaces.items():
        ET.register_namespace(prefix, uri)
    
    # Find all logDetail elements
    log_details = root.findall('.//log:logDetail', namespaces)
    
    print(f"Processing {len(log_details)} logDetail elements...")
    
    for detail in log_details:
        # Extract attributes from current logDetail
        lccn = detail.get('lccn')
        qid = detail.get('qid')
        action = detail.get('action')
        old_val = detail.get('old', '')
        new_val = detail.get('new', '')

        wiki_label_instance_of = query_wikidata_for_label_and_instanceOf(qid)
        detail.set('wiki-label', wiki_label_instance_of.get('label', ''))
        detail.set('wiki-instanceOf', wiki_label_instance_of.get('instance', ''))

        lc_label_type = get_lccn_label_and_type(lccn)
        detail.set('lc-label', lc_label_type.get('label', ''))
        detail.set('lc-type', lc_label_type.get('type', ''))

        constraints = constraint_violations(qid)
        constraints_value = "no"
        if len(constraints) > 0:
            constraints_value = "yes"       

        for con in constraints:
            if con.get('property_P_number','') == 'P244':
                constraints_value = 'p244'

        detail.set('constraint', constraints_value)

        # auto_eval = auto_route_prompt(qid,lccn)

        if auto_eval_url != '':
            # auto_eval is a url to the service, do a get request and return the json
            response = requests.get(auto_eval_url, params={'qid': qid, 'lccn': lccn})
            if response.status_code == 200:
                auto_eval = response.json()
                match_val = str(auto_eval.get('result', {}).get('match', 'ERROR'))
                detail.set('auto-eval', match_val)
                detail.set('auto-eval-reason', auto_eval.get('result', {}).get('reason', 'ERROR'))

            else:
                auto_eval = {"error": "Failed to retrieve auto_eval data"}

        
        # print("detail",detail)
        # print(ET.tostring(detail, encoding='unicode', method='xml'))
       
    
    # Convert the modified tree back to string
    xml_bytes = ET.tostring(root, encoding='unicode', method='xml')
    
    return xml_bytes


# Example placeholder functions - implement these based on your needs
def validate_lccn(lccn: str) -> bool:
    """Validate LCCN format."""
    # TODO: Implement LCCN validation logic
    return True

def check_qid_exists(qid: str) -> bool:
    """Check if QID exists in Wikidata."""
    # TODO: Implement Wikidata QID check
    return True

def process_action(action: str, lccn: str, qid: str, old_val: str, new_val: str) -> str:
    """Process based on action type."""
    # TODO: Implement action-specific processing
    return "processed"

def custom_validation(lccn: str, qid: str, action: str) -> str:
    """Perform custom validation."""
    # TODO: Implement custom validation logic
    return "valid"

def constraint_violations(qid):

    html_req = fetch_wikidata_with_login(os.getenv("WIKIDATA_USERNAME"), os.getenv("WIKIDATA_PASSWORD"), f"https://www.wikidata.org/wiki/Special:ConstraintReport/{qid}")

    if 'result' in html_req and html_req['result'] != False:
        html = html_req['html']

        if 'Permission error - Wikidata' in html:
            print("ERROR: Permission error encountered.")
            results = False
        else:
            results = parse_constraint_report_html(html)

    else:
        print("ERROR: Login failed")
        results = False

    return results

if __name__ == "__main__":
    pass
    # qid = "Q42"  # Example QID
    # violations = constraint_violations(qid)

    # print(violations)
