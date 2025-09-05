
import json
import os
from .parse_data_sources import build_lc_data, return_wikidata, get_wikidata_labels_aliases_descriptions

from google import genai
from google.genai import types


model = 'gemini-2.5-flash'

client = genai.Client(
    api_key=os.environ.get("GOOGLE_GENAI"),
)


def translate_dict(to_translate):

    contents = [
        types.Content(
            role="user",
            parts=[
                types.Part.from_text(text=f"""You are translating the values  of labels if they are not in English already. For each key value pair in this json if the value is not in English translate it to english and return it as the new value for the object if it already english set its value to null. Return JSON:\n {json.dumps(to_translate, indent=2)}"""),
            ],
        ) 
    ]
    generate_content_config = types.GenerateContentConfig(
        temperature=0,
        thinking_config = types.ThinkingConfig(
            thinking_budget=-1,
        ),
        response_mime_type="application/json",
    )

    response_text = ""

    for chunk in client.models.generate_content_stream(
        model=model,
        contents=contents,
        config=generate_content_config,
    ):
        if chunk != None and chunk.text != None and chunk.text.strip() != "":
            response_text=response_text+chunk.text        


    try:
        response_json = json.loads(response_text)
    except json.JSONDecodeError:
        response_json = {"error": "Failed to parse response as JSON"}



    return response_json


def send_prompt(prompt, match_is_boolean=False):

    
    properties = {
        "match": genai.types.Schema(
            type = genai.types.Type.STRING,
        ),
        "reason": genai.types.Schema(
            type = genai.types.Type.STRING,
        ),
    }    

    
    if match_is_boolean:
        properties = {
            "match": genai.types.Schema(
                type = genai.types.Type.BOOLEAN,
            ),
            "reason": genai.types.Schema(
                type = genai.types.Type.STRING,
            ),
        }



    generate_content_config = types.GenerateContentConfig(
        temperature=0,
        thinking_config = types.ThinkingConfig(
            thinking_budget=-1,
        ),
        response_mime_type="application/json",
        response_schema=genai.types.Schema(
            type = genai.types.Type.OBJECT,
            required = ["match", "reason"],
            properties = properties
        ),


    )




    response_text = ""

    for chunk in client.models.generate_content_stream(
        model=model,
        contents=prompt,
        config=generate_content_config,
    ):

        if chunk != None and chunk.text != None and chunk.text.strip() != "":
            response_text=response_text+chunk.text


    # try to parse response_text as json response
    try:
        response_json = json.loads(response_text)
    except json.JSONDecodeError:
        response_json = {"error": "Failed to parse response as JSON"}


    return response_json


def build_prompt_single_wiki_to_lccns(qid, lccns):

    error = False
    prompt = 'You are a helpful assistant comparing entities between multiple sources. You will be given an entity and data about it along with multiple possible matches from another database.  You are trying to identifiy which entitiy is the best match. Reply in JSON Object with two keys, \"match\" that is the LCCN identfier for the correct match and \"reason\" a short one sentence explanation for your reasoning. If all the options do not appear correct set match to "None".'


    error_message = ''

    wiki_data = return_wikidata(qid)

    wiki_prompt = wiki_data['prompt']


    prompt = prompt + f"\n\nThis is the source entitiy and it's data you are trying to find the best match for: \n {qid} \n {wiki_prompt} \n"

    if len(wiki_prompt.split("\n")) < 3:
        error = True
        prompt = ''
        error_message = 'Not enough information in Wikidata prompt'

    for lccn in lccns:
        lccn_prompt, lccn_data = build_lc_data(lccn)

        if len(lccn_prompt.split("\n")) < 3:
            error = True
            prompt = ''
            error_message = 'Not enough information in LCCN prompt'
            break

        prompt = prompt + f"\n-------------------------------\nThis is a possible match LCCN: {lccn} \n {lccn_prompt} \n"


    return {
        'error': error,
        'error_message': error_message,
        'prompt': prompt
    }


def build_prompt_one_to_one(qid, lccn):

    error = False
    prompt = 'You are a helpful assistant comparing entities between two systems. You will be given an entity and data about it along with a possible matche from another database.  You are trying to identifiy if the entity is a good match. Reply in JSON Object with two keys, \"match\" that is a true or false boolean value if the two entities are a good match and \"reason\" a short one sentence explanation for your reasoning why they are or are not a good match.'


    error_message = ''

    wiki_data = return_wikidata(qid)

    wiki_prompt = wiki_data['prompt']


    prompt = prompt + f"\n\nThis is the source entitiy and it's data you are comparing: \n {qid} \n {wiki_prompt} \n"

    if len(wiki_prompt.split("\n")) < 3:
        error = True
        prompt = ''
        error_message = 'Not enough information in Wikidata prompt'

    lccn_prompt, lccn_data = build_lc_data(lccn)

    if len(lccn_prompt.split("\n")) < 3:
        error = True
        prompt = ''
        error_message = 'Not enough information in LCCN prompt'
        

    prompt = prompt + f"\n-------------------------------\nThis is a possible match LCCN: {lccn} \n {lccn_prompt} \n"


    return {
        'error': error,
        'error_message': error_message,
        'prompt': prompt
    }

def auto_route_prompt(qids,lccns):


    # test if qids and lccns are both strings
    if isinstance(qids, str) and isinstance(lccns, str):
        # do a single 1-to-1 match check
        prompt = build_prompt_one_to_one(qids, lccns)
        if prompt['error'] != True:
            print(prompt.get('prompt', ''))
            return send_prompt(prompt.get('prompt', ''),True)
        else:
            return {"error": prompt['error_message']}

    elif isinstance(qids, list) and isinstance(lccns, list):

        if len(qids) == 1 and len(lccns) == 1:
            prompt = build_prompt_one_to_one(qids[0], lccns[0])
            if prompt['error'] != True:
                print(prompt.get('prompt', ''))
                return send_prompt(prompt.get('prompt', ''),True)
            else:
                return {"error": prompt['error_message']}            

        elif len(qids) == 1 and len(lccns) > 1:
            prompt = build_prompt_single_wiki_to_lccns(qids[0], lccns)
            return send_prompt(prompt)
        elif len(qids) > 1 and len(lccns) == 1:
            # do a many to many?
            pass





