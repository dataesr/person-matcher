import requests
import os
from retry import retry
from project.server.main.logger import get_logger
logger = get_logger(__name__)

def get_from_mongo(pid, myclient):
    mydb = myclient['scanr']
    collection_name = 'project_classification'
    mycoll = mydb[collection_name]
    res = mycoll.find_one({'id': pid})
    if res:
        return res['cache']
    return

@retry(delay=30, tries=2, logger=logger)
def get_mistral_answer(project, myclient):
    pre_computed = get_from_mongo(str(project['id']), myclient)
    if pre_computed and isinstance(pre_computed, list):
        return pre_computed
    label = project.get('label')
    description = project.get('description')
    keywords = project.get('keywords')
    description_txt, label_txt, keywords_txt = '', '', ''
    if isinstance(description, dict):
        for lang in ['en', 'default', 'fr']:
            if isinstance(description.get(lang), str):
                description_txt = description[lang]
                break
    if isinstance(label, dict):
        for lang in ['en', 'default', 'fr']:
            if isinstance(label.get(lang), str):
                label_txt = label[lang]
                break
    if isinstance(keywords, dict):
        for lang in ['en', 'default', 'fr']:
            if isinstance(keywords.get(lang), list):
                keywords_txt = ' ; '.join(keywords[lang])
                break
    msg = ''
    if label_txt:
        msg += f'Title: "{label_txt}"\n'
    if description_txt:
        msg += f'Abstract: "{description_txt}"\n'
    if keywords_txt:
        msg += f'Keywords: "{keywords_txt}"\n'
    messages = [{'role': 'user', 'content': msg}]
    r = requests.post(os.getenv('MISTRAL_COMPLETION_URL'), json = {'messages': messages, 'agent_id': os.getenv('MISTRAL_AGENT_CLASSIF_PROJ')},
                  headers={
                      'Authorization': 'Bearer '+os.getenv('MISTRAL_API_KEY'),
                      'Accept': 'application/json',
                      'Content-Type': 'application/json'
                  })
    try:
        res_md = r.json()['choices'][0]['message']['content']
        clean = res_md.strip()
        clean = clean.replace("```json", "").replace("```", "").strip()
        return json.loads(clean)
    except:
        logger.debug(f'error in response from LLM : {r.text}')
        logger.debug(f"input was {msg}")
