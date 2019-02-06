import jaydebeapi
import jpype
import requests
import json

token_ct = '###############################'

dsn = '############################'
uid = '########'
pwd = '########'
tbl = 'METRO_STATUS'

jar = 'db2jcc4.jar' # location of the jdbc driver jar
args='-Djava.class.path=%s' % jar
jvm = jpype.getDefaultJVMPath()
jpype.startJVM(jvm, args)

def exec_query(query):
    conn = jaydebeapi.connect(
        'com.ibm.db2.jcc.DB2Driver', 
        dsn,
        [uid,pwd]
    )

    curs = conn.cursor()

    curs.execute(query)

    curs.close()
    conn.close()
    return 200, 'Success'

def build_query(content):
    
    num_rows = len(content['metro_content'])
    wc = content['weather_content']['data']
    query = 'INSERT INTO ' + tbl + ' VALUES '
    
    for idx, record in enumerate(content['metro_content']):
        query = query + "(" + record['Codigo'] + ",'" + record['StatusOperacao'] \
             + "','" + record['Descricao'] + "'," +  str(wc['temperature']) + "," \
             + str(wc['sensation']) + ",'" + str(wc['wind_direction']) + "'," + \
             str(wc['wind_velocity']) + "," + str(wc['humidity']) + ",'" + \
             str(wc['condition']) + "'," + str(wc['pressure']) + ", CURRENT TIMESTAMP)"

        if idx+1 < num_rows:
            query = query + ', '

    return query

def save_content(content):
    query  = build_query(content)
    return exec_query(query)

def get_metro_content():
    req = requests.get('https://www.viamobilidade.com.br/_vti_bin/SituacaoService.svc/GetAllSituacao')

    if req.status_code == 200:
        json_response = json.loads(str(req.content))
        return json_response, req.status_code
    else:
        return {}, req.status_code

def get_weather_content():
    req = requests.get('http://apiadvisor.climatempo.com.br/api/v1/weather/locale/3477/current?token=' + token_ct)

    if req.status_code == 200:
        json_response = json.loads(req.content)
        return json_response, req.status_code
    else:
        return {}, req.status_code


def get_content():
    metro_content, m_status = get_metro_content()
    weather_content, w_status = get_weather_content()

    if m_status == 200 and w_status == 200:
        content = {}
        content['metro_content'] = metro_content
        content['weather_content'] = weather_content

        return content, 200, ''

    else:
        msg = 'Could not retrieve'
        if m_status != 200: msg = msg + ', Metro'
        if w_status != 200: msg = msg + ', Weather'
        msg = msg + ' content.'

        return {}, 500, msg

    

def main():
    content, status, msg = get_content()

    if(status == 200):
        status, msg = save_content(content)

    if(status != 200):
        pass
        # TODO
        # send_warning_email(status, msg)
    
    return status


if __name__ == "__main__":
    main()
