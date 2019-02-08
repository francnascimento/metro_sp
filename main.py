import datetime
import queue
import threading
import jaydebeapi
import jpype
import requests
import json


token_ct = '###############################'

dsn = '############################'
uid = '########'
pwd = '########'
tbl_m = 'METRO_STATUS'
tbl_w = 'WEATHER'

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

    if(isinstance(query, list)):
        for stmt in query:
           curs.execute(stmt) 
    else:
        curs.execute(query)

    curs.close()
    conn.close()
    return 200, 'Success'

def build_query(content):

    num_rows = len(content['metro_content'])
    datenow = str(datetime.datetime.now())
    wc = content['weather_content']['data']

    # defining weather query
    query_w = 'INSERT INTO ' + tbl_w + ' VALUES (' + str(wc['temperature']) + "," \
        + str(wc['sensation']) + ",'" + str(wc['wind_direction']) + "'," + \
        str(wc['wind_velocity']) + "," + str(wc['humidity']) + ",'" + \
        str(wc['condition']) + "'," + str(wc['pressure']) + ",'" + datenow + "')"

    # defining metro_status query
    query_m = 'INSERT INTO ' + tbl_m + ' VALUES '
    for idx, record in enumerate(content['metro_content']):
        query_m = query_m + "(" + record['Codigo'] + ",'" + record['StatusOperacao'] \
             + "','" + record['Descricao'] + "','" + datenow + "')"

        if idx+1 < num_rows:
            query_m = query_m + ', '

    return [query_m, query_w]

def save_content(content):
    querys = build_query(content)
    return exec_query(querys)

def get_metro_content(out_queue):
    req = requests.get('https://www.viamobilidade.com.br/_vti_bin/SituacaoService.svc/GetAllSituacao')

    if req.status_code == 200:
        json_response = req.json()
    else:
        json_response = []

    out_queue.put(json_response)
    return json_response

def get_weather_content(out_queue):
    req = requests.get('http://apiadvisor.climatempo.com.br/api/v1/weather/locale/3477/current?token=' + token_ct)

    if req.status_code == 200:
        json_response = req.json()
    else:
        json_response = {}

    out_queue.put(json_response)
    return json_response

def get_content_multhreading():
    my_queue = queue.Queue()
    t1 = threading.Thread(target=get_metro_content, args=(my_queue,))
    t2 = threading.Thread(target=get_weather_content, args=(my_queue,))

    t1.start()
    t2.start()
    t1.join()
    t2.join()

    content_w = my_queue.get()
    content_m = my_queue.get()

    m_status = 200
    w_status = 200

    if(isinstance(content_m, dict)):
        content_m, content_w = content_w, content_m

    if len(content_m) == 0: m_status = 500
    if len(content_w) == 0: w_status = 500

    return content_m, m_status, content_w, w_status

def get_content():

    #metro_content, m_status = get_metro_content()
    #weather_content, w_status = get_weather_content()

    content_m, m_status, content_w, w_status = get_content_multhreading()

    if m_status == 200 and w_status == 200:
        content = {}
        content['metro_content'] = content_m
        content['weather_content'] = content_w

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
