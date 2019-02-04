import jaydebeapi
import jpype
import requests
import json

dsn = '####################################'
uid = '#########'
pwd = '#########'
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
    
    num_rows = len(content)
    query = 'INSERT INTO ' + tbl + ' VALUES '
    
    for idx, record in enumerate(content):
        query = query + "(1," + record['Codigo'] + ",'" + record['StatusOperacao'] + "','" + record['Descricao'] + "', CURRENT TIMESTAMP)"

        if idx+1 < num_rows:
            query = query + ', '

    return query

def save_content(content):
    query  = build_query(content)
    return exec_query(query)

def get_content():
    req = requests.get('https://www.viamobilidade.com.br/_vti_bin/SituacaoService.svc/GetAllSituacao')

    if req.status_code == 200:
        json_response = json.loads(req.content)
        return json_response, req.status_code, ''
    else:
        return {}, req.status_code, 'Could not retrieve content.'

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
