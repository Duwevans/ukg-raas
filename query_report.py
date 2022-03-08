from zeep import Client as ZeepClient
import pandas as pd
import time
from zeep import Client as Zeep
from zeep import xsd
import json
import datetime
import requests
import decimal
from zeep.transports import Transport


class UltiProEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return str(obj)
        if isinstance(obj, decimal.Decimal):
            return float(obj)

        return json.UltiProEncoder.default(self, obj)


class UltiProClient:

    def __init__(self, username, password, client_access_key, user_access_key, base_url):
        assert(username is not None)
        assert(password is not None)
        assert(client_access_key is not None)
        assert(user_access_key is not None)
        assert (base_url is not None)
        self.username = username
        self.password = password
        self.client_access_key = client_access_key
        self.user_access_key = user_access_key
        self.base_url = base_url


def create_client():
    client = UltiProClient(
        'USERNAME',
        'PASSWORD',
        'CLIENTACCESSKEY',
        'USERACCESSKEY',
        'https://service2.ultipro.com/services/'
    )
    return client


def authenticate(client):
    print('authenticating client...')
    login_header = {
        'UserName': client.username,
        'Password': client.password,
        'ClientAccessKey': client.client_access_key,
        'UserAccessKey': client.user_access_key,
    }
    endpoint = 'LoginService'

    # Log in and get session token
    zeep_client = Zeep(f"{client.base_url}{endpoint}")
    result = zeep_client.service.Authenticate(_soapheaders=login_header)
    client.token = result['Token']

    # Create xsd ComplexType header - http://docs.python-zeep.org/en/master/headers.html
    header = xsd.ComplexType([
        xsd.Element(
            '{http://www.ultimatesoftware.com/foundation/authentication/ultiprotoken}UltiProToken',
            xsd.String()),
        xsd.Element(
            '{http://www.ultimatesoftware.com/foundation/authentication/clientaccesskey}ClientAccessKey',
            xsd.String()),
    ])

    # Add authenticated header to client object
    client.session_header = header(UltiProToken=client.token, ClientAccessKey=client.client_access_key)


def create_client_and_authenticate():
    client = create_client()
    authenticate(client)

    return client


def retrieve_report(client, report_key):
    endpoint = 'BiStreamingService'
    zeep_client = ZeepClient(f"{client.base_url}{endpoint}")
    return zeep_client.service.RetrieveReport(_soapheaders={'ReportKey': report_key})


def get_report_list(client, context):
    endpoint = 'BiDataService'

    zeep_client = ZeepClient(f"{client.base_url}{endpoint}")
    return zeep_client.service.GetReportList(context)


def get_report_parameters(client, report_path, context):
    endpoint = 'BiDataService'

    zeep_client = ZeepClient(f"{client.base_url}{endpoint}")
    return zeep_client.service.GetReportParameters(report_path, context)


def execute_report(client, context, report_path, delimiter=','):
    endpoint = 'BiDataService'

    session = requests.Session()
    session.headers.update({'US-DELIMITER': delimiter})
    transport = Transport(session=session)
    payload = {'ReportPath': report_path}
    zeep_client = ZeepClient(f"{client.base_url}{endpoint}",
                             transport=transport)
    element = zeep_client.get_element('ns5:ReportRequest')
    obj = element(**payload)
    r = zeep_client.service.ExecuteReport(request=obj, context=context)
    return r['ReportKey']


def log_on_with_token(client):
    endpoint = 'BiDataService'

    credentials = {
        'Token': client.token,
        'ClientAccessKey': client.client_access_key
    }

    # Log on to get ns5:DataContext object with auth
    zeep_client = ZeepClient(f"{client.base_url}{endpoint}")
    element = zeep_client.get_element('ns5:LogOnWithTokenRequest')
    obj = element(**credentials)
    return zeep_client.service.LogOnWithToken(obj)


def execute_and_fetch(client, report_path, delimiter='|', retries=3, retry_pause_seconds=60):
    start_time = time.perf_counter()
    print('Executing report...')
    context = log_on_with_token(client)
    k = execute_report(client, context, report_path, delimiter=delimiter)
    print('Execute report request sent.')
    print('Report key is ' + k)
    print('Retrieving report...')

    for retry in range(retries):
        r = retrieve_report(client, k)
        if r['header']['Status'] == 'Failed':
            print('Response of "Failed" received on attempt ' + (str(retry + 1)) + ' of ' + str(retries))
            break
        elif r['header']['Status'] == 'Working':
            print('Response of "Working" received on attempt ' + (str(retry + 1)) +
                  ' of ' + str(retries) + '. Pausing and trying again...')
            time.sleep(retry_pause_seconds)

        elif r['header']['Status'] == 'Completed':
            print('Report completed successfully on attempt ' + (str(retry + 1)) + ' of ' + str(retries))
            break

    end_time = time.perf_counter()

    print('response returned in ' + str(round(end_time - start_time, 2)) + " seconds.")

    return r['body']['ReportStream'].decode('unicode-escape')


# hard code your report paths here if you'd like then pass just the key to get_ukg_report
#  e.g., df = get_ukg_report(report="example_report")
report_paths = {
    "example_report": "/content/folder[@name='YOURCOMPANY']/folder[@name='YOURCOMPANYNAME']/folder[@name='UltiPro']/folder[@name='FOLDERNAME']/report[@name='REPORTNAME']",
    "new_report_title": "/path/to/report/",
}


def get_ukg_report(report=None, report_path=None, retries=3):
    if (report is None) & (report_path is None):
        print('ERROR: please enter either a report name or a full report path in get_ukg_report function')
        raise Exception

    client = create_client_and_authenticate()

    if not report_path:
        try:
            print('Getting ukg report "' + str(report) + '"...')
            report_path = report_paths[report]  # full path in ukg of the report
        except KeyError:
            print('report name of "' + report + '" not found in available reports.')
            raise Exception
    elif report_path:
        print('Getting ukg report at path of "' + str(report_path) + '" ...')

    for i in range(retries):
        try:
            data = execute_and_fetch(client, report_path, delimiter="|", retries=retries)  # this takes some time
        except Exception as e:
            print(e)
            print('UKG error. Retrying...')
            time.sleep(5)
        else:
            break

    # data comes through as a string, need to turn it into a list of lists
    #  rows are separated by '\r\n' as string form
    data_as_list = []
    for row in data.split('\r\n'):
        ind_list = []
        # this ensures each row is returned as a list of values (otherwise returns one long string still)
        for value in row.split('|'):
            ind_list.append(value)  # add to list of values in row
        data_as_list.append(ind_list)  # add to list of lists

    column_names = data_as_list[0]  # column names are the first row returned
    all_data = data_as_list[1:]  # data is all following rows

    df = pd.DataFrame(all_data, columns=column_names)  # create a dataframe

    return df
