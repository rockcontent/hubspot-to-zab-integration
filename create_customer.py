import json
from turtle import update
import requests
import hubspot
from hubspot.crm.deals import SimplePublicObjectInput, ApiException
from hubspot.crm.line_items import SimplePublicObjectInput, ApiException
from hubspot.crm.companies import SimplePublicObjectInput, ApiException
import replay
import os
import time
import logging
import oauth2 as oauth  # oauth2-ingaia
from hubspot.crm.line_items import SimplePublicObjectInput, ApiException
from hubspot.crm.deals import SimplePublicObjectInput, ApiException
from hubspot.crm.companies import SimplePublicObjectInput, ApiException
from datetime import date
from datetime import timedelta
from datetime import datetime
from dateutil.relativedelta import relativedelta
import re

print("Running HS>ZAB Customer Creation")

# Set enviroment Var and Global Var
hsKey = replay.reg_get("HS_KEY")
client = hubspot.Client.create(api_key=hsKey)
zab_api_url = "https://4804052-sb1.restlets.api.netsuite.com"

# Save webhook payload
request = replay.webhook_in()
replay.webhook_out()
payload = json.loads(request.text)
deal_id = payload['objectId']

# Set Subsidiary ID Dict
subsidiary_id_dict = {
    "Visually Inc.": 14,
    "Rock Content Serviços de Midia LTDA": 7,
    "Rock Content Mexico S.R.L de C.V.": 21
}

#################
# ABSTRACTION
#################


def readCompanyData(deal_id, read_company_properties_list):
    try:
        api_response = client.crm.deals.associations_api.get_all(
            deal_id=deal_id, to_object_type="Companies", limit=500)
        companies = api_response.results
        companies = companies[0].to_dict()
        company_id = companies['id']
    except ApiException as e:
        print("Exception when calling associations_api->get_all: %s\n" % e)

    try:
        api_response_company = client.crm.companies.basic_api.get_by_id(
            company_id=company_id, properties=read_company_properties_list, archived=False)
        company_data = api_response_company.to_dict()
        company_data = company_data['properties']
    except ApiException as e:
        print("Exception when calling basic_api->get_by_id: %s\n" % e)
    return company_data


def readDealData(deal_id, read_deal_properties_list):
    try:
        api_response = client.crm.deals.basic_api.get_by_id(
            deal_id=deal_id, properties=read_deal_properties_list, archived=False).to_dict()
        deal_data = api_response['properties']
        return deal_data
    except ApiException as e:
        print("Error getting Deal information")


def updateDeal(deal_id, post_deal_properties_dict):
    # Updates the Deal.
    simple_public_object_input = SimplePublicObjectInput(
        properties=post_deal_properties_dict)
    try:
        api_response = client.crm.deals.basic_api.update(
            deal_id=deal_id, simple_public_object_input=simple_public_object_input)
        print(api_response)
    except ApiException as e:
        print("Exception when calling Update Deal endpoint: %s\n" % e)


def updateCompany(company_id, post_company_properties_dict):
    simple_public_object_input = SimplePublicObjectInput(
        properties=post_company_properties_dict)
    try:
        api_response = client.crm.companies.basic_api.update(
            company_id=company_id, simple_public_object_input=simple_public_object_input)
        print(api_response)
    except ApiException as e:
        print("Exception when calling basic_api->update: %s\n" % e)
    return api_response


def postOnSlack(message):
    """
    Posts message on Slack #pool-ion-overages channel
    Args:
        message: string with the message that should be posted on the channel 
    """
    payload = {'text': str(message)}
    header = {'Content-type': 'application/json'}
    try:
        # Call the chat.postMessage method using the WebClient
        response = requests.post(
            "https://hooks.slack.com/services/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", json=payload, headers=header)
        print(response)
        return response
    except:
        print(f"Error posting message: {response.text}")
        return response


def getZabAuth(method, url):
    TOKEN_KEY = '43aefafee4cb269b02fd74ed86639d680f4d044342d85afa5e2a2564d7063fcb'
    TOKEN_SECRET = 'b0b62795068fde5254c215db7685b39fbe76984178f826fae99b3fa0cbd9bf53'
    CONSUMER_KEY = '91e0fed4cb36fdebacdc5925b725986363ed932e5c8f2c9fc66d3c128eae8620'
    CONSUMER_SECRET = '66ed7f5d5fe4ce4c3a2eddac8be7e49399903269bae034d38d28636dd6262c3f'
    NETSUITE_ACCOUNT = '4804052_SB1'
    # url = zab_api_url+'/app/site/hosting/restlet.nl?script=customscriptzab_api_restlet&deploy=customdeployzab_api_restlet&export_id=zab_customer'
    consumer = oauth.Consumer(key=CONSUMER_KEY, secret=CONSUMER_SECRET)
    token = oauth.Token(key=TOKEN_KEY, secret=TOKEN_SECRET)
    realm = NETSUITE_ACCOUNT  # NetSuite account ID
    params = {
        'oauth_version': "1.0",
        'oauth_nonce': oauth.generate_nonce(),
        'oauth_timestamp': str(int(time.time())),
        'oauth_token': TOKEN_KEY,
        'oauth_consumer_key': CONSUMER_KEY
    }

    req = oauth.Request(method=method, url=url, parameters=params)
    signature_method = oauth.SignatureMethod_HMAC_SHA256()
    req.sign_request(signature_method, consumer, token)
    header = req.to_header(realm)
    headery = header['Authorization'].encode('ascii', 'ignore')
    headerx = {"Authorization": headery, "Content-Type": "application/json"}
    #conn = requests.post(url,data=json.dumps(payload),headers=headerx)
    # conn = requests.get(url, headers=headerx)
    # print('Response: ' + conn.text)
    return headerx
    # print(conn.headers)


def getCurrencyId(currency_code):
    for key in currency_ids_list:
        if currency_code == key:
            currency_id = currency_ids_list[key]
            print(currency_id)
            break
    return currency_id


def setCustomerData(company_data, deal_data):

    # Get Entity Subsidiary ID
    entity_subsidiary = deal_data['billing_subsidiary']
    entity_subsidiary_id = int(subsidiary_id_dict[entity_subsidiary])

    # Remove special caracteres from Zip Code
    zip_code = deal_data['zip_code']
    zip_code = removeSpecialCarac(zip_code, [".", "-", "/", ""])
    company_document = removeSpecialCarac(
        company_data['company_document'], [".", "-", "/", ""])
    

    customer_data = {
        "companyname": company_data['name'],
        "externalid": company_data['rock_id'],
        "custentity6": company_data['rock_id'],
        "document": company_document,
        "entitystatus": 13,
        "subsidiary": entity_subsidiary_id,
        "currency": deal_data['deal_currency_code'],  # update to currency ID
        "label": company_data['tradename'],
        "country": deal_data['billing_country'],
        "state": deal_data['billing_state'],
        "addr1": deal_data['billing_address'],
        # "addr2": deal_data['deal_currency_code'],
        "city": deal_data['billing_city'],
        "zip": zip_code
    }
    return customer_data


def createZabCustomer(customer_data):
    url = zab_api_url+"/app/site/hosting/restlet.nl?script=customscriptzab_api_restlet&deploy=customdeployzab_api_restlet"
    payload = json.dumps({
        "operation": "create",
        "recordType": "customer",
        "record": {
            "body": {
                "companyname": customer_data['companyname'],
                "entitystatus": "13",  # "Fechada Ganha" is the default status for customers 
                "subsidiary": customer_data['subsidiary'],  # Reference Subsidiary
                "externalid": customer_data['externalid'],
            },
            "sublists": {
                "addressbook": [
                    {
                        "defaultbilling": "true",
                        "defaultshipping": "true",
                        "label": customer_data['companyname'],
                        "addressbookaddress": {
                            # "country": customer_data['country'],
                            # "custrecord_enl_uf": customer_data['state'],
                            "attention": "",
                            "addrphone": "",
                            "addr1": customer_data['addr1'],
                            "addr2": "",
                            # "custrecord_enl_city": customer_data['city'],
                            "zip": customer_data['zip']
                        }
                    }
                ]
            }
        }
    })
    # headers = {'Content-Type': 'application/json',
    # 			'authorization':tokenBearer
    # }
    headers = getZabAuth("POST", url)
    response = requests.request("POST", url, headers=headers, data=payload)
    print("Customer Creation in ZAB Request payload: ")
    print(payload)
    # print(response.text)
    return response


def removeSpecialCarac(string, spec_car_list):
    for i in spec_car_list:
        string = string.replace(i, '')
    return string

#################
# CODE START
#################


# Set Currency ID Dict
currency_ids_list = {
    "AUD": 6,
    "CAD": 3,
    "CNY": 7,
    "EUR": 4,
    "GBP": 5,
    "NZD": 1,
    "USD": 2
}

# Set Read Company Properties list
read_company_properties_list = [
    "name",
    "domain",
    "tradename",
    "hs_object_id",
    "rock_id",
    "zab_customer_id",
    "company_document",
    "company_document_type"
]

# Set Read Deal Properties list
read_deal_properties_list = [
    "dealname",
    "zab_customer_id",  # ZAB Customer ID at Deal level in HubSpot
    "cs_deal_contract_due_date_datetime",  # Contract Start Date
    "cs_deal_contract_due_date",  # Contract End Date
    "deal_currency_code",  # Currency
    "deal_document",  # Company Document
    "deal_document_type",  # Company Document Type
    "hs_object_id",  # Deal ID
    "rock_id",  # Customer Rock ID
    "trade_name",  # Customer Trade Name
    "rockos_contract_id",  # RockOS Contract ID
    "billing_address",  # Billing Address
    "billing_city",  # Billing City
    "billing_country",  # Billing country
    "billing_state",  # Billing State
    "zip_code",  # Zip Code
    "billing_subsidiary",  # Billing Subsidiary
    "partner_id",  # Rock's Partner ID
    "financial_responsible",  # Financial Responsible
    "billing_contact"  # Billing Contact
]

company_data = readCompanyData(deal_id, read_company_properties_list)
deal_data = readDealData(deal_id, read_deal_properties_list)
customer_data = setCustomerData(company_data, deal_data)
try:
    zab_customer_response = createZabCustomer(customer_data)
except ApiException as e:
    print("Error creation Zab Customer: "+str(e))

if zab_customer_response.ok:
    print(zab_customer_response.text)
    zab_customer_response = zab_customer_response.json()
    if zab_customer_response['success'] == True or zab_customer_response['success'] == 'true':
        print("Customer Created in ZoneBilling")
        zab_customer_id = zab_customer_response['internalid']
        post_deal_properties = {
            "zab_customer_id": zab_customer_id,
            "zab_customer_status": "Created"
        }
        updateDeal(deal_id, post_deal_properties)
        post_company_properties = {
            "zab_customer_id": zab_customer_id,
            "zab_customer_status": "Created"
        }
        updateCompany(company_data['hs_object_id'], post_company_properties)
    else:
        print("Error creting customer in ZoneBilling")
        print(zab_customer_response.text)
        post_deal_properties = {
            "zab_customer_status": "Not Created"
        }
        updateDeal(deal_id, post_deal_properties)
        post_company_properties = {
            "zab_customer_status": "Not Created"
        }
        updateCompany(company_data['hs_object_id'], post_company_properties)

print("End of HubSpot > ZAB: Create Customer")
