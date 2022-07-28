import json
from venv import create
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

print("Runnning HubSpot > ZAB: Create Subscription and Subscription Item")


#################
# Global Variables
#################

# Set enviroment Var and Global Var
hsKey = replay.reg_get("HS_KEY")
client = hubspot.Client.create(api_key=hsKey)

# Save webhook payload
request = replay.webhook_in()
replay.webhook_out()
payload = json.loads(request.text)
deal_id = payload['objectId']

# Set Location ID Dict
location_id_dict = {
    "International": 15,
    "EUA/CAN": 14
}

# Set Currency ID Dict
currency_ids_dict = {
    "BRL": 2,
    "USD": 1,
    "EUR": 4,
    "GBP": 5,
    "CAD": 3,
    "MXN": 6
}

# Set Charge Schedule ID Dict
charge_schedule_id_dict = {
    "Monthly": 7,
    "Quarterly": 4,
    "Annual": 6,
    "Upfront": 1,
    "Twice a year": 4
}


# Set Read HubSpot Company Properties list
read_company_properties_list = [
    "name",  # Company Name
    "domain",  # Company Domain Name
    "hs_object_id",  # Company ID
    "rock_id",  # Rock ID
    "zab_customer_id"  # ZAB Customer ID at the Company level in HS
]

# Set Read HubSpot Deal Properties list
read_deal_properties_list = [
    "dealname",
    "zab_customer_id",  # ZAB Customer ID at Deal level in HubSpot
    "zab_subscription_id",  # ZAB Subscription ID at Deal level in HubSpot
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
    "billing_contact",  # Billing Contact
    "geographicsegmentationid"  # Giographic Segmentation = Location
]

# Set Read HubSpot Line Item Properties list
read_item_properties_list = [
    "change_order",
    "rockos_parent_id",
    "hs_recurring_billing_period",
    "recurringbillingfrequency",
    "price_index",
    "discount",
    "hs_tcv",
    "name",
    "hs_recurring_billing_start_date",
    "end_date",
    "product_auto_renewal",
    "upfront_amount",
    "rockos_offer_sku_id",
    "rockos_bundle_offer_sku_id",
    "upfront_payment_date",
    "upfront_payment_method",
    "installment_frequency",
    "installment_date",
    "installment_payment_method",
    "quantity",
    "revenue_type",
    "rockos_offer_sku_id",
    "rockos_bundle_offer_sku_id",
    "amount",
    "price",
    "product_family",
    "product_category",
    "revenue_stream",
    "description",
    "zab_offer_item_id"
]


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


def listLineItems(deal_id):
    # Get Line Items List
    line_item_ids = []
    try:
        api_response = client.crm.deals.associations_api.get_all(
            deal_id=deal_id, to_object_type="Line_Items", limit=500)
        LineItems = api_response.to_dict()
    except ApiException as e:
        print("Error getting LineItems List: "+str(e))
    LineItems = LineItems["results"]
    for i in LineItems:
        line_item_ids.append(i['id'])
    return line_item_ids


def readLineItemData(line_item_id_list, read_item_properties_list):
    planLineItems = []
    for item in line_item_id_list:
        try:
            api_response = client.crm.line_items.basic_api.get_by_id(
                line_item_id=item, properties=read_item_properties_list, archived=False)
            planLineItems.append(api_response)
        except ApiException as e:
            print("Error getting Line Item information")
    return planLineItems


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


def updateLineItem(line_item_id, post_item_properties_dict):
    simple_public_object_input = SimplePublicObjectInput(
        properties=post_item_properties_dict)
    try:
        api_response = client.crm.line_items.basic_api.update(
            line_item_id=line_item_id, simple_public_object_input=simple_public_object_input)
        print(api_response)
    except ApiException as e:
        print("Exception when calling basic_api->update: %s\n" % e)


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


def createZabSubscription(subscription_data_dict):
    url = "https://4804052-sb1.restlets.api.netsuite.com/app/site/hosting/restlet.nl?script=customscriptzab_api_restlet&deploy=customdeployzab_api_restlet"
    payload = json.dumps({
        "operation": "create",
        "recordType": "customrecordzab_subscription",
        "record": subscription_data_dict
    })
    headers = getZabAuth("POST", url)
    response = requests.request("POST", url, headers=headers, data=payload)
    print(payload)
    print(response)
    print(response.text)
    return response


def createZabSubscriptionItem(subscription_item_data_dict):
    url = "https://4804052-sb1.restlets.api.netsuite.com/app/site/hosting/restlet.nl?script=customscriptzab_api_restlet&deploy=customdeployzab_api_restlet"
    payload = json.dumps({
        "operation": "create",
        "recordType": "customrecordzab_subscription_item",
        "record": subscription_item_data_dict
    })
    headers = getZabAuth("POST", url)
    response = requests.request("POST", url, headers=headers, data=payload)
    print("Create Subscription Item Response: ")
    print(payload)
    print(response)
    print(response.text)
    return response


def getCurrencyId(currency_code):
    for key in currency_ids_dict:
        if currency_code == key:
            currency_id = currency_ids_dict[key]
            print(currency_id)
            break
        else:
            currency_id = 2  # Default Currency ID is int(2), representing USD
    return currency_id


def validateEndDate(start_date, term_duration):
    # Validate end_date
    end_date = start_date + relativedelta(months=term_duration)
    end_date = end_date + relativedelta(days=-1)
    return end_date


def getTermDates(items_data_list):
    # Returns a Dict with Term data: duration, start_date, end_date
    subs_term_duration = 1
    subs_start_date = datetime.today()
    subs_end_date = datetime.today()
    for item in items_data_list:
        item = item.to_dict()

        # Validate Item Start Date on datetime object
        item_start_date = item['properties']['hs_recurring_billing_start_date']
        if item_start_date == None or item_start_date == '':
            item_start_date = datetime.today()
        else:
            item_start_date = datetime.strptime(item_start_date, "%Y-%m-%d")

        # Validate Term duration
        item_term_duration = item['properties']['hs_recurring_billing_period']
        if (item_term_duration == None or item_term_duration == '' or item_term_duration == "0"):
            item_term_duration = 1
        else:
            item_term_duration = re.sub(
                '[^0-9]', '', item_term_duration)
            item_term_duration = int(item_term_duration)

        # Validate Item End Date on datetime object
        item_end_date = validateEndDate(item_start_date, item_term_duration)

        # Validate Subscription Term duration: subs_term_duration = item_term_duration if bigger then current subs_term_duration
        if item_term_duration > subs_term_duration:
            subs_term_duration = item_term_duration

        # Validate Subscription Start Date
        if item_start_date < subs_start_date:
            subs_start_date = item_start_date

        # Validate Subscription End Date
        if item_end_date > subs_end_date:
            subs_end_date = item_end_date

    # Convert Subscription Start Date and End Date to string from datetime
    subs_start_date = subs_start_date.strftime("%m/%d/%Y")
    subs_end_date = subs_end_date.strftime("%m/%d/%Y")
    term_dates = {
        "duration": subs_term_duration,
        "start_date": subs_start_date,
        "end_date": subs_end_date
    }
    return term_dates


def setSubscriptionData(deal_data):
    name = deal_data['dealname']
    zab_customer_id = deal_data['zab_customer_id']
    external_id = deal_data['hs_object_id']
    currency = deal_data['deal_currency_code']
    currency_id = getCurrencyId(currency)
    rock_id = deal_data['rock_id']

    # Set Location ID based on NetSuite locations list
    location = deal_data['geographicsegmentationid']
    location_id = location_id_dict[location]

    # Remove special characteres from company_document
    company_document = str(deal_data['deal_document'])
    company_document = removeSpecialCarac(
        company_document, [".", "-", "/", ""])

    # Get Term Start Date, End Date, Duration, in a Dict
    term_dates = getTermDates(items_data_list)
    # global_term_duration = term_dates['duration']

    # Set Charge Schedule
    charge_schedule = 3

    # Set Start Date
    start_date = term_dates['start_date']
    end_date = term_dates['end_date']

    subscription_data = {
        "name": name,
        "custrecordzab_s_customer": zab_customer_id,
        "externalid": external_id,
        # "custrecordrc_s_company_document": company_document,
        # 2 is the default value, representing Invoice
        "custrecordzab_s_create_trans_type": 2,
        "custrecordrc_localidade": location_id,
        "custrecordzab_s_currency": currency_id,
        "custrecordzab_s_charge_schedule": charge_schedule,
        "custrecordzab_s_start_date": start_date,
        "custrecordzab_s_end_date": end_date,
        "rock_id": rock_id
    }

    return subscription_data


def setSubscriptionItemData(items_data):
    subscription_item_data_list = []
    for item in items_data:
        # Converts HubSpot Library object to Dict
        item = item.to_dict()

        # Save property value in variables
        discount = item['properties']['discount']
        price = item['properties']['price']
        net_value = item['properties']['amount']
        name = item['properties']['name']
        line_item_id = item['properties']['hs_object_id']
        start_date = item['properties']['hs_recurring_billing_start_date']
        end_date = item['properties']['end_date']
        term_duration = item['properties']['hs_recurring_billing_period']
        quantity = int(item['properties']['quantity'])
        product_family = item['properties']['product_family']
        product_category = item['properties']['product_category']
        product_stream = item['properties']['revenue_stream']
        revenue_type = item['properties']['revenue_type']
        description = item['properties']['description']
        zab_offer_item_id = item['properties']['zab_offer_item_id']

        # Set charge_schedule
        installment_frequency = item['properties']['installment_frequency']
        if (installment_frequency == None or installment_frequency == ''):
            charge_schedule = charge_schedule_id_dict['Upfront']
        else:
            charge_schedule = charge_schedule_id_dict[installment_frequency]

        # Validate Zab Subscription ID
        if deal_data['zab_subscription_id'] == '' or deal_data['zab_subscription_id'] == None:
            zab_subscription_id = ''
        else:
            zab_subscription_id = int(deal_data['zab_subscription_id'])

        # Set Rate Type
        # CHECK
        if revenue_type == "Recurring":
            rate_type = 1
        elif revenue_type == "On Demand":
            rate_type = 3

        # Validate Discount if None or ''
        if discount == None or discount == '':
            discount = float(price) - float(net_value)

        # Validate Term duration
        if (term_duration == None or term_duration == '' or term_duration == '0' or term_duration == 0):
            term_duration = '1'
        else:
            term_duration = re.sub(
                '[^0-9]', '', term_duration)
        term_duration = int(term_duration)

        # Set Start Date
        if start_date == None or start_date == '':
            start_date = date.today()
        else:
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
        # Validate end_date
        end_date = validateEndDate(start_date, term_duration)
        end_date = end_date.strftime("%d/%m/%Y")
        start_date = start_date.strftime("%d/%m/%Y")

        subscription_item_data = {
            "name": name,  # Required
            "custrecordzab_si_subscription": zab_subscription_id,  # Required
            "custrecordzab_si_item": zab_offer_item_id,  # Required
            "custrecordzab_si_rate_type": rate_type,  # Required
            "quantity": quantity,  # Required
            "custrecordzab_si_rate": net_value,
            "custrecordzab_si_proration_type": 1,  # Required
            "custrecordzab_si_charge_schedule": charge_schedule,  # Required
            # "custrecordzab_si_rate_plan": , # Optional
            # "custrecordzab_si_overage_rate": , # Optional
            # "custrecordrc_cfop_code": , # Optional
            # "custrecordrc_mcn_code": , # Optional
            "custrecordrc_si_product_faimily": product_family,  # Required
            "custrecordrc_si_product_stream": product_stream,  # Required
            "custrecordrc_si_product_category": product_category,  # Required
            "custrecordzab_si_bill_in_arrears": False,  # Optional
            # "custrecordzab_si_customer": , # Autopopulate from the Subscription Field
            "custrecordzab_si_item_description": description,  # Required
            # "custrecordzab_si_minimum_charge": , # Optional
            # "custrecordzab_si_price_book": , # Optional
            "externalID": line_item_id,  # Required
            "custrecordzab_si_start_date": start_date,  # Required
            "custrecordzab_si_end_date": end_date,  # Required
        }
        subscription_item_data_list.append(subscription_item_data)
    return subscription_item_data_list


def removeSpecialCarac(string, spec_car_list):
    for i in spec_car_list:
        string = string.replace(i, '')
    return string

#################
# CODE START
#################


company_data = readCompanyData(deal_id, read_company_properties_list)
deal_data = readDealData(deal_id, read_deal_properties_list)
items_id_list = listLineItems(deal_id)
items_data_list = readLineItemData(items_id_list, read_item_properties_list)

if deal_data['zab_subscription_id'] == '' or deal_data['zab_subscription_id'] == None:
    subscription_data = setSubscriptionData(deal_data)
    create_subscription_response = createZabSubscription(subscription_data)
    if create_subscription_response.ok:
        create_subscription_response = create_subscription_response.json()
        if (create_subscription_response['success'] == 'true' or create_subscription_response['success'] == True):
            zab_subs_id = create_subscription_response['internalid']
            deal_data['zab_subscription_id'] = zab_subs_id
            post_deal_properties = {
                "zab_subscription_id": str(zab_subs_id),
                "zab_subscription_status": "Created"
            }
            updateDeal(deal_id, post_deal_properties)
        else:
            post_deal_properties = {
                "zab_subscription_status": "Not Created"
            }
            updateDeal(deal_id, post_deal_properties)
else:
    print("Subscription already exists in ZAB.")


subcription_items_data_list = setSubscriptionItemData(items_data_list)
print(subcription_items_data_list)
for item in subcription_items_data_list:
    create_subscription_item_response = createZabSubscriptionItem(item)
    if create_subscription_item_response.ok:
        print(create_subscription_item_response)
        create_subscription_item_response = create_subscription_item_response.json()
        if create_subscription_item_response['success'] == True or create_subscription_item_response == 'true':
            zab_item_id = create_subscription_item_response['internalid']
            post_properties = {
                "zab_subscription_item_id": str(zab_item_id)
            }
            updateLineItem(item['externalID'], post_properties)
        else:
            print("Error creating subcription item in ZAB")
            print(create_subscription_item_response)


print("End of HubSpot > ZAB: Create Subscription and Subscription Item")
