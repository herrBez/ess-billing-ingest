import os
import logging
import requests
from elasticsearch import Elasticsearch, helpers
from elasticsearch.helpers import BulkIndexError
from time import time, sleep
from datetime import datetime, timedelta


import sys
import json

__version__ = 0.1

'''
connect to Elastic Cloud Billing API to pull down detailed cluster billing metrics
send them to an elasticsearch cluster for magic
https://www.elastic.co/guide/en/cloud/current/Billing_Costs_Analysis.html
'''
presentday = datetime.now()
beforeyesterday = presentday - timedelta(days=1)
yesterday = presentday - timedelta(days=1)

def ess_connect(cluster_id, cluster_api_key):
    '''
    Create a connection to Elastic Cloud
    '''

    logging.info ('Attempting to create connection to elastic cluster')

    es = Elasticsearch(
        cloud_id = cluster_id,
        api_key = cluster_api_key
    )

    return es

def get_billing_api(endpoint, billing_api_key, **kwargs):
    '''
    make a GET request to the billing api
    '''

    logging.info(f'calling billing api with {endpoint}')
    ess_api = 'api.elastic-cloud.com'

    params = kwargs["params"] if "params" in kwargs else None

    response = requests.get(
        url = f'https://{ess_api}{endpoint}',
        headers = {'Authorization': billing_api_key},
        params = {
            "from": beforeyesterday.strftime("%Y-%m-%d"),
            "to": yesterday.strftime("%Y-%m-%d")
        }
    )

    return response

def normalize_cost(d):
    costs = d.pop("costs")
    d["costs"] = {}
    d["costs"]["total"] = costs["total"]


    for dim in costs["dimensions"]:
        d["costs"][dim["type"]] = dim["cost"]

    # Aggregate as it used to be
    d["costs"]["storage"] = d["costs"]["storage_api"] + d["costs"]["storage_bytes"]
    d["costs"]["data_transfer"] = d["costs"]["data_in"] + d["costs"]["data_out"] + d["costs"]["data_internode"]
    d["costs"]["data_transfer_and_storage"] = d["costs"]["storage"] + d["costs"]["data_transfer"]

def pull_org_id(billing_api_key):
    '''
    Get account /api/v1/account info to org_id
    return org_id
    '''

    logging.info(f'Starting pull_org_id')

    # get org_id if it doesn't exist
    account_endp = '/api/v1/account'
    response = get_billing_api(account_endp, billing_api_key)

    if response.status_code != 200:
        logging.error(f'pull_org_id returned error {response} {response.reason}')
        # TODO Need to decide what to do in this situation
    else:
        rj = response.json()
        logging.info(rj)
        return rj['id']

def pull_org_summary(org_id, org_summary_index, now):
    '''
    Get org billing summary including balance
    '''

    logging.info(f'starting pull_org_summary')

    org_summary_endp = f'/api/v1/billing/costs/{org_id}'
    response = get_billing_api(org_summary_endp, billing_api_key)

    if response.status_code != 200:
        raise
        #TODO something else
    else:
        rj = response.json()

        rj['org_id'] = org_id
        rj['_index'] = org_summary_index
        rj['api'] = org_summary_endp
        rj['@timestamp'] = now
        ## Normalize_Cost
        normalize_cost(rj)

        logging.debug(rj)
        return rj

def pull_deployments( org_id, billing_api_key, deployment_index, now):
    '''
    Pull list of deployments from /api/v1/billing/costs/<org_id>/deployments
    return list of deployments payload
    '''

    logging.info(f'starting pull_deployments')

    # get deployments
    deployments_endp = f'/api/v1/billing/costs/{org_id}/deployments'
    response = get_billing_api(deployments_endp, billing_api_key)

    if response.status_code != 200:
        logging.error(response.status_code)
        raise
        #TODO something else
    else:
        rj = response.json()

        #build deployments payload
        payload = []
        for d in rj['deployments']:
            d['_index'] = deployment_index
            d['api'] = deployments_endp
            d['@timestamp'] = now

            normalize_cost(d)


            payload.append(d)




        logging.debug(payload)
        return (payload)


def pull_deployment_itemized(org_id, billing_api_key, deployment_itemized_index, deployment, now):
    '''
    Get the itemized billing for a deployment
    '''

    logging.info(f'starting pull_deployment_itemized')

    # get itemized
    deployment_id = deployment['deployment_id']
    itemized_endp = f'/api/v1/billing/costs/{org_id}/deployments/{deployment_id}/items'
    response = get_billing_api(itemized_endp, billing_api_key)

    if response.status_code != 200:
        raise
        #TODO something else
    else:
        rj = response.json()
        payload = []

        #Break apart the itemized items to make aggregating easier
        common = {
                'deployment_id' : deployment['deployment_id'],
                'deployment_name' : deployment['deployment_name'],
                'api' : itemized_endp,
                '_index' : deployment_itemized_index,
                '@timestamp' : now
                }


        # high level costs

        normalize_cost(rj)
        tmp = rj.pop("costs")
        rj["costs"] = {}
        rj["costs"]["costs"] = tmp
        rj["costs"].update(common)
        rj["costs"]['bill.type'] = 'costs-summary'
        payload.append(rj["costs"])

        # split out dts and resources line items and add cloud_provider field
        for bt in ('data_transfer_and_storage', 'resources'):
            for item in rj[bt]:
                if bt == "resources":
                    item["period"]["gte"] = item["period"].pop("start")
                    item["period"]["lte"] = item["period"].pop("end")
                item['cloud.provider'] = item['sku'].split('.')[0]
                item['bill.type'] = bt
                item.update(common)
                payload.append(item)



    logging.debug(payload)
    return payload



def pull_deployment_charts(org_id, billing_api_key, deployment_charts_index, deployment, now):
    '''
    Get the charts billing for a deployment
    '''

    logging.info(f'starting pull_deployment_charts')

    # get charts
    deployment_id = deployment['deployment_id']
    charts_endp = f'/api/v1/billing/costs/{org_id}/deployments/{deployment_id}/charts'
    response = get_billing_api(charts_endp, billing_api_key)

    if response.status_code != 200:
        print(response.json())
        raise
        #TODO something else
    else:
        rj = response.json()

        logging.info(rj)

        payload = []

        #Break apart the charts items to make aggregating easier
        common = {
                'deployment_id' : deployment['deployment_id'],
                'deployment_name' : deployment['deployment_name'],
                'api' : charts_endp,
                '_index' : deployment_charts_index,
                }
        for r in rj["data"]:

            element = {
                "@timestamp": r["timestamp"],
                "read_timestamp": now
            }

            total = 0
            for v in r["values"]:
                id = v["id"]
                name = v["name"]
                value = v["value"]
                element[id + "_value"] = value
                total += value

            element["total"] = total

            payload.append(
                {
                    **common,
                    **element
                }
            )




    logging.debug(payload)
    return payload


def main(billing_api_key, es, organization_delay, org_summary_index, deployment_inventory_delay, deployment_index, deployment_itemized_delay, deployment_itemized_index, deployment_charts_delay, deployment_charts_index):
    '''
    Connect to API to pull organization id from account,needed for billing APIs
    get list of all deployments currently in the account
    pull the billing info for:
    - account summary level
    - deployment summary level
    - deployment itemized level

    index into elastic cluster
    '''

    logging.info(f'Starting main ')

    # Run the billing pulls on the startup
    deployment_inventory_last_run = 0
    organization_last_run = 0
    deployment_itemized_last_run = 0
    deployment_charts_last_run = 0


    # get the account org_id
    logging.info(f'calling pull_org_id')
    org_id = pull_org_id(billing_api_key)


    logging.info(f'Starting main loop')
    while True:
        # This is kinf of a lazy way to do a timer but exactly running on intervals is not super important so here we are


        billing_payload = []
        now = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')

        # get deployment summary billing
        deployment_inventory_elapsed = time() - deployment_inventory_last_run
        if deployment_inventory_elapsed >= deployment_inventory_delay:
            logging.info(f'calling pull_deployments after {deployment_inventory_elapsed} seconds')
            deployments = pull_deployments(org_id, billing_api_key, deployment_index, now)
            billing_payload.extend(deployments)
            deployment_inventory_last_run = time()

        # get organization billing summary
        organization_elapsed = time() - organization_last_run
        if organization_elapsed >= organization_delay:
            logging.info(f'calling pull_org_summary after {organization_elapsed} seconds')
            org_summary = pull_org_summary(org_id, org_summary_index, now)
            billing_payload.append(org_summary)
            organization_last_run = time()

        # # get deployment itemized billing
        deployment_itemized_elapsed = time() - deployment_itemized_last_run
        if deployment_itemized_elapsed >= deployment_itemized_delay:
            for d in deployments:
                logging.info(f'calling pull_deployment_itemized after {deployment_itemized_elapsed} seconds')
                itemized = pull_deployment_itemized(org_id, billing_api_key, deployment_itemized_index, d, now)
                billing_payload.extend(itemized)
            deployment_itemized_last_run = time()

        # get deployment charts
        deployment_charts_elapsed = time() - deployment_charts_last_run
        if deployment_charts_elapsed >= deployment_charts_delay:
            for d in deployments:
                logging.info(f'calling pull_deployment_charts after {deployment_charts_elapsed} seconds')
                charts = pull_deployment_charts(org_id, billing_api_key, deployment_charts_index, d, now)
                billing_payload.extend(charts)
            deployment_charts_last_run = time()


        if billing_payload:
            logging.info('sending payload to bulk')
            try:
                helpers.bulk(es, billing_payload)
            except BulkIndexError as bie:
                print(json.dumps(bie.errors))
                sys.exit(0)
            logging.info('Bulk indexing complete')
        
        break

        # don't spin
        sleep(1)




if __name__ == '__main__':

    logging.basicConfig(format='%(asctime)s:%(levelname)s:%(module)s:%(funcName)s:%(lineno)d:%(message)s', level=logging.DEBUG)
    logging.info('Starting up')

    # ESS Billing
    billing_api_key = os.getenv('billing_api_key')

    logging.debug(billing_api_key)

    organization_delay = 60
    org_summary_index = 'ess.billing'

    deployment_inventory_delay = 60
    deployment_index = 'ess.billing.deployment'

    deployment_itemized_delay = 60
    deployment_itemized_index = 'ess.billing.deployment.itemized'

    deployment_charts_delay = 60
    deployment_charts_index = 'ess.billing.deployment.charts'

    # Destination Elastic info
    es_id = os.getenv('billing_es_id')
    es_api = os.getenv('billing_es_api')
    indexName = 'ess_billing'
    es = ess_connect(es_id, es_api)

    # charts = pull_deployment_charts("2185109087", billing_api_key, deployment_charts_index, {"deployment_id": "12d4a46562aa4c7e809cb5b880314505", "deployment_name": "test"}, datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ'))

    # print(charts)
    # Start main loop
    main(billing_api_key, es, organization_delay, org_summary_index, deployment_inventory_delay, deployment_index, deployment_itemized_delay, deployment_itemized_index, deployment_charts_delay, deployment_charts_index)




#vim: expandtab tabstop=4
