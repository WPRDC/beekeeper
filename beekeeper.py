# This script scans the current package list of a CKAN instance
# and finds the datasets that have not been updated on their
# self-identified schedule.

# Usage:
# python beekeeper.py <package_id> <field_name> <type (e.g., int)>

import os, sys, json, requests, time, textwrap, traceback, ckanapi, fire

from datetime import datetime, timedelta, date
from dateutil import parser

from copy import copy

from notify import send_to_slack

from pprint import pprint
try:
    from icecream import ic
except ImportError:  # Graceful fallback if IceCream isn't installed.
    ic = lambda *a: None if not a else (a[0] if len(a) == 1 else a)  # noqa

def buzz(mute_alerts, msg, username='beekeeper', channel='@david', icon=':bee:'):
    if not mute_alerts:
        send_to_slack(msg, username, channel, icon)

def get_archive_path():
    # Change path to script's path for cron job.
    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    os.chdir(dname)
    last_scan_file = dname+'/last_scan.json'
    return last_scan_file

def store_as_json(output):
    last_scan_file = get_archive_path()
    with open(last_scan_file, 'w') as f:
        json.dump(output, f, ensure_ascii=True, indent = 4)

def load_from_json():
    last_scan_file = get_archive_path()
    if os.path.exists(last_scan_file):
        with open(last_scan_file, 'r') as f:
            return json.load(f)
    else:
        return []

def pluralize(word,xs,return_count=True,count=None):
    # This version of the pluralize function has been modified
    # to support returning or not returning the count
    # as part of the conditionally pluralized noun.
    if xs is not None:
        count = len(xs)
    if return_count:
        return "{} {}{}".format(count,word,'' if count == 1 else 's')
    else:
        return "{}{}".format(word,'' if count == 1 else 's')

def functionalize(assertion):
    if assertion == 'int':
        return lambda x: type(int(x)) == int
    raise ValueError(f"No function currently assigned to {assertion}.")

def get_number_of_rows(site,resource_id,API_key=None):
    """Returns the number of rows in a datastore. Note that even when there is a limit
    placed on the number of results a CKAN API call can return, this function will
    still give the true number of rows."""
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    results_dict = ckan.action.datastore_info(id = resource_id)
    try:
        print("Trying to get datastore info.")
        ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
        results_dict = ckan.action.datastore_info(id = resource_id)
        return results_dict['meta']['count']
    except:
        return None

# results_dict from datastore_info looks like this:
# ic| results_dict: {'meta': {'count': 31798},
#                   'schema': {'Breed': 'text',
#                              'Color': 'text',
#                              'DogName': 'text',
#                              'ExpYear': 'text',
#                              'LicenseType': 'text',
#                              'OwnerZip': 'text',
#                              'ValidDate': 'text'}}
#
# [ ] So, both the schema and the count could be obtained in one call.

def get_schema(site, resource_id, API_key=None):
    # In principle, it should be possible to do this using the datastore_info
    # endpoint instead and taking the 'schema' part of the result.

    # schema is a list of entries like this:
    #       {'id': 'zip', 'type': 'text'},
    try:
        ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
        results_dict = ckan.action.datastore_search(resource_id=resource_id,limit=0)
        schema = results_dict['fields']
    except:
        return None

    return schema

def get_resource_data(site,resource_id,API_key=None,count=50,offset=0,fields=None):
    # Use the datastore_search API endpoint to get <count> records from
    # a CKAN resource starting at the given offset and only returning the
    # specified fields in the given order (defaults to all fields in the
    # default datastore order).
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    if fields is None:
        response = ckan.action.datastore_search(id=resource_id, limit=count, offset=offset)
    else:
        response = ckan.action.datastore_search(id=resource_id, limit=count, offset=offset, fields=fields)
    # A typical response is a dictionary like this
    #{u'_links': {u'next': u'/api/action/datastore_search?offset=3',
    #             u'start': u'/api/action/datastore_search'},
    # u'fields': [{u'id': u'_id', u'type': u'int4'},
    #             {u'id': u'pin', u'type': u'text'},
    #             {u'id': u'number', u'type': u'int4'},
    #             {u'id': u'total_amount', u'type': u'float8'}],
    # u'limit': 3,
    # u'records': [{u'_id': 1,
    #               u'number': 11,
    #               u'pin': u'0001B00010000000',
    #               u'total_amount': 13585.47},
    #              {u'_id': 2,
    #               u'number': 2,
    #               u'pin': u'0001C00058000000',
    #               u'total_amount': 7827.64},
    #              {u'_id': 3,
    #               u'number': 1,
    #               u'pin': u'0001C01661006700',
    #               u'total_amount': 3233.59}],
    # u'resource_id': u'd1e80180-5b2e-4dab-8ec3-be621628649e',
    # u'total': 88232}
    data = response['records']
    return data

def select(field_name, record):
    return record[field_name]

def apply_function_to_all_records(site, resource_id, field_name, assertion_function, API_key=None, chunk_size=5000):
    all_records = []
    assertion_failed = False
    failures = 0
    k = 0
    offset = 0 # offset is almost k*chunk_size (but not quite)
    row_count = get_number_of_rows(site, resource_id, API_key)
    if row_count == 0: # or if the datastore is not active
       print("No data found in the datastore.")
       return True

    failure_limit = 5
    while len(all_records) < row_count and failures < failure_limit and not assertion_failed:
        time.sleep(0.01)
        try:
            records = get_resource_data(site, resource_id, API_key, chunk_size, offset, [field_name])
            for record in records:
                if not assertion_function(select(field_name, record)):

                    assertion_failed = True
                    break
            if records is not None:
                all_records += records
            failures = 0
            offset += chunk_size
        except:
            failures += 1

        # If the number of rows is a moving target, incorporate
        # this step:
        #row_count = get_number_of_rows(site,resource_id,API_key)
        k += 1
        #print("{} iterations, {} failures, {} records, {} total records".format(k, failures, len(records) if records is not None else 0, len(all_records)))

        # Another option for iterating through the records of a resource would be to
        # just iterate through using the _links results in the API response:
        #    "_links": {
        #  "start": "/api/action/datastore_search?limit=5&resource_id=5bbe6c55-bce6-4edb-9d04-68edeb6bf7b1",
        #  "next": "/api/action/datastore_search?offset=5&limit=5&resource_id=5bbe6c55-bce6-4edb-9d04-68edeb6bf7b1"
        # Like this:
            #if r.status_code != 200:
            #    failures += 1
            #else:
            #    URL = site + result["_links"]["next"]

        # Information about better ways to handle requests exceptions:
        #http://stackoverflow.com/questions/16511337/correct-way-to-try-except-using-python-requests-module/16511493#16511493
    if assertion_failed:
        return False

    if failures == failure_limit:
        raise ValueError("apply_function_to_all_records() failed to get all the records.")
    return not assertion_failed

def mind_resource(b, **kwargs):
    from credentials import site, ckan_api_key as API_key
    schema = get_schema(site, b['resource_id'], API_key=API_key)
    field_names = [s['id'] for s in schema]
    assertion_function = functionalize(b['assertion'])
    if b['field_name'] in field_names:
        # Run assertion_function on all values in the field.
        everything_is_fine = apply_function_to_all_records(site, b['resource_id'], b['field_name'], assertion_function, API_key)
        if everything_is_fine:
            print("Everything is fine.")
        else:
            msg = f"The assertion {assertion_function} failed on field name '{b['field_name']}' for resource with ID {b['resource_id']}."
            print(msg)
            buzz(kwargs['mute_alerts'], msg)
    else:
        msg = f"Unable to find field called '{b['field_name']}' in schema for resource with resource ID {b['resource_id']}."
        print(msg)
        buzz(kwargs['mute_alerts'], msg)

def get_all_resources(package_id):
    from credentials import site, ckan_api_key as API_key
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    metadata = ckan.action.package_show(id=package_id)
    return metadata['resources']

def get_resource_metadata(resource_id):
    from credentials import site, ckan_api_key as API_key
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    metadata = ckan.action.resource_show(id=resource_id)
    return metadata

def has_public_datastore(resource_id):
    metadata = get_resource_metadata(resource_id)
    return metadata['datastore_active']

def mind_package(b, **kwargs):
    # Currently this function just applies the assertion to
    # resources within the package. However, there could also
    # be assertions that target the metadata of a pacakge or resource,
    # so an assertion type or assertion target might be a useful
    # way of representing that.
    # Get all resources in package
    resources = get_all_resources(b['package_id'])

    for resource_id in resource_ids:
        if has_public_datastore(resource_id):
            b_resource = dict(b)
            b_resource['resource_id'] = resource_id
            mind_resource(b_resource, **kwargs)

def mind_beeswax(**kwargs):
    for b in beeswax:
        if 'resource_id' in b:
            mind_resource(b, **kwargs)
        elif 'package_id' in b:
            mind_package(b, **kwargs)
        else:
            raise ValueError(f"mind_beeswax does not know how to handle this task: {b}")


#mind_resource(resource_id="37b11f07-361f-442a-966e-fbdc5eef0840", field_name="OwnerZip", assertion_function=functionalize("int"), mute_alerts=True)
#        "128b3ad6-5b2e-4112-bef1-08154190ad01" Resource ID of a private test version of Geocoded Foo Facilities. Guess what?
# datastore_info doesn't work on private datasets because private datasets don't have queryable datastores.


# Ways to specify checks:

#1) Hard-code a list of beeswax dicts.
beeswax = [{
    'resource_id': "37b11f07-361f-442a-966e-fbdc5eef0840",
    'field_name': "OwnerZip",
    'assertion': 'int'
    }]

# A beeswax dict can specify a resource ID (to run the test just on
# that resource) or a package ID (to run the test on all resources
# in the package.

# 2) Have a beeswax.json version of the hard-coded beeswax list.
#       Here the functionalize function is used so that 'assertion'
#       can just be a string and functionalize can assign the
#       actual function, but that still means that a new check
#       could require the modification of the functionalize
#       function.

# 3) It would be nice to have a mode where a check could be specified
# from the command-line, like
# > python beekeeper.py mind_resource --resource_id=37b11f07-361f-442a-966e-fbdc5eef0840 --field_name=OwnerZip --assertion=int
# This should be possible using Google Fire, but it'll take some work to get Google Fire and this script's command-line
# parsing to work together.

from credentials import production
try:
    if __name__ == '__main__':
        mute_alerts = not production
        check_private_datasets = False
        skip_watchdog = False
        test_mode = False
        args = sys.argv[1:]
        copy_of_args = list(args)
        for k,arg in enumerate(copy_of_args):
            if arg in ['mute', 'mute_alerts']:
                mute_alerts = True
                args.remove(arg)
            elif arg in ['test']:
                test_mode = True
                args.remove(arg)
            elif arg in ['production']:
                test_mode = False
                args.remove(arg)
            #elif arg in ['private']: # This won't work.
            #    check_private_datasets = True
            #    args.remove(arg)
        if len(args) > 0:
            print("Unused command-line arguments: {}".format(args))

        mind_beeswax(mute_alerts,test_mode)

except:
    e = sys.exc_info()[0]
    msg = "Error: {} : \n".format(e)
    exc_type, exc_value, exc_traceback = sys.exc_info()
    lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
    msg = ''.join('!! ' + line for line in lines)
    msg = "beekeeper/beekeeper.py failed for some reason.\n" + msg
    print(msg) # Log it or whatever here
    if production:
        buzz(mute_alerts, msg, username='beekeeper', channel='@david', icon=':illuminati:')
