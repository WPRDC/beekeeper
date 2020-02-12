import ckanapi

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


def package_id_of(b):
    if 'package_id' in b:
        return b['package_id']
    if 'resource_id' in b:
        metadata = get_resource_metadata(b['resource_id'])
        return metadata['package_id']
    raise ValueError(f"Unable to find package ID for {b}.")

def make_package_private(b):
    package_id = package_id_of(b)
    from credentials import site, ckan_api_key as API_key
    set_package_parameters_to_values(site, package_id, ['private'], [True], API_key)
    print(f"Made the package {package_id} private.")

def get_package_parameter(site,package_id,parameter=None,API_key=None):
    """Gets a CKAN package parameter. If no parameter is specified, all metadata
    for that package is returned."""
    # Some package parameters you can fetch from the WPRDC with
    # this function are:
    # 'geographic_unit', 'owner_org', 'maintainer', 'data_steward_email',
    # 'relationships_as_object', 'access_level_comment',
    # 'frequency_publishing', 'maintainer_email', 'num_tags', 'id',
    # 'metadata_created', 'group', 'metadata_modified', 'author',
    # 'author_email', 'state', 'version', 'department', 'license_id',
    # 'type', 'resources', 'num_resources', 'data_steward_name', 'tags',
    # 'title', 'frequency_data_change', 'private', 'groups',
    # 'creator_user_id', 'relationships_as_subject', 'data_notes',
    # 'name', 'isopen', 'url', 'notes', 'license_title',
    # 'temporal_coverage', 'related_documents', 'license_url',
    # 'organization', 'revision_id'
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    metadata = ckan.action.package_show(id=package_id)
    if parameter is None:
        return metadata
    else:
        if parameter in metadata:
            return metadata[parameter]
        else:
            return None

def set_package_parameters_to_values(site, package_id, parameters, new_values, API_key):
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    original_values = [get_package_parameter(site,package_id,p,API_key) for p in parameters]
    payload = {}
    payload['id'] = package_id
    for parameter,new_value in zip(parameters,new_values):
        payload[parameter] = new_value
    results = ckan.action.package_patch(**payload)
    #print(results)
    print("Changed the parameters {} from {} to {} on package {}".format(parameters, original_values, new_values, package_id))

def package_is_private(site, package_id, API_key=None):
    return get_package_parameter(site, package_id, 'private', API_key)

def resource_is_private(site, resource_id, API_key=None):
    metadata = get_resource_metadata(resource_id)
    package_id = metadata['package_id']
    return get_package_parameter(site, package_id, 'private', API_key)

