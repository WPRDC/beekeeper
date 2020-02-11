import ckanapi

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

