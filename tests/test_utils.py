import pytest
from fairmeta.metadata_model import MetadataRecord
from copy import deepcopy
from pydantic import AnyHttpUrl, Field
from sempyro.hri_dcat import HRICatalog
from rdflib import DCTERMS

class FDPCatalog(HRICatalog):
    is_part_of: [AnyHttpUrl] = Field(
        description="Link to parent object", 
        json_schema_extra={
            "rdf_term": DCTERMS.isPartOf, 
            "rdf_type": "uri"
        })
    
def extend_dict(dictionary: dict, extension: dict) -> dict:
    """Extends a nested dictionary with another nested dictionary preserving structure"""
    for key, value in extension.items():
        if key in dictionary and isinstance(dictionary[key], dict) and isinstance(value, dict):
            extend_dict(dictionary[key], value)
        else:
            dictionary[key] = value
    return dictionary

def extract_paths(config, key):
    """Returns what the path to a field would be if it was in config"""
    path = []
    for k, v in config.items():
        if k == "mapping":
            if key in v:
                return [v[key][0]]
        elif isinstance(v, dict):
            value = extract_paths(v, key)
            if value != []:
                path.append(k)
                path.extend(extract_paths(v, key))
    return path

def resolve_path(obj, path, target, config):
    """Returns the value of a field in config or api_data""" 
    match target:
        case "config":
            for key in path:
                obj = getattr(obj, key)
                if isinstance(obj, list):
                    obj = obj[0]
            return obj
        
        case "api_data":
            key = path[0]
            internal_path = extract_paths(config, key)
            for subkey in internal_path[:-1]:
                obj = getattr(obj, subkey)
                if isinstance(obj, list):
                    obj = obj[0]
            obj = getattr(obj, internal_path[-1])
            return obj, internal_path
        case _:
            raise ValueError

def adapted_instance(target, config, api_data, path, value):
    """Changes a field in config or api_data and creates an FDPBase with that"""
    match target:
        case "config":
            adapted_data = deepcopy(config) 
        case "api_data":
            adapted_data = deepcopy(api_data) 
        case _:
            return MetadataRecord.create_metadata_schema_instance(config, api_data)
    
    d = adapted_data
    for key in path[:-1]:
        d = d[key]
    d[path[-1]] = value
    
    if target == "config":
        return MetadataRecord.create_metadata_schema_instance(adapted_data, api_data)
    else:
        return MetadataRecord.create_metadata_schema_instance(config, adapted_data)

def is_list_field(model: MetadataRecord, path):
    """Helper function to decide if a field should be a list"""
    for key in path[:-1]:
        model = getattr(model, key)
        if isinstance(model, list):
            model = model[0]
    field = model.__class__.model_fields[path[-1]]
    return 'List' in str(field.annotation)