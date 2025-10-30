from .schema_definitions_hri import Agent, Catalog, Dataset, Distribution, Kind
import logging
from pydantic import BaseModel, AnyHttpUrl, TypeAdapter, ValidationError
from sempyro.hri_dcat import HRIVCard, HRIAgent
from typing import List, Optional
from .mappings import themes, access_rights, frequencies, statuses, licenses#, distributionstatuses
import warnings

def _is_valid_http_url(value: str) -> bool:
    try:
        TypeAdapter(AnyHttpUrl).validate_python(value)
        return True
    except ValidationError:
        return False

class MetadataRecord(BaseModel):
    catalog: Catalog
    config: Optional[List[dict]] = None
    api_data: Optional[dict] = None

    @classmethod
    def create_metadata_schema_instance(cls, configs : list = None, api_data : dict = None) -> "MetadataRecord":
        """Fills the metadata schema using configs and API data"""
        schema_obj = cls.model_construct(config=configs, api_data=api_data)
        if schema_obj.config is not None:
            for config in schema_obj.config:
                MetadataRecord._fill_fields_default(schema_obj, config)
        if schema_obj.api_data is not None:
            MetadataRecord._populate_schema(schema_obj, api_data, schema_obj.config[0])
        return schema_obj
    
    def transform_schema(self):
        """Calls all functions to change fields to Health-RI complient formats"""
        MetadataRecord._ensure_lists(self)
        MetadataRecord._string_to_enum(self)
        MetadataRecord._agent_to_HRIAgent(self)
        MetadataRecord._kind_to_HRIVCard(self)  

    def validate(self):
        """Validates if mandatory fields have acceptable values"""
        cleaned = MetadataRecord._drop_none(self)
        type(self).model_validate(cleaned, strict=True)
        logging.info("Validation successful")

    # def validate(self): 
    #     self.model_validate(self.model_dump()) 
    #     logging.info("Validation successful") 
          
    @staticmethod
    def _fill_fields_default(schema_obj, config: dict | list):
        """Recursively fills in the fields from the config file"""
        try:
            for key, value in config.items():
                if isinstance(value, list):
                    match key:
                        case "keyword":
                            setattr(schema_obj, key, getattr(schema_obj, key) + value)
                        case _:
                            setattr(schema_obj, key, value)
                else:
                    match key:
                        case "catalog":
                            if not hasattr(schema_obj, key):
                                setattr(schema_obj, key, Catalog.model_construct())
                            MetadataRecord._fill_fields_default(getattr(schema_obj, key), value)
                        case "dataset":
                            if not hasattr(schema_obj, key):
                                setattr(schema_obj, key, Dataset.model_construct())
                            MetadataRecord._fill_fields_default(getattr(schema_obj, key), value)
                        case "distribution":
                            if not hasattr(schema_obj, key) or getattr(schema_obj, key) == None:
                                setattr(schema_obj, key, Distribution.model_construct())
                            MetadataRecord._fill_fields_default(getattr(schema_obj, key), value)
                        case "creator" | "publisher":
                            if not hasattr(schema_obj, key) or getattr(schema_obj, key) == None:
                                setattr(schema_obj, key, Agent.model_construct())
                            MetadataRecord._fill_fields_default(getattr(schema_obj, key), value)
                        case "contact_point":
                            if not hasattr(schema_obj, key) or getattr(schema_obj, key) == None:
                                setattr(schema_obj, key, Kind.model_construct())
                            MetadataRecord._fill_fields_default(getattr(schema_obj, key), value)
                        case "mapping":
                            pass
                        case _:
                            if value:
                                try:
                                    v = getattr(schema_obj, key)
                                    if v == None:
                                        raise AttributeError
                                    else:
                                        warnings.warn(f"Field value overwritten: {key}: {getattr(schema_obj, key)} with {value}")
                                        raise AttributeError
                                except AttributeError:
                                    setattr(schema_obj, key, value)

        except AttributeError as e:
            print("Likely in one of the fields creator, publisher, or contact_point, something else than a dictionary or list was given")
            raise e


    @staticmethod
    def _populate_schema(schema_obj, api_data: dict, config: dict):
        """Recursively fills in the fields from the api data"""
        for field, value in config.items():
            match field:
                case "catalog":
                    MetadataRecord._populate_schema(getattr(schema_obj, field), api_data, value)
                case "dataset":
                    MetadataRecord._populate_schema(getattr(schema_obj, field), api_data, value)
                case "distribution":
                    MetadataRecord._populate_schema(getattr(schema_obj, field), api_data, value)
                case "mapping":
                    if isinstance(value, dict):
                        for api_field, internal_fields in value.items():
                            if api_field in api_data:
                                for internal_field in internal_fields:
                                    if api_data[api_field]:
                                        if internal_field == "keyword" and isinstance(schema_obj.keyword, list):
                                            setattr(schema_obj, internal_field, schema_obj.keyword + api_data[api_field]) # Not using extend here because it changes keyword in config
                                        else:
                                            setattr(schema_obj, internal_field, api_data[api_field])


    @staticmethod
    def _ensure_lists(schema_obj):
        """Changes all fields that need to be lists in the Health-RI metadata schema into lists, and ensures fields that are not allowed to be lists are not"""
        for field_name, field in schema_obj.model_fields.items():
            value = getattr(schema_obj, field_name)
            if isinstance(value, BaseModel):
                MetadataRecord._ensure_lists(value)
                
            is_list_type = 'List' in str(field.annotation)
            if is_list_type and not isinstance(value, list) and value is not None:
                setattr(schema_obj, field_name, [value])
            elif not is_list_type and isinstance(value, list):
                if len(value) == 1:
                    setattr(schema_obj, field_name, value[0])
                    warnings.warn(f"Please do not put list in field: {field_name}")
                else:
                    raise TypeError(f"Found list where it is not supposed to be: {field_name}")


    @staticmethod
    def _string_to_enum(schema_obj):
        """Changes field values into Health-RI supported categories"""
        for field_name, _ in schema_obj.model_fields.items():
            value = getattr(schema_obj, field_name)
            if value:
                if isinstance(value, BaseModel):
                    MetadataRecord._string_to_enum(value)

                elif isinstance(value, list):
                    for v in value:
                        if isinstance(v, BaseModel):
                            MetadataRecord._string_to_enum(v)

                dict_backed = {
                    "access_rights": access_rights,
                    "theme": themes,
                    "license": licenses,
                    "status": statuses,
                    "frequency": frequencies,
                }
                transformer_backed = {"format", "language", "legal_basis", "personal_data", "purpose"}

                if field_name in dict_backed:
                    kind = dict_backed[field_name]
                    if isinstance(value, list):
                        for i, v in enumerate(value):
                            value[i] = MetadataRecord._to_enum(v, kind)
                    else:
                        setattr(schema_obj, field_name, MetadataRecord._to_enum(value, kind))

                elif field_name in transformer_backed:
                    if isinstance(value, list):
                        for i, v in enumerate(value):
                            value[i] = MetadataRecord._to_enum(v, field_name)
                    else:
                        setattr(schema_obj, field_name, MetadataRecord._to_enum(value, field_name))

                elif field_name == "spatial":
                    pass

                else:
                    pass


    @staticmethod
    def _to_enum(value, kind):
        match kind:
            case "format":
                return MetadataRecord._format_transformation(value)
            case "language":
                return MetadataRecord._language_transformation(value)
            case "legal_basis":
                return MetadataRecord._legal_basis_transformation(value)
            case "personal_data":
                return MetadataRecord._personal_data_transformation(value)
            case "purpose":
                return MetadataRecord._purpose_transformation(value)
            case _:
                try:
                    return kind[value.lower()]
                except:
                    if not value in kind.values():
                        raise ValueError(f"{value} incorrect or not supported. Supported values: {', '.join(kind.keys())}")
        

    @staticmethod
    def _format_transformation(value):
        if not _is_valid_http_url(value):
            return f"http://publications.europa.eu/resource/authority/file-type/{value}"
        else:
            if not "http://publications.europa.eu/resource/authority/file-type/" in value:
                raise ValueError(f"Format should be in the form: http://publications.europa.eu/resource/authority/file-type/<code> not {value}")
            else:
                return value


    @staticmethod
    def _language_transformation(value):
        if not _is_valid_http_url(value):
            match value.lower():
                case "nederlands" | "dutch" | "ned":
                    return "http://publications.europa.eu/resource/authority/language/NED"
                case "english" | "engels" | "eng":
                    return "http://publications.europa.eu/resource/authority/language/ENG"
                case _:
                    raise ValueError("For language: either provide 'ned' or 'eng', or in the form http://publications.europa.eu/resource/authority/language/<code>")
        else:
            if not "http://publications.europa.eu/resource/authority/language/" in value:
                raise ValueError(f"Language should be in the form: http://publications.europa.eu/resource/authority/language/<code> not {value}")
            else:
                return value
    
    
    @staticmethod
    def _legal_basis_transformation(value):
        if not _is_valid_http_url(value):
            return f"https://w3id.org/dpv#{value}"
        else:
            return value
        
    @staticmethod
    def _personal_data_transformation(value):
        if not _is_valid_http_url(value):
            return f"https://w3id.org/dpv/pd#{value}"
        else:
            return value
        
    @staticmethod
    def _purpose_transformation(value):
        if not _is_valid_http_url(value):
            return f"https://w3id.org/dpv#{value}"
        else:
            return value

    @staticmethod
    def _agent_to_HRIAgent(schema_obj):
        """Changes Agents into Health-RI Agents"""
        for field_name, _ in schema_obj.model_fields.items():
            value = getattr(schema_obj, field_name)
            if isinstance(value, Agent):
                setattr(schema_obj, field_name, MetadataRecord._create_HRIAgent(value)) 
                
            elif isinstance(value, list) and any(isinstance(v, Agent) for v in value):
                new_agents = []
                for agent in value:
                    if isinstance(agent, Agent):
                        new_agents.append(MetadataRecord._create_HRIAgent(agent))
                    elif isinstance(agent, HRIAgent):
                        new_agents.append(agent)
                    else:
                        raise ValueError("Encountered not Agent or HRIAgent in list")
                setattr(schema_obj, field_name, new_agents)

            elif isinstance(value, BaseModel):
                MetadataRecord._agent_to_HRIAgent(value)    
            elif isinstance(value, list):
                for v in value:
                    if isinstance(v, BaseModel):
                        MetadataRecord._agent_to_HRIAgent(v)

    @staticmethod
    def _create_HRIAgent(agent: Agent) -> HRIAgent:
        kwargs = {
            'mbox': agent.mbox,
            'identifier': agent.identifier,
            'name': agent.name,
            'homepage': agent.homepage
        }
        if agent.spatial is not None:
            kwargs['spatial'] = agent.spatial
        if agent.type is not None:
            kwargs['type'] = agent.type
        if agent.publisher_type is not None:
            kwargs['publisher_type'] = agent.publisher_type
        if agent.publisher_note is not None:
            kwargs['publisher_note'] = agent.publisher_note
        return HRIAgent(**kwargs)

    @staticmethod
    def _kind_to_HRIVCard(schema_obj):
        """Changes kinds into Health-RI VCards"""
        for field_name, _ in schema_obj.model_fields.items():
            value = getattr(schema_obj, field_name)
            if isinstance(value, Kind):
                setattr(schema_obj, field_name, MetadataRecord._create_HRIVCard(value))
            elif isinstance(value, list) and any(isinstance(v, Kind) for v in value):
                new_card = []
                for kind in value:
                    if isinstance(kind, Kind):
                        new_card.append(MetadataRecord._create_HRIVCard(kind))
                    elif isinstance(kind, HRIVCard):
                        new_card.append(kind)
                    else:
                        raise ValueError("Encountered not Kind or VCard in list")
                setattr(schema_obj, field_name, new_card)

            elif isinstance(value, BaseModel):
                MetadataRecord._kind_to_HRIVCard(value)

            elif isinstance(value, list):
                for v in value:
                    if isinstance(v, BaseModel):
                        MetadataRecord._kind_to_HRIVCard(v)

    @staticmethod
    def _create_HRIVCard(kind: Kind) -> HRIVCard:
        kwargs = {
            'hasEmail': kind.hasEmail,
            'formatted_name': kind.fn
        }
        if kind.hasUrl is not None:
            kwargs['contact_page'] = kind.hasUrl
        return HRIVCard(**kwargs)

    # The _drop_none function below is necessary because when validating an HRIVCard or HRIAgent which has
    # optional values that are None, it gives a ValidationError
    @staticmethod
    def _drop_none(data):
        """Removes all None values in non mandatory fields"""
        if isinstance(data, BaseModel):
            result = {}
            for name, field in data.model_fields.items():
                try:
                    value = getattr(data, name)
                except:
                    raise ValueError("Likely put null or null equivalent value in required field")
                if value is not None or field.is_required():
                    result[name] = MetadataRecord._drop_none(value)
            return result

        elif isinstance(data, dict):
            return {k: MetadataRecord._drop_none(v) for k, v in data.items() if v is not None}
        elif isinstance(data, list):
            return [MetadataRecord._drop_none(v) for v in data if v is not None]
        else:
            return data