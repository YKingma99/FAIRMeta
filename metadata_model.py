from schema_definitions_hri import Agent, Catalog, Dataset, Distribution, Kind
import logging
from pydantic import BaseModel, AnyHttpUrl
from sempyro.dcat import AccessRights
from sempyro.hri_dcat import HRIVCard, HRIAgent, DatasetTheme, DatasetStatus, DistributionStatus
from typing import Optional
from mappings import themes, access_rights, frequencies, statuses, licenses, distributionstatuses
import warnings

class MetadataRecord(BaseModel):
    catalog: Catalog
    config: Optional[dict] = None
    api_data: Optional[dict] = None

    @classmethod
    def create_FDP_instance(cls, config : dict = None, api_data : dict = None) -> "MetadataRecord":
        FDP_obj = cls.model_construct(config=config, api_data=api_data)
        if FDP_obj.config is not None:
            MetadataRecord._fill_fields(FDP_obj, config)
            if FDP_obj.api_data is not None:
                MetadataRecord._populate_FDP(FDP_obj, api_data, config)
        return FDP_obj
    
    def transform_FDP(self):
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
    def _fill_fields(FDP_obj, config: dict | list):
        """Recursively fills in the fields from the config file"""
        try:
            for key, value in config.items():
                if isinstance(value, list):
                    setattr(FDP_obj, key, value)
                else:
                    match key:
                        case "catalog":
                            setattr(FDP_obj, key, Catalog.model_construct())
                            MetadataRecord._fill_fields(getattr(FDP_obj, key), value)
                        case "dataset":
                            setattr(FDP_obj, key, Dataset.model_construct())
                            MetadataRecord._fill_fields(getattr(FDP_obj, key), value)
                        case "distribution":
                            setattr(FDP_obj, key, Distribution.model_construct())
                            MetadataRecord._fill_fields(getattr(FDP_obj, key), value)
                        case "creator" | "publisher":
                            setattr(FDP_obj, key, Agent.model_construct())
                            MetadataRecord._fill_fields(getattr(FDP_obj, key), value)
                        case "contact_point":
                            setattr(FDP_obj, key, Kind.model_construct())
                            MetadataRecord._fill_fields(getattr(FDP_obj, key), value)
                        case "mapping":
                            pass
                        case _:
                            if value:
                                setattr(FDP_obj, key, value)
        except AttributeError as e:
            print("Likely in one of the fields creator, publisher, or contact_point, something else than a dictionary or list was given")
            raise e

    @staticmethod
    def _populate_FDP(FDP_obj, api_data: dict, config: dict):
        """Recursively fills in the fields from the api data"""
        for field, value in config.items():
            match field:
                case "catalog":
                    MetadataRecord._populate_FDP(getattr(FDP_obj, field), api_data, value)
                case "dataset":
                    MetadataRecord._populate_FDP(getattr(FDP_obj, field), api_data, value)
                case "distribution":
                    MetadataRecord._populate_FDP(getattr(FDP_obj, field), api_data, value)
                case "mapping":
                    if isinstance(value, dict):
                        for api_field, internal_fields in value.items():
                            if api_field in api_data:
                                for internal_field in internal_fields:
                                    if api_data[api_field]:
                                        if internal_field == "keyword" and isinstance(FDP_obj.keyword, list):
                                            setattr(FDP_obj, internal_field, FDP_obj.keyword + api_data[api_field]) # Not using extend here because it changes keyword in config
                                        else:
                                            setattr(FDP_obj, internal_field, api_data[api_field])

    @staticmethod
    def _ensure_lists(FDP_obj):
        """Changes all fields that need to be lists in the Health-RI metadata schema into lists, and ensures fields that are not allowed to be lists are not"""
        for field_name, field in FDP_obj.model_fields.items():
            value = getattr(FDP_obj, field_name)
            if isinstance(value, BaseModel):
                MetadataRecord._ensure_lists(value)
                
            is_list_type = 'List' in str(field.annotation)
            if is_list_type and not isinstance(value, list) and value is not None:
                setattr(FDP_obj, field_name, [value])
            elif not is_list_type and isinstance(value, list):
                if len(value) == 1:
                    setattr(FDP_obj, field_name, value[0])
                    warnings.warn(f"Please do not put list in field: {field_name}")
                else:
                    raise TypeError(f"Found list where it is not supposed to be: {field_name}")

    @staticmethod
    def _string_to_enum(FDP_obj):
        """Changes field values into Health-RI supported categories"""
        for field_name, _ in FDP_obj.model_fields.items():
            value = getattr(FDP_obj, field_name)
            if value:
                if isinstance(value, BaseModel):
                    MetadataRecord._string_to_enum(value)

                elif isinstance(value, list):
                    for v in value:
                        if isinstance(v, BaseModel):
                            MetadataRecord._string_to_enum(v)

                match field_name:
                    case "access_rights":
                        if not isinstance(value, AccessRights):
                            setattr(FDP_obj, field_name, MetadataRecord._to_enum(value, access_rights))
                    case "theme":
                        if isinstance(value, list):
                            for position, theme in enumerate(value):
                                if not isinstance(value[position], DatasetTheme):
                                    value[position] = MetadataRecord._to_enum(theme, themes)
                        else:
                            if not isinstance(FDP_obj, DatasetTheme):
                                setattr(FDP_obj, field_name, MetadataRecord._to_enum(value, themes))
                    case "language":
                        if isinstance(value, list):
                            for position, lang in enumerate(value):
                                if not isinstance(value[position], AnyHttpUrl):
                                    value[position] = MetadataRecord._to_enum(lang, field_name)
                        else:
                            if not isinstance(FDP_obj, AnyHttpUrl):
                                setattr(FDP_obj, field_name, MetadataRecord._to_enum(value, field_name))
                    case "license":
                        if not isinstance(value, AnyHttpUrl):
                            setattr(FDP_obj, field_name, MetadataRecord._to_enum(value, licenses))
                    case "legal_basis":
                        if isinstance(value, list):
                            for position, basis in enumerate(value):
                                if not isinstance(value[position], AnyHttpUrl):
                                    value[position] = MetadataRecord._to_enum(value, field_name)
                        else:
                            if not isinstance(value, AnyHttpUrl):
                                setattr(FDP_obj, field_name, MetadataRecord._to_enum(value, field_name))
                    case "status":
                        if not isinstance(value, DatasetStatus):
                            setattr(FDP_obj, field_name, MetadataRecord._to_enum(value, statuses))
                    case "spatial":
                        pass
                    case "frequency":
                        if not isinstance(FDP_obj, AnyHttpUrl):
                            setattr(FDP_obj, field_name, MetadataRecord._to_enum(value, frequencies))
                    case _:
                        pass

    @staticmethod
    def _to_enum(value, kind):
        match kind:
            case "language":
                return MetadataRecord._language_transformation(value)
            case "legal_basis":
                return MetadataRecord._legal_basis_transformation(value)
            case _:
                try:
                    return kind[value.lower()]
                except:
                    raise ValueError(f"{value} incorrect or not supported. Supported values: {', '.join(kind.keys())}")
        
    @staticmethod
    def _language_transformation(value):
        if len(value) >= 3:
            return f"http://publications.europa.eu/resource/authority/language/{value[:3].upper()}"
        else:
            raise ValueError("Provide at least 3 characters for language")
    
    @staticmethod
    def _legal_basis_transformation(value):
        return f"https://w3id.org/dpv#{value}"

    # @staticmethod
    # def _language_to_enum(lang: str) -> str:
    #     try:
    #         return lang_map[lang.lower()]
    #     except:
    #         raise ValueError(f"Language code: {lang} incorrect or not supported")
            
    # @staticmethod
    # def _access_rights_to_enum(access_rights: str) -> AccessRights:
    #     match access_rights.lower():
    #         case "public":
    #             return AccessRights.public
    #         case "non-public":
    #             return AccessRights.non_public
    #         case "restricted":
    #             return AccessRights.restricted
    #         case _:
    #             raise ValueError(f"Access right: {access_rights} incorrect or not supported")
            
    # @staticmethod
    # def _theme_to_enum(theme: str) -> DatasetTheme:
    #     match theme.lower():
    #         case "heal":
    #             return DatasetTheme.heal
    #         case _:
    #             raise ValueError("Theme: " + theme + " incorrect or not supported")

    # Investigate if necessary
    # @staticmethod
    # def _license_to_enum(license: str):
    #     return -1

    # @staticmethod
    # def _format_to_enum(format: str):
    #     return -1

    @staticmethod
    def _agent_to_HRIAgent(FDP_obj):
        """Changes Agents into Health-RI Agents"""
        for field_name, _ in FDP_obj.model_fields.items():
            value = getattr(FDP_obj, field_name)
            if isinstance(value, Agent):
                setattr(FDP_obj, field_name, MetadataRecord._create_HRIAgent(value)) 
                
            elif isinstance(value, list) and any(isinstance(v, Agent) for v in value):
                new_agents = []
                for agent in value:
                    if isinstance(agent, Agent):
                        new_agents.append(MetadataRecord._create_HRIAgent(agent))
                    elif isinstance(agent, HRIAgent):
                        new_agents.append(agent)
                    else:
                        raise ValueError("Encountered not Agent or HRIAgent in list")
                setattr(FDP_obj, field_name, new_agents)

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
    def _kind_to_HRIVCard(FDP_obj):
        """Changes kinds into Health-RI VCards"""
        for field_name, _ in FDP_obj.model_fields.items():
            value = getattr(FDP_obj, field_name)
            if isinstance(value, Kind):
                setattr(FDP_obj, field_name, MetadataRecord._create_HRIVCard(value))
            elif isinstance(value, list) and any(isinstance(v, Kind) for v in value):
                new_card = []
                for kind in value:
                    if isinstance(kind, Kind):
                        new_card.append(MetadataRecord._create_HRIVCard(kind))
                    elif isinstance(kind, HRIVCard):
                        new_card.append(kind)
                    else:
                        raise ValueError("Encountered not Kind or VCard in list")
                setattr(FDP_obj, field_name, new_card)

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