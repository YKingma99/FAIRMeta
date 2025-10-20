import pytest
from fairmeta import metadata_model, schema_definitions_hri
from fairmeta.metadata_model import MetadataRecord
from pydantic import ValidationError
from sempyro.hri_dcat import HRIAgent, HRIVCard, HRIDataset, HRIDistribution
from sempyro.geo import Location
from rdflib import URIRef
from test_utils import FDPCatalog, resolve_path, adapted_instance, is_list_field

@pytest.mark.parametrize("target,path,value,exception",[(None, None, None, None), # Minimal record
                                                        ("config", ("catalog", "dataset", "contact_point"), "not kind or card", AttributeError),
                                                        ("config", ("catalog", "publisher"), "not agent or HRIAgent", AttributeError),
                                                        ("config", ("catalog", "contact_point", "fn"), "", ValueError), # Empty string in mandatory field
                                                        ("api_data", (["challenge_title"]), [], ValueError), # Empty list in mandatory field
                                                        ("config", ("catalog", "dataset", "contact_point", "fn"), 'something "in quotes" \'excapedquo\'beep', None), # Quotes in string in config file
                                                        ("config", ("catalog", "homepage"), "", None),
                                                        ("config", ("catalog", "contact_point", "hasEmail"), "mailto:email@org.org", None)])
def test_metadata_model_creation(target, config, api_data, path, value, exception):
    """General test for creating a valid instance of the metadata schema"""
    if exception:
        with pytest.raises(exception) as exc_info:
            schema = adapted_instance(target, config, api_data, path, value)
            schema.validate()
            match target:
                case "config":
                    target = resolve_path(schema, path[:-1], target, config)
                    target = getattr(target, path[-1])
                case "api_data":
                    target, _ = resolve_path(schema, path, target, config)
                case _:
                    pass       
            if value:
                assert target == value
            else:
                assert target == None            
        print(exc_info)
        
    else:
        schema = adapted_instance(target, config, api_data, path, value)
        assert isinstance(schema, MetadataRecord)
        extended_keywords = config["catalog"]["dataset"]["keyword"] + api_data["challenge_keywords"]
        assert schema.catalog.dataset.keyword == extended_keywords
        match target:
            case "config":
                target = resolve_path(schema, path[:-1], target, config)
                target = getattr(target, path[-1])
                assert schema.api_data == api_data                
            case "api_data":
                target, _ = resolve_path(schema, path, target, config)
                assert schema.config == config
            case _:
                assert schema.config == config
                assert schema.api_data == api_data       
        if value:
            assert target == value
        else:
            assert target == None
        schema.validate()
        # Field tests?

@pytest.mark.parametrize("target,path,value,exception",[("config", ("catalog", "contact_point", "fn"),123,ValidationError), 
                                                        ("config", ("catalog", "contact_point", "fn"),True,ValidationError), 
                                                        ("config", ("catalog", "contact_point", "fn"),None,ValueError),
                                                        ("config", ("catalog", "contact_point", "fn"),"Firstname Lastname",None),
                                                        ("api_data", (["archive_title"]), 123, ValidationError),
                                                        ("api_data", (["distribution_size"]), "drie", ValidationError),
                                                        ("api_data", (["archive_title"]), "title", None)])
def test_data_types(target, config, api_data, path, value, exception):
    """Tests if basic types are enforced correctly"""
    schema = adapted_instance(target, config, api_data, path, value)
    if exception:
        with pytest.raises(exception) as exc_info:
            schema.validate() # Bad error: if type is ambigious (Kind | HRIVCard for example) it prints errors for both
        print(exc_info)
    else:
        schema.validate()

@pytest.mark.parametrize("target,path,value,exception,message",[("config", ("catalog", "dataset", "theme"), ["HEAL"], None, None),
                                                        ("config", ("catalog", "dataset", "theme"), ["INVALID_THEME"], ValueError, f"INVALID_THEME incorrect or not supported. Supported values: {', '.join(metadata_model.themes.keys())}"),
                                                        ("config", ("catalog", "dataset", "access_rights"), "public", None, None),
                                                        ("config", ("catalog", "dataset", "access_rights"), "NOT_ALLOWED", ValueError, f"NOT_ALLOWED incorrect or not supported. Supported values: {', '.join(metadata_model.access_rights.keys())}"),
                                                        ("config", ("catalog", "dataset", "theme"), "HEAL", None, None),
                                                        ("config", ("catalog", "dataset", "theme"), "BAD", ValueError, None),
                                                        ("config", ("catalog", "language"), "Eng", None, None),
                                                        ("config", ("catalog", "language"), "En", ValueError, None),
                                                        ("config", ("catalog", "language"), "https://example.com", ValueError, None),
                                                        ("config", ("catalog", "language"), "http://publications.europa.eu/resource/authority/language/anything", None, None), # Not correct but shouldn't raise error
                                                        ("config", ("catalog", "dataset", "frequency"), "quarterly", None, None),
                                                        ("config", ("catalog", "dataset", "frequency"), "dagelijks", ValueError, None),
                                                        ("config", ("catalog", "license"), "cc_byncsa_30", None, None),
                                                        ("config", ("catalog", "license"), "Apache-2.0", ValueError, None),
                                                        ("config", ("catalog", "dataset", "distribution", "status"), "develoP", None, None),
                                                        ("config", ("catalog", "dataset", "distribution", "status"), "asfd", ValueError, None)])
def test_string_to_enum(target, config, api_data, path, value, exception, message):
    """Tests string_to_enum function"""
    schema = adapted_instance(target, config, api_data, path, value)
    if exception:
        if message:
            with pytest.raises(exception, match=message):
                MetadataRecord._string_to_enum(schema)
        else:
            with pytest.raises(exception) as exc_info:
                MetadataRecord._string_to_enum(schema) # Prints good error
            print(exc_info)
    else:
        MetadataRecord._string_to_enum(schema)

@pytest.mark.parametrize("target,path,value,exception",[("config", ("catalog", "contact_point", "hasEmail"), "name", ValueError),
                                                        ("config", ("catalog", "contact_point", "hasEmail"), "email@org.com", None),
                                                        ("config", ("catalog", "dataset", "contact_point", "fn"), "anyone", None),
                                                        ("config", ("catalog", "dataset", "contact_point", "hasUrl"), ["https://example.com"], None),
                                                        ("config", ("catalog", "contact_point", "hasUrl"), ["no url"], ValueError),
                                                        ("config", ("catalog", "contact_point", "hasUrl"), None, None),
                                                        ("config", ("catalog", "contact_point"), [schema_definitions_hri.Kind(hasEmail="email@email.com", fn="name"), HRIVCard(hasEmail="email@email.com", formatted_name="name")], None),
                                                        ("config", ("catalog", "contact_point"), [schema_definitions_hri.Kind(hasEmail="email@email.com", fn="name"), HRIVCard(hasEmail="email@email.com", formatted_name="name"), "random"], ValueError)])
def test_kind_to_hrivcard(target, config, api_data, path, value, exception):
    """Tests the transformation function from Kind to HRIVCard"""
    schema = adapted_instance(target, config, api_data, path, value)
    if exception:
        with pytest.raises(exception) as exc_info:
            MetadataRecord._kind_to_HRIVCard(schema) # Prints good error
        print(exc_info)
    else:
        MetadataRecord._kind_to_HRIVCard(schema)
        target = resolve_path(schema, path[:-1], target, config)
        try:
            assert isinstance(target, HRIVCard)
        except:
            target = getattr(target, path[-1])
            assert all(isinstance(t, HRIVCard) for t in target)

        if path[-1] == "hasUrl" and value is not None:
            if isinstance(target, list):
                for t in target:
                    assert t.contact_page is not None
            else:
                assert target.contact_page is not None

@pytest.mark.parametrize("target,path,value,exception",[("config", ("catalog", "publisher", "mbox"), "name", ValueError), # No email in mailbox
                                                        ("config", ("catalog", "publisher", "homepage"), "no link", ValueError), # No link in homepage
                                                        ("config", ("catalog", "dataset", "creator", "type"), "typen", ValueError), # No link in type
                                                        ("config", ("catalog", "dataset", "creator", "type"), "https://typen.com", None),
                                                        ("config", ("catalog", "dataset", "creator", "spatial"), ["https://Nijmegen.com"], None),
                                                        ("config", ("catalog", "dataset", "creator", "spatial"), ["Nijmegen"], ValueError), # No link in location
                                                        ("config", ("catalog", "dataset", "creator", "spatial"), [Location(geometry="https://Nijmegen.com")], None),                                                    
                                                        ("config", ("catalog", "publisher", "identifier"), ["identification"], None),
                                                        ("config", ("catalog", "publisher", "publisher_type"), None, None),
                                                        ("config", ("catalog", "dataset", "publisher", "publisher_type"), "https://publishertype.com", None),
                                                        ("config", ("catalog", "dataset", "publisher", "publisher_type"), ["https://publishertype.com"], ValueError), # Publisher type in a list when it's not supposed to be
                                                        ("config", ("catalog", "publisher"), [metadata_model.Agent(mbox="dummy@email.com",identifier=["id"],name=["name"],homepage="https://pagina.nl"), HRIAgent(name=["name"],identifier=["id"],mbox="email@email.com",homepage="https://pagina.nl")], None),
                                                        ("config", ("catalog", "publisher"), [metadata_model.Agent(mbox="dummy@email.com",identifier=["id"],name=["name"],homepage="https://pagina.nl"), HRIAgent(name=["name"],identifier=["id"],mbox="email@email.com",homepage="https://pagina.nl"), "random"], ValueError)]) # Not Agent in list
def test_agent_to_hriagent(target, config, api_data, path, value, exception):
    """Tests the tranformation function from Agent to HRIAgent"""
    schema = adapted_instance(target, config, api_data, path, value)
    if exception:
        with pytest.raises(exception) as exc_info:
            MetadataRecord._agent_to_HRIAgent(schema) # Prints good error
        print(exc_info)
    else:
        MetadataRecord._agent_to_HRIAgent(schema)
        target = resolve_path(schema, path[:-1], target, config)
        try:
            assert isinstance(target, HRIAgent) 
        except:
            target = getattr(target, path[-1])
            assert all(isinstance(t, HRIAgent) for t in target)

        if path[-1] == "type":
            assert target.type is not None
        elif path[-1] == "spatial":
            assert target.spatial is not None

@pytest.mark.parametrize("target,path,value,exception,message",[("api_data", (["challenge_title"]), None, ValueError, "Likely put null or null equivalent value in required field"),
                                                        ("api_data", (["challenge_title"]), "title", None, None),
                                                        ("config", ("catalog", "license"), None, None, None),
                                                        ("config", ("catalog", "license"), "cc0", None, None),
                                                        ("config", ("catalog", "dataset", "contact_point", "fn"), None, ValueError, "Likely put null or null equivalent value in required field"),
                                                        ("config", ("catalog", "contact_point", "fn"), "", ValueError, None),
                                                        ("api_data", (["challenge_title"]), [], ValueError, "Likely put null or null equivalent value in required field"),
                                                        ("config", ("catalog", "license"), "", None, None)])
def test_drop_none(target, config, api_data, path, value, exception,message):
    """Tests if drop_none function removes null-equivalent values and doesn't interfere with validation function"""
    schema = adapted_instance(target, config, api_data, path, value)
    if exception:
        if message:
            with pytest.raises(exception, match=message):
                schema.validate()
        else:
            with pytest.raises(exception) as exc_info:
                schema.validate() # Good error
            print(exc_info)
    else:
        schema.validate()
        match target:
            case "config":
                target = resolve_path(schema, path[:-1], target, config)
                target = getattr(target, path[-1])
            case "api_data":
                target, _ = resolve_path(schema, path, target, config)
            case _:
                pass       
        if value:
            assert target == value
        else:
            assert target == None

@pytest.mark.parametrize("target,path,value,exception,message",[("config", ("catalog", "dataset", "applicable_legislation"), "legislature", None, None),
                                                        ("config", ("catalog", "dataset", "applicable_legislation"), ["https://license.com"], None, None),
                                                        ("config", ("catalog", "dataset", "purpose"), None, None, None),
                                                        ("config", ("catalog", "dataset", "purpose"), "purposefield", None, None),
                                                        ("config", ("catalog", "dataset", "purpose"), ["purpose field", "purpose_2"], None, None),
                                                        ("api_data", (["challenge_url"]), ["idee"], None, None), # Warning?
                                                        ("api_data", (["challenge_url"]), ["idee2", "illegal_id"], TypeError, "Found list where it is not supposed to be: identifier")])
def test_ensure_lists(target, config, api_data, path, value, exception, message):  
    """Tests the function that creates lists where it needs to be, and removes lists where they don't need to be"""  
    schema = adapted_instance(target, config, api_data, path, value)
    if exception:
        if message:
            with pytest.raises(exception, match=message):
                MetadataRecord._ensure_lists(schema)
        else:
            with pytest.raises(exception) as exc_info:
                MetadataRecord._ensure_lists(schema) # Prints good error
            print(exc_info)
    else:
        MetadataRecord._ensure_lists(schema)
        match target:
            case "config":
                target = resolve_path(schema, path[:-1], target, config)
                target = getattr(target, path[-1])
                list_type = is_list_field(schema, path)
            case "api_data":
                target, internal_path = resolve_path(schema, path, target, config)
                list_type = is_list_field(schema, internal_path)
            case _:
                pass
        if list_type and value is not None:
            assert isinstance(target, list)
        else:
            assert not isinstance(target, list)

@pytest.mark.parametrize("target,path,value,exception",[(None, None, None, None),])
def test_transformation_hri(target, config, api_data, path, value, exception):
    """Tests transformation from unprocessed MetadataRecord to HRI schema"""
    schema = adapted_instance(target, config, api_data, path, value)
    schema.transform_schema()
    disallowed_fields = {"distribution", "dataset"}
    filtered_fields = {k: v for k, v in vars(schema.catalog).items() if k not in disallowed_fields and v is not None}
    catalog = FDPCatalog(
        is_part_of=[URIRef("https://test.com")],
        dataset=[],
        **filtered_fields)
    for dataset in schema.catalog.dataset:
        filtered_fields = {k: v for k, v in vars(dataset).items() if k not in disallowed_fields and v is not None}
        hri_dataset = HRIDataset(
            **filtered_fields
        )
        for distribution in dataset.distribution:
            filtered_fields = {k: v for k, v in vars(distribution).items() if k not in disallowed_fields and v is not None}
            hri_distribution = HRIDistribution(
                **filtered_fields
            )
