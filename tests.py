import pytest
import metadata_model
from metadata_model import MetadataRecord
from copy import deepcopy
from pydantic import ValidationError, AnyHttpUrl, Field
from sempyro.hri_dcat import HRIAgent, HRIVCard, HRICatalog, HRIDataset, HRIDistribution
from sempyro.geo import Location
from rdflib import DCTERMS, URIRef
from datetime import datetime
from sempyro.dcat import AccessRights
from sempyro.time import PeriodOfTime
from gatherers.gather_GC import GrandChallenge
from httpx import HTTPStatusError, MockTransport, Request, Response
from gcapi import Client

class FDPCatalog(HRICatalog):
    is_part_of: [AnyHttpUrl] = Field(
        description="Link to parent object", 
        json_schema_extra={
            "rdf_term": DCTERMS.isPartOf, 
            "rdf_type": "uri"
        })
    
def extend_dict(dictionary: dict, extension: dict) -> dict:
    for key, value in extension.items():
        if key in dictionary and isinstance(dictionary[key], dict) and isinstance(value, dict):
            extend_dict(dictionary[key], value)
        else:
            dictionary[key] = value
    return dictionary

@pytest.fixture(params=["minimal", "full"])
def config(request):
    config = {
        "catalog": {
            "mapping": {
                "challenge_description": ["description"],
                "challenge_title": ["title"]
            }, "contact_point": {
                "hasEmail": "test@testing.com",
                "fn": "David Tester"
            }, "publisher": {
                "mbox": "publisher@publishing.com",
                "identifier": ["identification"],
                "name": ["uitgever"],
                "homepage": "https://uitgeverij.nl"
            }, "license": "cc0",
        
            "dataset": {
                "mapping": {
                    "archive_description": ["description"],
                    "archive_title": ["title"],
                    "challenge_url": ["identifier"],
                    "challenge_keywords": ["keyword"]
                }, "access_rights": "non_public",
                "contact_point": {
                    "hasEmail": "support@test.com",
                    "fn": "testing support"
                }, "creator": {
                    "mbox": "person@testing.com",
                    "identifier": ["test person identifier"],
                    "name": ["datasetmaker"],
                    "homepage": "https://datasetmaker.org"
                }, "publisher": {
                    "mbox": "datapublisher@publishing.com",
                    "identifier": ["identification data"],
                    "name": ["uitgever data"],
                    "homepage": "https://uitgeverij.nl/data"
                }, "keyword": ["Test platform"],
                "theme": ["HEAL"],
                "applicable_legislation": "https://www.legislation.com",
        
                "distribution": {
                    "mapping": {
                        "distribution_access_url": ["access_url"],
                        "distribution_size": ["byte_size"],
                        "distribution_format": ["format"]
                    }, "license": "cc0",
                    "rights": "https://www.example.com/contracts/definitely_a_real_DPA.pdf",
                }
            }
        }
    }
    if request.param == "full":
        extended_config = {
            "catalog": {
                "publisher": {
                    "spatial": ["http://publications.europa.eu/resource/authority/country/NLD"],
                    "publisher_note": "Notitie",
                    "publisher_type": "http://purl.org/adms/publishertype/Academia-ScientificOragnisation",
                },
                "applicable_legislation": "https://www.legislation.com",
                "creator": {
                    "mbox": "catalog@testing.com",
                    "identifier": ["catalogtest person identifier"],
                    "name": ["catalogmaker"],
                    "homepage": "https://catalogmaker.org"
                }, "geographical_coverage": "https://www.geonames.org/countries/NL/the-netherlands.html",
                "homepage": "https://homepage.org",
                "language": "eng",
                "license": "cc0",
                "modification_date": datetime.now(),
                "release_date": datetime.now(),
                "rights": "https://www.websitewithfreetextrights.com",
                # "temporal_coverage": PeriodOfTime(start_date=datetime.now(), end_date=datetime.now()),
                "dataset": {
                    "code_values": "https://www.wikidata.org/wiki/Q32566",
                    "coding_system": "https://www.wikidata.org/wiki/Q81095",
                    "conforms_to": "https://www.wikidata.org/wiki/Q81095",
                    "distribution": {
                        "applicable_legislation": "https://www.legislation.com",
                        "compression_format": "https://www.iana.org/assignments/media-types/application/zip",
                        "description": ["Description of the distribution", "Description in another language"],
                        "documentation": "https://documentation.com",
                        "download_url": "https://google.com",
                        "language": ["Eng", "nLD"],
                        "media_type": "https://www.iana.org/assignments/media-types/text/csv",
                        "modification_date": datetime.now(),
                        "packaging_format": "https://package_information.com",
                        "release_date": datetime.now(),
                        "status": "completed",
                        "temporal_resolution": "3",
                        "title": ["title of distribution"]
                    },
                    "frequency": "daily",
                    "purpose": "https://purpose.com",
                    "geographical_coverage": "https://nijmegen.nl",
                    "is_referenced_by": "https://doi.org",
                    "language": "nld",
                    "legal_basis": "InformedConsent",
                    "maximum_typical_age": 55,
                    "minimum_typical_age": 29,
                    "modification_date": datetime.now(),
                    "number_of_records": 99,
                    "number_of_unique_individuals": 88,
                    "personal_data": "https://w3id.org/dpv/pd#Household",
                    "population_coverage": "Adults aged 18–65 diagnosed with type 2 diabetes in the Netherlands between 2015 and 2020",
                    "purpose": "https://w3id.org/dpv#CustomerManagement",
                    "release_date": datetime.now(),
                    "temporal_resolution": "3",
                    "type": "https://www.type.nl",
                    "status": "withdrawn",
                    "version": "1",
                    "version_notes": ["changed nothing", "still nothing"],
                    "was_generated_by": "https://me.nl"
                }
            }
        }
        config = extend_dict(config, extended_config)

    return config

@pytest.fixture
def api_data():
    return {
        "challenge_description": "Description given by challenge",
        "challenge_title": "Title given by challenge",
        "archive_description": "Description given by archive",
        "archive_title": "Title given by archive",
        "challenge_url": "url of the challenge",
        "challenge_keywords": ["Medical", "keyword2"],
        "distribution_access_url": "https://testing.com/dist1",
        "distribution_size": 489,
        "distribution_format": "http://publications.europa.eu/resource/authority/file-type/PDF"
    }

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
            return MetadataRecord.create_FDP_instance(config, api_data)
    
    d = adapted_data
    for key in path[:-1]:
        d = d[key]
    d[path[-1]] = value
    
    if target == "config":
        return MetadataRecord.create_FDP_instance(adapted_data, api_data)
    else:
        return MetadataRecord.create_FDP_instance(config, adapted_data)

@pytest.mark.parametrize("target,path,value,exception",[(None, None, None, None), # Tests if everything is correct
                                                        ("config", ("catalog", "dataset", "contact_point"), "not kind or card", AttributeError),
                                                        ("config", ("catalog", "publisher"), "not agent or HRIAgent", AttributeError),
                                                        ("config", ("catalog", "contact_point", "fn"), "", ValueError), # Empty string in mandatory field
                                                        ("api_data", (["challenge_title"]), [], ValueError), # Empty list in mandatory field
                                                        ("config", ("catalog", "dataset", "contact_point", "fn"), 'something "in quotes" \'excapedquo\'beep', None), # Quotes in string in config file
                                                        ("config", ("catalog", "homepage"), "", None),
                                                        ("config", ("catalog", "contact_point", "hasEmail"), "mailto:email@org.org", None)])
def test_FDP_creation(target, config, api_data, path, value, exception):
    if exception:
        with pytest.raises(exception) as exc_info:
            fdp = adapted_instance(target, config, api_data, path, value)
            fdp.validate()
            match target:
                case "config":
                    target = resolve_path(fdp, path[:-1], target, config)
                    target = getattr(target, path[-1])
                case "api_data":
                    target, _ = resolve_path(fdp, path, target, config)
                case _:
                    pass       
            if value:
                assert target == value
            else:
                assert target == None            
        print(exc_info)
        
    else:
        fdp = adapted_instance(target, config, api_data, path, value)
        assert isinstance(fdp, MetadataRecord)
        extended_keywords = config["catalog"]["dataset"]["keyword"] + api_data["challenge_keywords"]
        assert fdp.catalog.dataset.keyword == extended_keywords
        match target:
            case "config":
                target = resolve_path(fdp, path[:-1], target, config)
                target = getattr(target, path[-1])
                assert fdp.api_data == api_data                
            case "api_data":
                target, _ = resolve_path(fdp, path, target, config)
                assert fdp.config == config
            case _:
                assert fdp.config == config
                assert fdp.api_data == api_data       
        if value:
            assert target == value
        else:
            assert target == None
        fdp.validate()
        # Field tests?

@pytest.mark.parametrize("target,path,value,exception",[("config", ("catalog", "contact_point", "fn"),123,ValidationError), 
                                                        ("config", ("catalog", "contact_point", "fn"),True,ValidationError), 
                                                        ("config", ("catalog", "contact_point", "fn"),None,ValueError),
                                                        ("config", ("catalog", "contact_point", "fn"),"Firstname Lastname",None),
                                                        ("api_data", (["archive_title"]), 123, ValidationError),
                                                        ("api_data", (["distribution_size"]), "drie", ValidationError),
                                                        ("api_data", (["archive_title"]), "title", None)])
def test_data_types(target, config, api_data, path, value, exception):
    fdp = adapted_instance(target, config, api_data, path, value)
    if exception:
        with pytest.raises(exception) as exc_info:
            fdp.validate() # Bad error: if type is ambigious (Kind | HRIVCard for example) it prints errors for both
        print(exc_info)
    else:
        fdp.validate()

@pytest.mark.parametrize("target,path,value,exception,message",[("config", ("catalog", "dataset", "theme"), ["HEAL"], None, None),
                                                        ("config", ("catalog", "dataset", "theme"), ["INVALID_THEME"], ValueError, f"INVALID_THEME incorrect or not supported. Supported values: {', '.join(metadata_model.themes.keys())}"),
                                                        ("config", ("catalog", "dataset", "access_rights"), "public", None, None),
                                                        ("config", ("catalog", "dataset", "access_rights"), "NOT_ALLOWED", ValueError, f"NOT_ALLOWED incorrect or not supported. Supported values: {', '.join(metadata_model.access_rights.keys())}"),
                                                        ("config", ("catalog", "dataset", "theme"), "HEAL", None, None),
                                                        ("config", ("catalog", "dataset", "theme"), "BAD", ValueError, None),
                                                        ("config", ("catalog", "language"), "Eng", None, None),
                                                        ("config", ("catalog", "language"), "En", ValueError, None),
                                                        ("config", ("catalog", "dataset", "frequency"), "quarterly", None, None),
                                                        ("config", ("catalog", "dataset", "frequency"), "dagelijks", ValueError, None)])
def test_string_to_enum(target, config, api_data, path, value, exception, message):
    fdp = adapted_instance(target, config, api_data, path, value)
    if exception:
        if message:
            with pytest.raises(exception, match=message):
                MetadataRecord._string_to_enum(fdp)
        else:
            with pytest.raises(exception) as exc_info:
                MetadataRecord._string_to_enum(fdp) # Prints good error
            print(exc_info)
    else:
        MetadataRecord._string_to_enum(fdp)

@pytest.mark.parametrize("target,path,value,exception",[("config", ("catalog", "contact_point", "hasEmail"), "name", ValueError),
                                                        ("config", ("catalog", "contact_point", "hasEmail"), "email@org.com", None),
                                                        ("config", ("catalog", "dataset", "contact_point", "fn"), "anyone", None),
                                                        ("config", ("catalog", "dataset", "contact_point", "hasUrl"), ["https://example.com"], None),
                                                        ("config", ("catalog", "contact_point", "hasUrl"), ["no url"], ValueError),
                                                        ("config", ("catalog", "contact_point", "hasUrl"), None, None),])
                                                        # ("config", ("catalog", "contact_point"), [fdpData.Kind(hasEmail="email@email.com", fn="name"), HRIVCard(hasEmail="email@email.com", formatted_name="name")], None),
                                                        # ("config", ("catalog", "contact_point"), [fdpData.Kind(hasEmail="email@email.com", fn="name"), HRIVCard(hasEmail="email@email.com", formatted_name="name"), "random"], ValueError)])
def test_kind_to_hrivcard(target, config, api_data, path, value, exception):
    fdp = adapted_instance(target, config, api_data, path, value)
    if exception:
        with pytest.raises(exception) as exc_info:
            MetadataRecord._kind_to_HRIVCard(fdp) # Prints good error
        print(exc_info)
    else:
        MetadataRecord._kind_to_HRIVCard(fdp)
        target = fdp
        for key in path[:-1]:
            target = getattr(target, key)
        assert isinstance(target, HRIVCard)
        if path[-1] == "hasUrl" and value is not None:
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
    fdp = adapted_instance(target, config, api_data, path, value)
    if exception:
        with pytest.raises(exception) as exc_info:
            MetadataRecord._agent_to_HRIAgent(fdp) # Prints good error
        print(exc_info)
    else:
        MetadataRecord._agent_to_HRIAgent(fdp)
        target = resolve_path(fdp, path[:-1], target, config)
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
    fdp = adapted_instance(target, config, api_data, path, value)
    if exception:
        if message:
            with pytest.raises(exception, match=message):
                fdp.validate()
        else:
            with pytest.raises(exception) as exc_info:
                fdp.validate() # Good error
            print(exc_info)
    else:
        fdp.validate()
        match target:
            case "config":
                target = resolve_path(fdp, path[:-1], target, config)
                target = getattr(target, path[-1])
            case "api_data":
                target, _ = resolve_path(fdp, path, target, config)
            case _:
                pass       
        if value:
            assert target == value
        else:
            assert target == None

def is_list_field(model: MetadataRecord, path):
    """Helper function to decide if a field should be a list"""
    for key in path[:-1]:
        model = getattr(model, key)
        if isinstance(model, list):
            model = model[0]
    field = model.__class__.model_fields[path[-1]]
    return 'List' in str(field.annotation)

@pytest.mark.parametrize("target,path,value,exception,message",[("config", ("catalog", "dataset", "applicable_legislation"), "legislature", None, None),
                                                        ("config", ("catalog", "dataset", "applicable_legislation"), ["https://license.com"], None, None),
                                                        ("config", ("catalog", "dataset", "purpose"), None, None, None),
                                                        ("config", ("catalog", "dataset", "purpose"), "purposefield", None, None),
                                                        ("config", ("catalog", "dataset", "purpose"), ["purpose field", "purpose_2"], None, None),
                                                        ("api_data", (["challenge_url"]), ["idee"], None, None), # Warning?
                                                        ("api_data", (["challenge_url"]), ["idee2", "illegal_id"], TypeError, "Found list where it is not supposed to be: identifier")])
def test_ensure_lists(target, config, api_data, path, value, exception, message):    
    fdp = adapted_instance(target, config, api_data, path, value)
    if exception:
        if message:
            with pytest.raises(exception, match=message):
                MetadataRecord._ensure_lists(fdp)
        else:
            with pytest.raises(exception) as exc_info:
                MetadataRecord._ensure_lists(fdp) # Prints good error
            print(exc_info)
    else:
        MetadataRecord._ensure_lists(fdp)
        match target:
            case "config":
                target = resolve_path(fdp, path[:-1], target, config)
                target = getattr(target, path[-1])
                list_type = is_list_field(fdp, path)
            case "api_data":
                target, internal_path = resolve_path(fdp, path, target, config)
                list_type = is_list_field(fdp, internal_path)
            case _:
                pass
        if list_type and value is not None:
            assert isinstance(target, list)
        else:
            assert not isinstance(target, list)

@pytest.mark.parametrize("target,path,value,exception",[(None, None, None, None),])
def test_transformation_hri(target, config, api_data, path, value, exception):
    fdp = adapted_instance(target, config, api_data, path, value)
    fdp.transform_FDP()
    disallowed_fields = {"distribution", "dataset"}
    filtered_fields = {k: v for k, v in vars(fdp.catalog).items() if k not in disallowed_fields and v is not None}
    catalog = FDPCatalog(
        is_part_of=[URIRef("https://test.com")],
        dataset=[],
        **filtered_fields)
    for dataset in fdp.catalog.dataset:
        filtered_fields = {k: v for k, v in vars(dataset).items() if k not in disallowed_fields and v is not None}
        hri_dataset = HRIDataset(
            **filtered_fields
        )
        for distribution in dataset.distribution:
            filtered_fields = {k: v for k, v in vars(distribution).items() if k not in disallowed_fields and v is not None}
            hri_distribution = HRIDistribution(
                **filtered_fields
            )

@pytest.mark.parametrize("slug,status_code,exception",[("LUNA16", 200, None),
                                                       ("weird", 404, HTTPStatusError)])
def test_gather_gc_data(monkeypatch, slug, status_code, exception):
    def fake_get(self, path, **kwargs) -> Response:
        if status_code == 200:
            content = {"name": slug, "pk":1}
        else:
            content = {"detail": "Not found"}
        return Response(status_code=status_code, json=content, request=Request("GET", f"https://grand-challenge.org/api/v1/challenges{path}"))
    
    monkeypatch.setattr(Client, "get", fake_get)

    class FakeArchive:
        def __init__(self):
            self.pk = 2
            self.slug = slug.lower()
    
    class FakeArchives:
        def detail(self, slug):
            return FakeArchive()
        
    # class FakeImages:
    #     def iterate_all(self, params):
    #         return iter([{"id": "img1"}])
        
    #     def list(self, params):
    #         return [{"id": "img1"}, {"id": "img2"}]
        
    def fake_init(self):
        self.client = Client(token="token")
        self.client.archives = FakeArchives()
        # self.client.images = FakeImages()

    monkeypatch.setattr("gatherers.gather_GC.GrandChallenge.__init__", fake_init)

    platform = GrandChallenge()
    if exception:
        with pytest.raises(exception):
            platform._gather_challenge(f"/{slug}")
    else:
        archive_data = platform._gather_challenge(f"/{slug}")
        assert isinstance(archive_data, dict)