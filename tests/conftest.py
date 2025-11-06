import pytest
from datetime import datetime, date, timezone, timedelta
from test_utils import extend_dict
from fairmeta import RadboudFDP
from dotenv import load_dotenv
import os
import isodate

load_dotenv()

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
                "modification_date": datetime.now(timezone.utc),
                "release_date": datetime.now(timezone.utc),
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
                        "language": ["Eng", "nld"],
                        "media_type": "https://www.iana.org/assignments/media-types/text/csv",
                        "modification_date": datetime.now(timezone.utc),
                        "packaging_format": "https://package_information.com",
                        "release_date": datetime.now(timezone.utc),
                        "status": "completed",
                        "temporal_resolution": str(timedelta(days=1)),
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
                    "modification_date": datetime.now(timezone.utc),
                    "number_of_records": 99,
                    "number_of_unique_individuals": 88,
                    "personal_data": "https://w3id.org/dpv/pd#Household",
                    "population_coverage": "Adults aged 18–65 diagnosed with type 2 diabetes in the Netherlands between 2015 and 2020",
                    "purpose": "https://w3id.org/dpv#CustomerManagement",
                    "release_date": datetime.now(timezone.utc),
                    "temporal_resolution": isodate.duration_isoformat(timedelta(days=1)),
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
        "challenge_url": "https://challenge.org/slug",
        "challenge_keywords": ["Medical", "keyword2"],
        "distribution_access_url": "https://testing.com/dist1",
        "distribution_size": 489,
        "distribution_format": "http://publications.europa.eu/resource/authority/file-type/PDF"
    }

@pytest.fixture
def extra_config():
    config = {
        "catalog": {
            "dataset": {
                "keyword": ["CT", "Prostate"],
                "maximum_typical_age": 999,
                "minimum_typical_age": 1,
                "distribution": {
                    "byte_size": 69
                }
            }
        }
    }
    return config

@pytest.fixture
def FDP():
    local_url = "http://localhost:8080"
    FDP = RadboudFDP(token=os.getenv("local_test_FDP_key"))
    FDP.base_url = local_url
    FDP.post_url = local_url
    return FDP
    