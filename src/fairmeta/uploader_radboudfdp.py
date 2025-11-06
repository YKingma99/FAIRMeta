import os
import requests
from urllib.parse import urlparse, urlunparse
from .metadata_model import MetadataRecord
from pydantic import AnyHttpUrl, Field
from sempyro.hri_dcat import HRICatalog, HRIDataset, HRIDistribution
from rdflib import DCTERMS, URIRef, Graph
import logging

class FDPCatalog(HRICatalog):
    is_part_of: [AnyHttpUrl] = Field(
        description="Link to parent object", 
        json_schema_extra={
            "rdf_term": DCTERMS.isPartOf, 
            "rdf_type": "uri"
        })

class RadboudFDP:
    def __init__(self, test=False, token=None):
        self.test = test
        if token:
            self.FDP_key = token
        else:
            self.FDP_key = os.getenv("Radboud_FDP_key")
        self.base_url = "https://fdp.radboudumc.nl"
        if test:
            self.post_url = "https://fdp.radboudumc.nl/acc"
        else:
            self.post_url = self.base_url
        

    def create_and_publish(self, metadata_record: MetadataRecord, catalog_name: str):
        """Uploads a MetadataRecord object to Radboud FDP"""
        disallowed_fields = {"distribution", "dataset"}
        filtered_fields = {k: v for k, v in vars(metadata_record.catalog).items() if k not in disallowed_fields and v is not None}
        catalog = FDPCatalog(
            is_part_of=[URIRef(self.base_url)],
            dataset = [],
            **filtered_fields
        )
        metadata_catalog_record = catalog.to_graph(URIRef(f"{self.post_url}/catalog/{catalog_name}"))
        metadata_catalog_turtle = metadata_catalog_record.serialize(format="turtle")
        metadata_catalog_url = self._post(metadata_catalog_turtle, "catalog")

        for dataset in metadata_record.catalog.dataset:
            filtered_fields = {k: v for k, v in vars(dataset).items() if k not in disallowed_fields and v is not None}
            hri_dataset = HRIDataset(
                **filtered_fields
            )
            metadata_dataset_record = hri_dataset.to_graph(subject=URIRef(hri_dataset.identifier))
            metadata_dataset_record.add((URIRef(hri_dataset.identifier), DCTERMS.isPartOf, URIRef(metadata_catalog_url)))
            metadata_dataset_turtle = metadata_dataset_record.serialize(format="turtle")
            metadata_dataset_url = self._post(metadata_dataset_turtle, "dataset")

            # Cannot test this due to SHACLs: byteSize gives DatatypeConstraintComponent* and title gives MinCountConstraintComponent (Even though it is not mandatory)
            # in SeMPyRO, byteSize is xsd:integer, I think defining it as xsd:nonnegativeinteger would immediately solve this problem.
            # for distribution in dataset.distribution:
            #     filtered_fields = {k: v for k, v in vars(distribution).items() if k not in disallowed_fields and v is not None}
            #     hri_distribution = HRIDistribution(
            #         **filtered_fields
            #     )
            #     access_url_str = str(hri_distribution.access_url)
            #     distribution_uri = URIRef(f"{hri_dataset.identifier}/distribution/{access_url_str.split('/')[-1]}")
            #     metadata_distribution_record = hri_distribution.to_graph(subject=distribution_uri)
            #     metadata_distribution_record.add((distribution_uri, DCTERMS.isPartOf, URIRef(f"{metadata_dataset_url}")))
            #     metadata_distribution_turtle = metadata_distribution_record.serialize(format="turtle")

            #     metadata_distribution_url = self._post(metadata_distribution_turtle, "distribution")
            #     self._publish(metadata_distribution_url)

            self._publish(metadata_dataset_url)

        self._publish(metadata_catalog_url)


    def update(self, target: str, metadata_record: MetadataRecord, url: str, pointer_url):
        disallowed_fields = {"distribution", "dataset"}            
        match target:
            case "catalog":
                filtered_fields = {k: v for k, v in vars(metadata_record).items() if k not in disallowed_fields and v is not None}
                catalog = FDPCatalog(
                    is_part_of=[URIRef(self.base_url)],
                    dataset = [pointer_url],
                    **filtered_fields
                )
                metadata_catalog_record = catalog.to_graph(URIRef(url))
                metadata_catalog_turtle = metadata_catalog_record.serialize(format="turtle")
                rsp = self._put(metadata_catalog_turtle, url)

            case "dataset":
                filtered_fields = {k: v for k, v in vars(metadata_record).items() if k not in disallowed_fields and v is not None}
                hri_dataset = HRIDataset(
                    **filtered_fields
                )
                metadata_dataset_record = hri_dataset.to_graph(subject=URIRef(url))
                metadata_dataset_record.add((URIRef(url), DCTERMS.isPartOf, URIRef(pointer_url)))
                metadata_dataset_turtle = metadata_dataset_record.serialize(format="turtle")
                rsp = self._put(metadata_dataset_turtle, url)

            case _:
                raise ValueError(f"Target: {target} invalid")

        logging.info(f"Updating: {target}, response (should be 200): {rsp}")
        return rsp


    def _post(self, turtle, location) -> str:
        url = f"{self.post_url}/{location}"
        headers = {
            'Authorization': f'Bearer {self.FDP_key}',
            'Content-Type': 'text/turtle'
        }
        rsp = requests.post(url, headers=headers, data=turtle, allow_redirects=True)
        rsp.raise_for_status()
        logging.info(f"Posting: {location}, response (should be 201): {rsp}")
        return rsp.headers["Location"]
    

    def _publish(self, url):
        if self.test:
            parsed = urlparse(url)
            new_path = "/acc" + parsed.path
            url = urlunparse(parsed._replace(path=new_path))

        publish_url = f"{url}/meta/state"
        headers = {
            'Authorization': f'Bearer {self.FDP_key}',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        json_data = {
            'current': 'PUBLISHED'
        }
        rsp = requests.put(url=publish_url, headers=headers, json=json_data)
        rsp.raise_for_status()
        logging.info(f"Published, this should be 200: {rsp}")

    
    def _put(self, turtle, url):
        if self.test:
            parsed = urlparse(url)
            new_path = "/acc" + parsed.path
            url = urlunparse(parsed._replace(path=new_path))

        headers = {
                'Authorization': f'Bearer {self.FDP_key}',
                'Accept': 'text/turtle',
                'Content-Type': 'text/turtle'
            }
        
        rsp = requests.put(url, headers=headers, data=turtle)
        rsp.raise_for_status()
        return rsp

    
