import os
from dotenv import load_dotenv
import requests
from urllib.parse import urlparse, urlunparse
from fairmeta.metadata_model import MetadataRecord
from pydantic import AnyHttpUrl, Field
from sempyro.hri_dcat import HRICatalog, HRIDataset, HRIDistribution
from rdflib import DCTERMS, URIRef
import logging

class FDPCatalog(HRICatalog):
    is_part_of: [AnyHttpUrl] = Field(
        description="Link to parent object", 
        json_schema_extra={
            "rdf_term": DCTERMS.isPartOf, 
            "rdf_type": "uri"
        })

class RadboudFDP:
    def __init__(self, test=False):
        load_dotenv()
        self.test = test
        self.FDP_key = os.getenv("Radboud_FDP_key")
        self.base_url = "https://fdp.radboudumc.nl"
        if test:
            self.post_url = "https://fdp.radboudumc.nl/acc"
        else:
            self.post_url = self.base_url
        
    def create_and_publish(self, FDP: MetadataRecord, catalog_name: str):
        """Uploads an FDP object to Radboud FDP"""
        disallowed_fields = {"distribution", "dataset"}
        filtered_fields = {k: v for k, v in vars(FDP.catalog).items() if k not in disallowed_fields and v is not None}
        catalog = FDPCatalog(
            is_part_of=[URIRef(self.base_url)],
            dataset = [],
            **filtered_fields
        )
        fdp_catalog_record = catalog.to_graph(URIRef(f"{self.post_url}/catalog/{catalog_name}"))
        fdp_catalog_turtle = fdp_catalog_record.serialize(format="turtle")
        fdp_catalog_url = self._post(fdp_catalog_turtle, "catalog")

        for dataset in FDP.catalog.dataset:
            filtered_fields = {k: v for k, v in vars(dataset).items() if k not in disallowed_fields and v is not None}
            hri_dataset = HRIDataset(
                **filtered_fields
            )
            fdp_dataset_record = hri_dataset.to_graph(subject=URIRef(hri_dataset.identifier))
            fdp_dataset_record.add((URIRef(hri_dataset.identifier), DCTERMS.isPartOf, URIRef(fdp_catalog_url)))
            fdp_dataset_turtle = fdp_dataset_record.serialize(format="turtle")
            fdp_dataset_url = self._post(fdp_dataset_turtle, "dataset")

            self._publish(fdp_dataset_url)

            # # Cannot test this right now due to SHACLes on radboud FDP
            # for distribution in dataset:
            #     filtered_fields = {k: v for k, v in vars(distribution).items() if k not in disallowed_fields and v is not None}
            #     hri_distribution = HRIDistribution(
            #         **filtered_fields
            #     )
            #     access_url_str = str(hri_distribution.access_url)
            #     distribution_uri = URIRef(f"{hri_dataset.identifier}/distribution/{access_url_str.split('/')[-1]}")
            #     fdp_distribution_record = hri_distribution.to_graph(subject=distribution_uri)
            #     fdp_distribution_record.add((distribution_uri, DCTERMS.isPartOf, URIRef(f"{fdp_dataset_url}")))
            #     fdp_distribution_turtle = fdp_distribution_record.serialize(format="turtle")

            #     fdp_distribution_url = self._post(fdp_distribution_turtle, "distribution")
            #     self._publish(fdp_distribution_url)

        self._publish(fdp_catalog_url)

    def _post(self, turtle, location) -> str:
        url = f"{self.post_url}/{location}"
        headers = {
            'Authorization': f'Bearer {self.FDP_key}',
            'Content-Type': 'text/turtle'
        }
        rsp = requests.post(url, headers=headers, data=turtle, allow_redirects=True)
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
        logging.info(f"Published, this should be 200: {rsp}")
