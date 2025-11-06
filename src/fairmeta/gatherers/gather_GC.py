import os
import gcapi
from urllib.parse import quote
import logging

class GrandChallenge:
    def __init__(self, token=None):
        if token:
            self.client = gcapi.Client(token=token)
        else:
            self.client = gcapi.Client(token=os.getenv("GC_key"))

    def gather_data(self, slug) -> dict:
        """Gathers data from challenges and archives based on their slug and combines them in a dictionary"""
        slug = slug.strip("/")
        slug = quote(slug, safe="")
        
        try:
            archive_dict = self._gather_archive(slug)
        except:
            logging.log("Couldn't gather data from the archive")

        try:
            challenge_dict = self._gather_challenge(slug)        
        except:
            logging.log("Couldn't gather data from challenge")

        try:
            images = self.client.images.iterate_all(params={'archive': archive_dict["pk"]})
            for image in images:
                example_image = image
                break
            num_images = len(self.client.images.list(params={'archive': archive_dict["pk"]}))
        except:
            logging.log("Couldn't determine number of images")

        combined_dict = {f'challenge_{k}': v for k, v in challenge_dict.items()}
        combined_dict.update({f'archive_{k}': v for k, v in archive_dict.items()})
        combined_dict.update({'byteSize': num_images})
        return combined_dict
    
    def _gather_archive(self, slug) -> dict:
        archive = self.client.archives.detail(slug=slug.lower()) # Remove lower if you can
        return archive.__dict__
    
    def _gather_challenge(self, slug) -> dict:
        challenge = self.client.get(f"/challenges/{slug}", follow_redirects=True)
        challenge.raise_for_status()
        return challenge.json()