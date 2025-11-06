import pytest
from fairmeta.gatherers.gather_GC import GrandChallenge
from httpx import HTTPStatusError, Request, Response
from gcapi import Client
import test_utils
from fairmeta import metadata_model
import requests

@pytest.mark.parametrize("slug,status_code,exception",[("LUNA16", 200, None),
                                                       ("weird", 404, HTTPStatusError)])
def test_gather_gc_data(monkeypatch, slug, status_code, exception):
    calls = []

    def fake_get(self, path, **kwargs) -> Response:
        calls.append((path, kwargs))
        if status_code == 200:
            content = {"name": slug, "pk": 1}
        else:
            content = {"detail": "Not found"}
        print("path", path)
        return Response(status_code=status_code, json=content, request=Request("GET", f"https://grand-challenge.org/api/v1/challenges{path}"))

    monkeypatch.setattr(Client, "get", fake_get)
    class FakeArchive:
        def __init__(self):
            self.pk = 2
            self.slug = slug.lower()
    
    class FakeArchives:
        def detail(self, slug):
            return FakeArchive()

    class FakeImages:
        def __init__(self):
            self._items = [{"id": "img1"}, {"id": "img2"}]

        def iterate_all(self, params):
            for item in self._items:
                yield item

        def list(self, params):
            return self._items

    def fake_init(self):
        self.client = Client(token="token")
        self.client.archives = FakeArchives()
        self.client.images = FakeImages()

    monkeypatch.setattr("fairmeta.gatherers.gather_GC.GrandChallenge.__init__", fake_init)

    platform = GrandChallenge()
    if exception:
        with pytest.raises(exception) as exc_info:
            platform._gather_challenge(f"{slug}")
        print(exc_info)
    else:
        archive_data = platform._gather_challenge(f"{slug}")
        assert calls
        assert calls[0][0].endswith(f"/challenges/{slug}"), f"Unexpected ending: {calls[0][0]}"
        assert isinstance(archive_data, dict)
        assert archive_data.get("name") == slug


# @pytest.mark.parametrize()
def test_FDP_post_and_publish(FDP, config, api_data):
    schema = test_utils.adapted_instance(None, config, api_data, None, None)
    metadata_model.MetadataRecord.transform_schema(schema)
    catalog_name = "test_catalog"
    urls = FDP.create_and_publish(schema, catalog_name)

    for url in urls:
        rsp = requests.get(url)
        assert rsp.status_code == 200

    FDP.delete(urls[0], confirm=False) # The first URL is the catalog url. Deleting this one deletes all items in the catalog

def test_FDP_update(FDP):
    pass