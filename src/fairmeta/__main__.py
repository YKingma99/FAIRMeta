from .metadata_model import MetadataRecord
from .gatherers import GrandChallenge
from .uploader_radboudfdp import RadboudFDP
import argparse
import yaml
import logging
from dotenv import load_dotenv

load_dotenv()

parser = argparse.ArgumentParser()
parser.add_argument('-c', '--config', help="YAML configuration file(s)", action='append', required=True)
parser.add_argument('platform', help="Platform to fetch metadata from")
parser.add_argument('slug', help="Unique identifier of dataset")
parser.add_argument('catalog_name', help="Name of catalog in FDP")
parser.add_argument('--test', action='store_true', help="Run in test mode")
parser.add_argument('-v', '--verbose', action='store_true', help="Verbose logging") 

def main():
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.WARNING,
        format="%(levelname)s: %(message)s"
    )

    logging.info("Loading configuration")
    config_data = []
    for c in args.config:
        with open(c, 'r') as f:
            config_data.append(yaml.safe_load(f))
    platforms = config_data[0]["platforms"]

    logging.info(f"Fetching data from platform: {args.platform}")
    match args.platform.lower():
        case "grand_challenge":
            config = platforms["grand_challenge"]
            platform = GrandChallenge()
            api_data = platform.gather_data(f"/{args.slug}")
        case _:
            raise ValueError(f"Unsupported platform: {args.platform}. Pick from: {', '.join(platforms.keys())}")

    data = MetadataRecord.create_metadata_schema_instance(configs=config, api_data=api_data)
    logging.info("Validating relaxed metadata schema")
    data.validate()
    MetadataRecord.transform_schema(data)
    logging.info("Validating strict metadata schema")
    data.validate()

    FDP = RadboudFDP(test=args.test)
    FDP.create_and_publish(data, args.catalog_name)

    logging.info("Done")


if __name__=="__main__":
    main()