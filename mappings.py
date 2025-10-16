from enum import Enum
from sempyro.dcat import AccessRights
from sempyro.dcat.dcat_dataset import Frequency
from sempyro.hri_dcat import DatasetTheme, DatasetStatus, GeonovumLicences, DistributionStatus

access_rights = {right.name: right for right in AccessRights}

distributionstatuses = {status.name: status for status in DistributionStatus}

frequencies = {frequency.name: frequency for frequency in Frequency}

licenses = {lic.name: lic.value for lic in GeonovumLicences}

statuses = {status.name: status for status in DatasetStatus}

themes = {theme.name: theme for theme in DatasetTheme}

