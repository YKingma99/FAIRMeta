from __future__ import annotations
from datetime import datetime
from pydantic import BaseModel, AnyHttpUrl, AnyUrl
from sempyro.dcat import AccessRights
from sempyro.geo import Location
from sempyro.hri_dcat import HRIVCard, HRIAgent, DatasetTheme, DatasetStatus, DistributionStatus
from sempyro.prov import Activity
from sempyro.time import PeriodOfTime
from typing import List, Optional

# Support classes
class Agent(BaseModel):
    mbox: str | AnyUrl
    identifier: str | List[str]
    name: str | List[str]
    homepage: AnyUrl

    spatial: Optional[AnyHttpUrl | Location | List[AnyHttpUrl | Location]] = None
    # Unclear what type is
    type: Optional[AnyHttpUrl] = None
    publisher_type: Optional[AnyUrl] = None
    publisher_note: Optional[str] = None

class Kind(BaseModel):
    hasEmail: str | AnyUrl
    fn: str
    
    hasUrl: Optional[AnyHttpUrl | List[AnyHttpUrl]] = None

class PeriodOfTime(BaseModel):
    startDate: Optional[datetime] = None
    endDate: Optional[datetime] = None

# Main classes
class Distribution(BaseModel):
    access_url: AnyHttpUrl
    byte_size: int
    format: str | AnyHttpUrl
    license: str | AnyHttpUrl
    rights: AnyHttpUrl

    # access_service: Optional[AnyHttpUrl | DataService] = None
    applicable_legislation: Optional[str | AnyHttpUrl | List[str | AnyHttpUrl]] = None
    # checksum: Optional[Checksum] = None 
    compression_format: Optional[AnyHttpUrl] = None
    description: Optional[str | List[str]] = None
    documentation: Optional[AnyHttpUrl | List[AnyHttpUrl]] = None
    download_url: Optional[AnyHttpUrl] = None
    language: Optional[str | AnyHttpUrl | List[str | AnyHttpUrl]] = None
    linked_schemas: Optional[AnyHttpUrl | List[AnyHttpUrl]] = None
    media_type: Optional[AnyHttpUrl] = None
    modification_date: Optional[datetime] = None
    packaging_format: Optional[AnyHttpUrl] = None
    release_date: Optional[datetime] = None
    retention_period: Optional[AnyHttpUrl | PeriodOfTime] = None
    status: Optional[str | AnyHttpUrl | DistributionStatus] = None
    temporal_resolution: Optional[str] = None
    title: Optional[str | List[str]] = None


class Dataset(BaseModel):
    title: str | List[str]
    description: str | List[str]
    access_rights: str | AccessRights
    contact_point: Kind | HRIVCard
    creator: Agent | HRIAgent | List[Agent | HRIAgent]
    identifier: str
    keyword: List[str] = []
    publisher: Agent | HRIAgent
    theme: str | DatasetTheme | List[str | DatasetTheme]
    applicable_legislation: AnyHttpUrl | List[AnyHttpUrl]

    analytics: Optional[Distribution | List[Distribution]] = None
    code_values: Optional[AnyHttpUrl | List[AnyHttpUrl]] = None
    coding_system: Optional[AnyHttpUrl | List[AnyHttpUrl]] = None
    conforms_to: Optional[AnyHttpUrl | List[AnyHttpUrl]] = None
    distribution: Optional[Distribution | List[Distribution]] = None
    documentation: Optional[AnyHttpUrl | List[AnyHttpUrl]] = None
    frequency: Optional[str | AnyHttpUrl] = None
    geographical_coverage: Optional[AnyHttpUrl | Location | List[AnyHttpUrl | Location]] = None
    # has_version: Optional[AnyHttpUrl | Dataset | List[AnyHttpUrl | Dataset]] = None # Should point at other dataset? HRIDataset says it should be url though
    health_theme: Optional[AnyHttpUrl | List[AnyHttpUrl]] = None # HealthDCAT-AP is exploring sources that can be used for this
    # in_series: Optional[DatasetSeries | List[DatasetSeries]] = None
    is_referenced_by: Optional[AnyHttpUrl | List[AnyHttpUrl]] = None
    language: Optional[str | AnyHttpUrl | List[str | AnyHttpUrl]] = None
    legal_basis: Optional[str | AnyHttpUrl | List[str | AnyHttpUrl]] = None
    maximum_typical_age: Optional[int] = None
    minimum_typical_age: Optional[int] = None
    modification_date: Optional[str | datetime] = None
    number_of_records: Optional[int] = None
    number_of_unique_individuals: Optional[int] = None
    # other_identifier: Optional[Identifier | List[Identifier]] = None
    personal_data: Optional[str | AnyHttpUrl | List[str | AnyHttpUrl]] = None
    population_coverage: Optional[str] = None # Cardinality 0..* but doesn't allow for lists
    purpose: Optional[AnyHttpUrl | List[AnyHttpUrl]] = None
    # qualified_attribution: Optional[AnyHttpUrl | Attribution | List[AnyHttpUrl | Attribution]] = None
    # qualified_relation: Optional[AnyHttpUrl | Relationship | List[AnyHttpUrl | Relationship]] = None
    # quality_annotation: Optional[AnyHttpUrl | QualityCertificate | List[AnyHttpUrl | QualityCertificate]] = None
    release_date: Optional[str | datetime] = None
    retention_period: Optional[PeriodOfTime | List[PeriodOfTime]] = None
    sample: Optional[AnyHttpUrl | Distribution | List[AnyHttpUrl | Distribution]] = None
    source: Optional[AnyHttpUrl | Dataset | List[AnyHttpUrl | Dataset]] = None
    status: Optional[str | DatasetStatus] = None
    temporal_coverage: Optional[PeriodOfTime | List[PeriodOfTime]] = None
    temporal_resolution: Optional[str] = None
    type: Optional[AnyHttpUrl | List[AnyHttpUrl]] = None # Controlled vocabulary
    # version: Optional[str] = None
    version_notes: Optional[str | List[str]] = None
    was_generated_by: Optional[AnyHttpUrl | Activity | List[AnyHttpUrl | Activity]] = None

class Catalog(BaseModel):
    contact_point: Kind | HRIVCard
    dataset: Dataset | List[Dataset]
    description: str | List[str]
    publisher: Agent | HRIAgent
    title: str | List[str]

    applicable_legislation: Optional[AnyHttpUrl | List[AnyHttpUrl]] = None
    # Not currently supported:
    # catalog: Optional[Catalog | List[Catalog]] = None
    creator: Optional[Agent | HRIAgent | List[Agent | HRIAgent]] = None
    geographical_coverage: Optional[AnyHttpUrl | Location | List[AnyHttpUrl | Location]] = None
    # has_part: Optional[Catalog | List[Catalog]] = None
    homepage: Optional[AnyHttpUrl] = None
    language: Optional[str | AnyHttpUrl | List[str | AnyHttpUrl]] = None
    license: Optional[str | AnyHttpUrl] = None
    modification_date: Optional[str | datetime] = None
    release_date: Optional[str | datetime] = None
    rights: Optional[AnyHttpUrl] = None
    # service: Optional[DataService | List[DataService]] = None
    temporal_coverage: Optional[PeriodOfTime] = None
    # Unclear:
    # themes: Optional[str | List[str]] = None