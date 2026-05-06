"""
ProfileConfig do Cosmos para BigQuery.

Os valores vêm de `include.config` (Airflow Variable / env / default), evitando
hardcode aqui.
"""

from cosmos import ProfileConfig
from cosmos.profiles import GoogleCloudServiceAccountDictProfileMapping

from include.config import load_gcp_config

_cfg = load_gcp_config()

bq_profile = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=GoogleCloudServiceAccountDictProfileMapping(
        conn_id=_cfg.bq_conn_id,
        profile_args={
            "project": _cfg.project_id,
            "dataset": _cfg.bq_dataset,
            "location": _cfg.bq_location,
        },
    ),
)
