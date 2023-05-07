import logging
import zipfile
from pathlib import Path
from typing import Iterable

import pandas as pd
import requests
from tqdm import tqdm

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def cached_download_files(
    filenames: Iterable[str], source_root_url: str, destination_dir: Path, force_redownload: bool = False
) -> None:
    destination_dir.mkdir(exist_ok=True)

    for fname in filenames:
        destination_fpath = destination_dir / fname
        if destination_fpath.is_file() and not force_redownload:
            logger.info(f"File {fname} already downloaded, set OVERWRITE=True to force re-download")
            continue

        url = f"{source_root_url}/{fname}"
        response = requests.get(url)
        with open(destination_dir / fname, "wb") as f:
            f.write(response.content)
            logger.info(f"File {fname} downloaded successfully")


def create_daily_turbine_data_files_from_zips(
    zip_filepaths: Iterable[Path], destination_dir: Path, turbine_data_prefix: str, timeshift: pd.Timedelta
) -> None:
    raw_timestamp_col = "# Date and time"
    clean_timestamp_col = "Timestamp"
    raw_col_subset = ["# Date and time", "Wind speed (m/s)", "Power (kW)"]
    destination_dir.mkdir(exist_ok=True)

    _dfs = []
    for zip_fp in zip_filepaths:
        zf = zipfile.ZipFile(zip_fp)
        for fileinfo in tqdm(zf.filelist):
            filename = fileinfo.filename
            if not filename.startswith(turbine_data_prefix):
                continue  # not interested in non-turbine data

            turbine_name = filename.replace(turbine_data_prefix, "").split("_")[0]
            _df = pd.read_csv(
                zf.open(filename), skiprows=9, usecols=raw_col_subset, parse_dates=[raw_timestamp_col]
            ).rename(columns={raw_timestamp_col: clean_timestamp_col})
            _df[clean_timestamp_col] = _df[clean_timestamp_col] + timeshift
            _df.insert(1, "TurbineName", turbine_name)
            _dfs.append(_df)

    for day, daily_df in tqdm(pd.concat(_dfs).groupby(pd.Grouper(key="Timestamp", freq="MS"))):
        output_filepath = destination_dir / f"{day.date().isoformat()}.csv"
        daily_df.to_csv(output_filepath, index=False)


def create_metadata_file(metadata_src_fpath: Path, destination_fpath: Path) -> None:
    def _get_name(x: str) -> int:
        return int(x.replace("T", ""))

    df = (
        pd.read_csv(metadata_src_fpath)
        .dropna(how="all")
        .assign(TurbineName=lambda d: d["Alternative Title"].apply(_get_name))
        .filter(["TurbineName", "Latitude", "Longitude"])
    )

    df.to_json(destination_fpath, orient="records")


if __name__ == "__main__":
    ROOT_URL = "https://zenodo.org/record/5946808/files"
    SCADA_DATA_FILENAMES = (
        "Penmanshiel_SCADA_2021_WT01-10_3108.zip",
        "Penmanshiel_SCADA_2021_WT11-15_3108.zip",
    )
    METADATA_FILENAME = "Penmanshiel_WT_static.csv"

    DESTINATION_DIR = Path(__file__).parents[1] / "data"

    cached_download_files(
        filenames=(*SCADA_DATA_FILENAMES, METADATA_FILENAME),
        source_root_url=ROOT_URL,
        destination_dir=DESTINATION_DIR,
        force_redownload=False,
    )
    create_daily_turbine_data_files_from_zips(
        zip_filepaths=[DESTINATION_DIR / i for i in SCADA_DATA_FILENAMES],
        destination_dir=DESTINATION_DIR / "turbine-data",
        turbine_data_prefix="Turbine_Data_Penmanshiel_",
        timeshift=pd.Timedelta(days=365 * 2),  # shift data by 2 years (to pretend it's recent)
    )
    create_metadata_file(
        metadata_src_fpath=DESTINATION_DIR / "Penmanshiel_WT_static.csv",
        destination_fpath=DESTINATION_DIR / "turbine-data" / "metadata.json",
    )
