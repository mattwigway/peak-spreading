"""
Scrape data from Caltrans PEMS.

Environment variables:
PEMS_USER - your PEMS username
PEMS_PASSWORD - your PEMS password

PeMS has a disclaimer:
All file downloads are recorded in the PeMS database.
Please do not use automated scripts to retrieve data
through this service. If using a batch downloading tool,
please configure it to visit links serially. PeMS will
block concurrent download requests.

Therefore, this is a "batch downloading tool" and not an
"automated script". It is configured to visit links serially.
"""

import requests
import logging
from os import environ
import os
import sys
from time import sleep
from random import random
import argparse

DISTRICTS = [3, 4, 5, 6, 7, 8, 10, 11, 12]
# DISTRICTS = [8, 10, 11, 12]
YEARS = [2022]
#YEARS = [2021]  # run script again to update data

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)

LOG = logging.getLogger(__name__)

parser = argparse.ArgumentParser()
parser.add_argument("--type", help="Type of data to retrieve", default="station_5min")
parser.add_argument("DATA_FOLDER", help="Output folder")
args = parser.parse_args()

if not "PEMS_USER" in environ or not "PEMS_PASSWORD" in environ:
    LOG.error("PEMS_USER and/or PEMS_PASSWORD not found in environment")
    sys.exit(1)

username = environ["PEMS_USER"]
pw = environ["PEMS_PASSWORD"]
LOG.info(f"Logging in to PEMS as user {username}")
jar = requests.cookies.RequestsCookieJar()
login = requests.post(
    "https://pems.dot.ca.gov",
    data={"username": username, "password": pw, "redirect": "", "login": "Login"},
)

data_folder = args.DATA_FOLDER
LOG.info(f"Saving output to {data_folder}")

if login.status_code == 200:
    LOG.info("Login successful")
else:
    LOG.error(f"Login gave error {login.status}:\n" + login.text)
    sys.exit(1)

sess = login.cookies

n_downloads = 0

for district in DISTRICTS:
    for year in YEARS:
        LOG.info(f"District {district}, {year}")
        file_req = requests.get(
            "https://pems.dot.ca.gov/",
            params={
                "srq": "clearinghouse",
                "district_id": district,
                "geotag": "",
                "yy": year,
                "type": args.type,
                "returnformat": "text",
            },
            cookies=sess,
        )

        file_req.raise_for_status()

        file_dir = file_req.json()

        for month, files in file_dir["data"].items():
            LOG.info(f"{month} ({len(files)} files)")

            for file in files:
                complete = False
                final_outfile = os.path.join(data_folder, file["file_name"])
                if os.path.exists(final_outfile):
                    LOG.info(
                        f"{file['file_name']} already exists in output directory, skipping"
                    )
                else:
                    outfile = os.path.join(
                        data_folder, file["file_name"] + ".download_in_progress"
                    )

                    for i in range(5):
                        sleep(random() * 2)
                        try:
                            with requests.get(
                                "https://pems.dot.ca.gov" + file["url"][1:],
                                cookies=sess,
                            ) as r:
                                r.raise_for_status()
                                    with open(outfile, "wb") as output:
                                        for chunk in r.iter_content(8192):
                                            output.write(chunk)
                        except:
                            if i < 4:
                                LOG.warning(
                                    f"Error retrieving {file['file_name']}, retrying"
                                )
                            else:
                                LOG.error(
                                    f"Could not retrieve {file['file_name']} after 5 tries, exiting"
                                )
                                sys.exit(1)
                        else:
                            n_downloads += 1
                            os.rename(outfile, final_outfile)
                            break


LOG.info(f"Downloaded {n_downloads} files")
