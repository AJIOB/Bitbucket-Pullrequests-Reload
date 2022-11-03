#!/usr/bin/env python3.10
# Created by AJIOB, 2022
#
# Importing multiple repositories with cross-references in single workspace (bitbucket project)
#
# Configuration JSON file structure:
## {
##   "url": "https://bitbucket.example.com/subfolder",
##   "username": "test-user",
##   "password": "password-or-app-password",
##   "project": "new-projectName",
##   "old-server-url": "https://bitbucket.org/",
##   "old-server-project": "oldProject-name",
##   "repositories": [
##     {
##       "name": "repo1",
##       "csv-pr": "data/export1.csv",
##       "csv-pr-comments": "data/_export_comments.csv"
##     },
##     {
##       "name": "repo2",
##       "csv-pr": "data/export1.csv",
##       "csv-pr-comments": "data/export2.comments.csv"
##     },
##     {
##       "name": "3repo",
##       "csv-pr": "data/export_3.csv",
##       "csv-pr-comments": "data/export2.comments.csv"
##     }
##   ],
##   "diffs-json": "data/diffs.json",
##   "images-folder": "data/images-folder/"
## }
#
# Args:
## $1 = json configuration file
## $2 = (optional) working mode
### '' (not passed) = execute all sequence
### '-dAll' = execute only removing

import aiohttp
import asyncio
import data_import
from enum import Enum
import sys

# First arg to pass child data_import calls
ARGV0_CHILD = ""

CURRENT_MODE = None

class ProcessingMode(Enum):
    FULL = 1
    DELETE_ONLY = 2

def init():
    data_import.init()

def args_read(argv):
    if len(argv) < 2:
        raise Exception("Configuration file was not passed")

    res = data_import.read_json_file(argv[1])

    global CURRENT_MODE
    CURRENT_MODE = ProcessingMode.FULL
    if len(argv) > 2:
        mode = argv[2]
        if mode == '-dAll':
            CURRENT_MODE = ProcessingMode.DELETE_ONLY

    return res

async def data_import_main(session, argv):
    data_import.args_read(argv)

    if data_import.CURRENT_MODE == None:
        raise Exception(f"Current mode was not selected. Argv: {argv}")

    return await data_import.main_select_mode(session)

async def call_all_data(session, cfg):
    if len(cfg) == 0:
        raise Exception("Empty configuration is not supported")

    # Loads basic fields
    newServerUrl = cfg["url"]
    authUsername = cfg["username"]
    authPass = cfg["password"]
    prj = cfg["project"]
    oldServerUrl = cfg["old-server-url"]
    oldServerPrj = cfg["old-server-project"]
    diffsFile = cfg["diffs-json"]
    imgFolder = cfg["images-folder"]

    # Patch fields values & create in args
    if not oldServerUrl.endswith('/'):
        oldServerUrl += '/'
    if not imgFolder.endswith('/'):
        imgFolder += '/'

    authInfo = f"{authUsername}:{authPass}"
    oldServerInfo = f"{oldServerUrl}:{oldServerPrj}"

    needToRestore = {}

    # start to call script
    for r in cfg["repositories"]:
        currRepo = r["name"]
        currRepoFull = prj + '/' + currRepo
        currPrCsv = r["csv-pr"]

        print(f"Working with repository {currRepoFull}")

        # Deleting old info
        await data_import_main(session, [ARGV0_CHILD, newServerUrl, authInfo, currRepoFull, '-dAll'])

        if CURRENT_MODE == ProcessingMode.FULL:
            # Restoring branches & almost all PRs
            needToRestore[currRepo] = await data_import_main(session, [ARGV0_CHILD, newServerUrl, authInfo, currRepoFull, '-uAll', oldServerInfo, imgFolder, diffsFile, currPrCsv])

    if CURRENT_MODE == ProcessingMode.DELETE_ONLY:
        return

    # Repeat loading if need
    needToRunAgain = sum(needToRestore.values()) > 0
    print("Uploading PRs again:", needToRunAgain)
    while needToRunAgain:
        needToRunAgain = False

        for r in cfg["repositories"]:
            currRepo = r["name"]
            currRepoFull = prj + '/' + currRepo
            currPrCsv = r["csv-pr"]

            print(f"Working with repository {currRepoFull}")

            # Restoring almost all PRs
            newValue = await data_import_main(session, [ARGV0_CHILD, newServerUrl, authInfo, currRepoFull, '-uPRs', oldServerInfo, imgFolder, diffsFile, currPrCsv])

            if needToRestore[currRepo] != newValue:
                needToRunAgain = True

            needToRestore[currRepo] = newValue

    print("Force uploading not-referenced PRs")
    for r in cfg["repositories"]:
        currRepo = r["name"]
        currRepoFull = prj + '/' + currRepo
        currPrCsv = r["csv-pr"]

        print(f"Working with repository {currRepoFull}")

        # Force restoring all possible PRs
        needToRestore[currRepo] = await data_import_main(session, [ARGV0_CHILD, newServerUrl, authInfo, currRepoFull, '-uPRsForce', oldServerInfo, imgFolder, diffsFile, currPrCsv])

    print("Uploading PR comments & finishing uploading")
    for r in cfg["repositories"]:
        currRepo = r["name"]
        currRepoFull = prj + '/' + currRepo
        currPrCommentsCsv = r["csv-pr-comments"]

        print(f"Working with repository {currRepoFull}")

        # Restoring all possible PR comments
        needToRestore[currRepo] = await data_import_main(session, [ARGV0_CHILD, newServerUrl, authInfo, currRepoFull, '-uAll', oldServerInfo, imgFolder, diffsFile, currPrCommentsCsv])

        # Close created PRs
        needToRestore[currRepo] = await data_import_main(session, [ARGV0_CHILD, newServerUrl, authInfo, currRepoFull, '-cPRs'])

        # Delete created branches - not required more
        needToRestore[currRepo] = await data_import_main(session, [ARGV0_CHILD, newServerUrl, authInfo, currRepoFull, '-dBranches'])

async def main(argv):
    try:
        init()
        cfg = args_read(argv)

        async with aiohttp.ClientSession() as session:
            await call_all_data(session, cfg)
    except Exception as e:
        print("Exception was caught")
        print(e)
        print()

if __name__ == '__main__':
    # force set event loop for execution on python 3.10+
    # Based on https://stackoverflow.com/a/73367187/6818663
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        asyncio.run(main(sys.argv))
    except KeyboardInterrupt:
        pass
