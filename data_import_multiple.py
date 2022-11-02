#!/usr/bin/env python3.10
# Created by AJIOB, 2022
#
# Importing multiple repositories with cross-references in single workspace
#
# Configuration JSON file structure:
## {
## }
#
# Args:
## $1 = json configuration file

import aiohttp
import asyncio
import data_import
import sys

def init():
    data_import.init()

def args_read(argv):
    if len(argv) < 2:
        raise Exception("Configuration file was not passed")

    return data_import.read_json_file(argv[1])

async def call_all_data(session, cfg):
    if len(cfg) == 0:
        raise Exception("Empty configuration is not supported")

    # TODO: implement required calls
    pass

async def main(argv):
    init()
    cfg = args_read(argv)

    async with aiohttp.ClientSession() as session:
        await call_all_data(session, cfg)

if __name__ == '__main__':
    # force set event loop for execution on python 3.10+
    # Based on https://stackoverflow.com/a/73367187/6818663
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        asyncio.run(main(sys.argv))
    except KeyboardInterrupt:
        pass
