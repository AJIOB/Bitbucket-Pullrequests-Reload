#!/usr/bin/env python3.10
# Created by AJIOB, 2022
#
# Full restoring sequence should be:
# 1. Using test repo with the same sources, as in original
# 2. Restoring almost all PRs & branches ($4 = '-uAll', $5 should be with old project URL (see the description for more details), $6 should be with images path, $7 should be with JSON file, $8 should be csv with PRs)
# 3. Done all previous steps for all required repos for next continue
# 4. Restoring almost all PRs ($4 = '-uPRs', $5 should be with old project URL (see the description for more details), $6 should be with images path, $7 should be with JSON file, $8 should be csv with PRs)
# 5. Done previous step for all required repos (all repos per single iteration) while any repo script return values or last row number will be changed for next continue
# 6. Force restoring all lost PRs ($4 = '-uPRsForce', $5 should be with old project URL (see the description for more details), $6 should be with images path, $7 should be with JSON file, $8 should be equal to csv with PRs)
# 7. Restoring all PR comments ($4 = '-uAll', $5 should be with old project URL (see the description for more details), $6 should be with images path, $7 should be with JSON file, $8 should be equal to csv with PR comments)
# 8. Close all PRs ($4 = '-cPRs')
# 9. Delete all created branches ($4 = '-dBranches')
#
# If results are correct, you can do that on production repo with the same sources, as in test/original repos. Command subsequence will be the equal, as for testing.
#
# Notes:
# - repo name must be the same in restoring time, because of CSV with multiple repo info are supported
# - you should create PR with attached CSV & JSON files for possible next re-restoring in another format or repo
# - tested with Bitbucket Server v8.5.0 (not Bitbucket Cloud)
# - Bitbucket app key (HTTP access token) should be used instead of real password (with repository write permissions)
# - as told in Bitbucket REST API docs (https://docs.atlassian.com/bitbucket-server/rest/5.16.0/bitbucket-rest.html, "Personal Repositories" part), if you want to access user project instead of workspace project, you should add '~' before your username. For example, use '~alex/my-repo' for accessing 'alex' personal workspace
# - all cross-referenced repositories should be in one new workgroup
# - PRs creation will be always incremented. Automatic branches creation for PR will be not
#
# Args sequence:
## $1 = new server URL (such as 'https://bitbucket.org/')
## $2 = new server auth info: "username:password"
## $3 = new server project/repo combination (such as 'my-workspace/test-repo')
## $4 = execution mode:
### -uAll = load info from file to PRs with auto-detection PRs/PR comments
### -uAllForce = '-uAll' + force creating cross refs
### -debug = print input variables & exit
### -uPRs = load info from file to PRs (not recreate branches)
### -uPRsForce = '-uPRs' + force creating cross refs
### -dAll = delete all created branches & PRs
### -dBranches = delete all created branches (keep PRs)
### -cPRs = close (decline) all created PRs
### -dPRs = delete all created PRs (keep branches)
## $5 = (optional, must be set to value or empty, if need to pass next args) source server url/team name. Default value is bitbucket cloud URL without any team name.
##    Values
##      'https://server.bitbucket.my/with/relative/path/projectName'
##      'https://server.bitbucket.my/with/relative/path/projectName/'
##    will be decoded as:
##      * "https://server.bitbucket.my/with/relative/path/" = old server name
##      * "projectName" = project name (!= repo name)
## $6..$x = (optional) additional args
### any_filename.json = json file will additional info:
#### - PR comments uses that info in format key:value, where key = diff URL (usually bitbucket API), value = downloaded diff info from that URL
### any_filename.csv = csv file with additional info:
#### - PRs & PR comments imports use file as source file name (csv-formatted data from ruby)
### any_foldername/ = folder will additional info:
#### - PRs & PR comments use files as mirrors for images uploading

import aiohttp
import asyncio
import csv
from datetime import datetime
from enum import Enum
import json
import os
import re
import sys
from urllib.parse import unquote
from zoneinfo import ZoneInfo

OPENED_PR_STATE = "OPEN"
ANY_PR_STATE = "ALL"
SRC_BRANCH_PREFIX = 'src'
DST_BRANCH_PREFIX = 'dst'
# URL match regex from https://uibakery.io/regex-library/url-regex-python
URLS_REGEX = r'https?:\/\/(?:www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b(?:[-a-zA-Z0-9()@:%_\+.~#?&\/=]*)'
# Used by creation & filtering too, uses '[' for generating more specific output
PR_START_NAME = "[Bitbucket Import"
BRANCH_START_NAME = "bitbucket/"
# May be set to None for enable default limit 25
DEFAULT_PAGE_RECORDS_LIMIT = 2000
# Exit if it was found
HTTP_EXIT_CODES = [
    # HTTP 401 Unauthorized error code
    401,
]
# Limit number of simultaneous requests. With big number we will have lots of miss-generated 500 errors
LIMIT_NUMBER_SIMULTANEOUS_REQUESTS = 25
LIMIT_NUMBER_SIMULTANEOUS_REQUESTS_BRANCH_DELETE = 10
# True => print attached diffs
# False => don't diffs as attaches
PRINT_ATTACHED_DIFFS = False
# True => create PRs with bad PR cross-refs
# False => don't create PRs with bad PR cross-refs
# Always True for PR comments creation
# Can be set from CLI
FORCE_CREATE_PRS_WITH_BAD_CROSS_REFS = None
TARGET_COMMENTS_TIMEZONE = ZoneInfo("Europe/Moscow")
SOURCE_SERVER_ABSOLUTE_URL_PREFIX = ""
SOURCE_SERVER_IMAGES_CONTAINS = "/images/"

class ProcessingMode(Enum):
    LOAD_INFO = 1
    DELETE_BRANCHES = 2
    DELETE_BRANCHES_PRS = 3
    DELETE_PRS = 4
    LOAD_INFO_ONLY_PRS = 5
    CLOSE_PRS = 6
    DEBUG = 7

CURRENT_MODE = None
JSON_ADDITIONAL_INFO = {}
IMAGES_ADDITIONAL_INFO_PATH = ''

class PullRequest:
    def __init__(self, id, user, title, state, createdAt, closedAt, body, bodyHtml, srcCommit, dstCommit, srcBranch, dstBranch, declineReason, mergeCommit, closedBy):
        self.id = id
        self.user = user
        self.title = title
        self.state = state
        self.createdAt = createdAt
        self.closedAt = closedAt
        self.body = body
        self.bodyHtml = bodyHtml
        self.srcCommit = srcCommit
        self.dstCommit = dstCommit
        self.srcBranch = srcBranch
        self.dstBranch = dstBranch
        self.declineReason = declineReason
        self.mergeCommit = mergeCommit
        self.closedBy = closedBy

class PRComment:
    def __init__(self, repo, prNumber, user, currType, currId, body, bodyHtml, createdAt, isDeleted, toLine, fromLine, file, diffUrl, parentComment, commit):
        self.repo = repo
        self.prId = prNumber
        self.user = user
        self.currType = currType
        self.id = currId
        self.body = body
        self.bodyHtml = bodyHtml
        self.createdAt = createdAt
        self.isDeleted = isDeleted
        self.toLine = toLine
        self.fromLine = fromLine
        self.file = file
        self.diffUrl = diffUrl
        self.parentCommentId = parentComment
        self.commit = commit

class PullRequestShort:
    def __init__(self, id, version):
        self.id = id
        self.version = version

def init():
    try:
        # see https://stackoverflow.com/a/15063941/6818663
        csv.field_size_limit(sys.maxsize)
    except OverflowError:
        maxLong = (1 << 31) - 1

        # Looks like Windows uses long instead of long long
        csv.field_size_limit(maxLong)

    # Init global templates
    # Almost all from https://docs.atlassian.com/bitbucket-server/rest/5.16.0/bitbucket-rest.html
    global URL_CREATE_PR
    URL_CREATE_PR = "{endpoint}rest/api/{version}/projects/{projectKey}/repos/{repositorySlug}/pull-requests"

    global URL_CREATE_PR_COMMENT
    URL_CREATE_PR_COMMENT = "{endpoint}rest/api/{version}/projects/{projectKey}/repos/{repositorySlug}/pull-requests/{pullRequestId}/comments"

    global URL_CLOSE_PR
    URL_CLOSE_PR = "{endpoint}rest/api/{version}/projects/{projectKey}/repos/{repositorySlug}/pull-requests/{pullRequestId}/decline"

    global URL_DELETE_PR
    URL_DELETE_PR = "{endpoint}rest/api/{version}/projects/{projectKey}/repos/{repositorySlug}/pull-requests/{pullRequestId}"

    global URL_CREATE_BRANCH
    URL_CREATE_BRANCH = "{endpoint}rest/api/{version}/projects/{projectKey}/repos/{repositorySlug}/branches"

    # From https://docs.atlassian.com/bitbucket-server/rest/5.16.0/bitbucket-branch-rest.html
    global URL_DELETE_BRANCH
    URL_DELETE_BRANCH = "{endpoint}rest/branch-utils/{version}/projects/{projectKey}/repos/{repositorySlug}/branches"

    global URL_GET_COMMIT
    URL_GET_COMMIT = "{endpoint}rest/api/{version}/projects/{projectKey}/repos/{repositorySlug}/commits/{commitId}"

    # Hand revert from UI attaching
    global URL_ATTACH_FILE
    URL_ATTACH_FILE = "{endpoint}projects/{projectKey}/repos/{repositorySlug}/attachments"

    # From https://confluence.atlassian.com/cloudkb/xsrf-check-failed-when-calling-cloud-apis-826874382.html
    global POST_HEADERS
    POST_HEADERS = {"X-Atlassian-Token": "no-check"}

    global MULTITHREAD_LIMIT
    MULTITHREAD_LIMIT = asyncio.Semaphore(LIMIT_NUMBER_SIMULTANEOUS_REQUESTS)

    global MULTITHREAD_LIMIT_BRANCH_DELETE
    MULTITHREAD_LIMIT_BRANCH_DELETE = asyncio.Semaphore(LIMIT_NUMBER_SIMULTANEOUS_REQUESTS_BRANCH_DELETE)

def args_read(argv):
    global SERVER
    SERVER = argv[1]
    if not SERVER.endswith('/'):
        SERVER += '/'

    global SERVER_API_VERSION
    global SERVER_PROJECTS_SUBSTRING
    global SERVER_REPOS_SUBSTRING
    # 1 for custom bitbucket server/datacenter, 2 for cloud
    if 'bitbucket.org' in SERVER:
        SERVER_API_VERSION = 2
        SERVER_PROJECTS_SUBSTRING = ''
        SERVER_REPOS_SUBSTRING = ''
    else:
        SERVER_API_VERSION = 1
        SERVER_PROJECTS_SUBSTRING = 'projects/'
        SERVER_REPOS_SUBSTRING = 'repos/'

    SERVER_API_VERSION = f"{SERVER_API_VERSION}.0"

    USER_PASS = argv[2]
    userPassSplit = USER_PASS.split(':')

    global AUTH
    AUTH = aiohttp.BasicAuth(userPassSplit[0], userPassSplit[1])

    PROJECT_REPO = argv[3]
    prjRepoSplit = PROJECT_REPO.split('/')

    global PROJECT
    global REPO

    PROJECT = prjRepoSplit[0]
    REPO = prjRepoSplit[1].lower()

    global FORCE_CREATE_PRS_WITH_BAD_CROSS_REFS
    FORCE_CREATE_PRS_WITH_BAD_CROSS_REFS = False

    if len(argv) > 4:
        global CURRENT_MODE
        mode = argv[4]
        if mode == '-dAll':
            CURRENT_MODE = ProcessingMode.DELETE_BRANCHES_PRS
        elif mode == '-uAll':
            CURRENT_MODE = ProcessingMode.LOAD_INFO
        elif mode == '-uAllForce':
            CURRENT_MODE = ProcessingMode.LOAD_INFO

            FORCE_CREATE_PRS_WITH_BAD_CROSS_REFS = True
        elif mode == '-dBranches':
            CURRENT_MODE = ProcessingMode.DELETE_BRANCHES
        elif mode == '-dPRs':
            CURRENT_MODE = ProcessingMode.DELETE_PRS
        elif mode == '-uPRs':
            CURRENT_MODE = ProcessingMode.LOAD_INFO_ONLY_PRS
        elif mode == '-uPRsForce':
            CURRENT_MODE = ProcessingMode.LOAD_INFO_ONLY_PRS

            FORCE_CREATE_PRS_WITH_BAD_CROSS_REFS = True
        elif mode == '-cPRs':
            CURRENT_MODE = ProcessingMode.CLOSE_PRS
        elif mode == '-debug':
            CURRENT_MODE = ProcessingMode.DEBUG
        else:
            CURRENT_MODE = None

    global SOURCE_SERVER_ABSOLUTE_URL_PREFIX
    SOURCE_SERVER_ABSOLUTE_URL_PREFIX = "https://bitbucket.org/"
    global OLD_PROJECT_NAME
    OLD_PROJECT_NAME = None
    if len(argv) > 5:
        prefix = argv[5]
        if re.fullmatch(URLS_REGEX, prefix):
            if not prefix.endswith('/'):
                prefix += '/'

            oldPrjName = OLD_PROJECT_NAME

            delim = prefix.rfind('/', 0, -1)
            if delim > 0:
                # no need end /
                oldPrjName = prefix[(delim + 1):-1]
                # need end /
                prefix = prefix[:(delim + 1)]

            SOURCE_SERVER_ABSOLUTE_URL_PREFIX = prefix
            OLD_PROJECT_NAME = oldPrjName

    global JSON_ADDITIONAL_INFO_FILE
    JSON_ADDITIONAL_INFO_FILE = None
    global SRC_FILE
    SRC_FILE = None
    global IMAGES_ADDITIONAL_INFO_PATH
    IMAGES_ADDITIONAL_INFO_PATH = ''
    for p in argv[6:]:
        if p.endswith('.json'):
            JSON_ADDITIONAL_INFO_FILE = p
        elif p.endswith('.csv'):
            SRC_FILE = p
        elif p.endswith('/'):
            IMAGES_ADDITIONAL_INFO_PATH = p

def read_csv_file(path):
    rows = []
    with open(path, "r", encoding="utf8") as src:
        inReader = csv.reader(src)

        for row in inReader:
            rows.append(row)

    return rows

def read_json_file(path):
    try:
        if path:
            with open(path, "r", encoding="utf8") as f:
                return json.load(f)
    except Exception as e:
        print(f"Cannot read source JSON file '{path}'")
        print(e)
        print()

    return {}

def formatTemplate(template, prId=None, commitId=None, repo=None):
    if repo == None:
        repo = REPO

    return template.format(
        endpoint=SERVER,
        version=SERVER_API_VERSION,
        projectKey=PROJECT,
        repositorySlug=repo,
        pullRequestId=prId,
        commitId=commitId
    )

def formatBranchName(id, prefix, originalName):
    res = f'{BRANCH_START_NAME}{id}/{prefix}/{originalName}'

    # we have limit of 111 chars
    # but it looks like need to be limited by 100:
    # https://jira.atlassian.com/browse/BSERV-10433
    return res[:100]

def append_timestamp_string_if_possible(text, textUtcSeconds, targetTimeZone=TARGET_COMMENTS_TIMEZONE, errorDescription=None):
    try:
        dt = datetime.utcfromtimestamp(int(textUtcSeconds))
    except Exception as e:
        if errorDescription:
            print(errorDescription)
        print(e)
        print()

        return text

    try:
        dt = dt.replace(tzinfo=targetTimeZone)
    except Exception as e:
        print("Bad user-provided time-zone")
        print(e)
        print()

        return text

    return f"{text} at {dt.isoformat()}"

async def response_process(res):
    # Force waiting message, as described here:
    # https://stackoverflow.com/a/56446507/6818663
    text = await res.text()

    try:
        res.raise_for_status()
        pass
    except aiohttp.ClientResponseError as e:
        # Force fill message field & rethrow exception
        raise aiohttp.ClientResponseError(e.request_info, e.history, status=e.status, headers=e.headers, message=text)

    return text

async def create_pr(session, title, description = None, srcBranch = "prTest1", dstBranch = "stage"):
    payload = {
        "title": title,
        "description": description,
        "fromRef": {
            "id": srcBranch,
        },
        "toRef": {
            "id": dstBranch,
        },
        "reviewers": [
        ]
    }

    async with MULTITHREAD_LIMIT:
        async with session.post(formatTemplate(URL_CREATE_PR), auth=AUTH, headers=POST_HEADERS, json=payload) as resp:
            return await response_process(resp)

async def list_prs(session, start=0, state=OPENED_PR_STATE, filterText=None, repo=None):
    payload = {
        "start": start,
        "state": state,
    }

    if DEFAULT_PAGE_RECORDS_LIMIT:
        payload["limit"] = DEFAULT_PAGE_RECORDS_LIMIT

    if filterText:
        payload["filterText"] = filterText

    async with MULTITHREAD_LIMIT:
        async with session.get(formatTemplate(URL_CREATE_PR, repo=repo), auth=AUTH, params=payload) as resp:
            return await response_process(resp)

async def close_pr(session, id, version, comment=None):
    payload = {
        "version": version,
    }

    if comment:
        payload["comment"] = comment

    async with MULTITHREAD_LIMIT:
        async with session.post(formatTemplate(URL_CLOSE_PR, prId=id), auth=AUTH, headers=POST_HEADERS, json=payload) as resp:
            return await response_process(resp)

async def delete_pr(session, id, version):
    payload = {
        "version": version,
    }

    async with MULTITHREAD_LIMIT:
        async with session.delete(formatTemplate(URL_DELETE_PR, prId=id), auth=AUTH, headers=POST_HEADERS, json=payload) as resp:
            return await response_process(resp)

async def create_pr_file_comment(session, prId, text, filePath, lineNum, fileType="TO", lineType="CONTEXT", fromHash=None, toHash=None, diffType="RANGE"):
    payload = {
        "text": text,
        "anchor": {
            "line": lineNum,
            "lineType": lineType,
            "fileType": fileType,
            "path": filePath,
        },
    }

    if fromHash or toHash:
        payload["anchor"]["fromHash"] = fromHash
        payload["anchor"]["toHash"] = toHash
        payload["anchor"]["diffType"] = diffType

    async with MULTITHREAD_LIMIT:
        async with session.post(formatTemplate(URL_CREATE_PR_COMMENT, prId=prId), auth=AUTH, headers=POST_HEADERS, json=payload) as resp:
            return await response_process(resp)

async def create_pr_comment(session, prId, text, parentCommit=None):
    payload = {
        "text": text,
    }

    if parentCommit:
        payload["parent"] = {
            "id": parentCommit,
        }

    async with MULTITHREAD_LIMIT:
        async with session.post(formatTemplate(URL_CREATE_PR_COMMENT, prId=prId), auth=AUTH, headers=POST_HEADERS, json=payload) as resp:
            return await response_process(resp)

async def get_commit_info(session, commitToRead):
    async with MULTITHREAD_LIMIT:
        async with session.get(formatTemplate(URL_GET_COMMIT, commitId = commitToRead), auth=AUTH) as resp:
            return await response_process(resp)

async def attach_file(session, filePathToAttach):
    fileName = os.path.basename(filePathToAttach)

    with open(filePathToAttach, "rb") as f:
        with aiohttp.MultipartWriter('form-data') as mpwriter:
            part = mpwriter.append(f)
            part.set_content_disposition('form-data', name='files', filename=fileName)

            async with MULTITHREAD_LIMIT:
                async with session.post(formatTemplate(URL_ATTACH_FILE), auth=AUTH, data=mpwriter) as resp:
                    return await response_process(resp)

async def create_branch(session, name, commit):
    payload = {
        "name": name,
        "startPoint": commit,
    }

    async with MULTITHREAD_LIMIT:
        async with session.post(formatTemplate(URL_CREATE_BRANCH), auth=AUTH, headers=POST_HEADERS, json=payload) as resp:
            return await response_process(resp)

async def list_branches(session, filterText=None, start=0):
    payload = {
        "start": start,
    }

    if filterText != None:
        payload["filterText"] = filterText

    if DEFAULT_PAGE_RECORDS_LIMIT:
        payload["limit"] = DEFAULT_PAGE_RECORDS_LIMIT

    async with MULTITHREAD_LIMIT:
        async with session.get(formatTemplate(URL_CREATE_BRANCH), auth=AUTH, params=payload) as resp:
            return await response_process(resp)

async def delete_branch(session, id, dryRun=False):
    payload = {
        "name": id,
        "dryRun": dryRun,
    }

    # Force blocking multithread deleting, because git almost not support concurrency on bitbucket backend
    async with MULTITHREAD_LIMIT_BRANCH_DELETE:
        async with MULTITHREAD_LIMIT:
            async with session.delete(formatTemplate(URL_DELETE_BRANCH, prId=id), auth=AUTH, headers=POST_HEADERS, json=payload) as resp:
                return await response_process(resp)

async def upload_prs(session, data):
    headers = data[0]

    prs = {}

    # Parse data
    for d in data[1:]:
        repo = d[headers.index('Repository')]
        number = d[headers.index('#')]
        user = d[headers.index('User')]
        title = d[headers.index('Title')]
        state = d[headers.index('State')]
        createdAt = d[headers.index('CreatedAt')]
        closedAt = d[headers.index('UpdatedAt')]
        body = d[headers.index('BodyRaw')]
        bodyHtml = d[headers.index('BodyHTML')]
        src = d[headers.index('SourceCommit')]
        dst = d[headers.index('DestinationCommit')]
        srcBranch = d[headers.index('SourceBranch')]
        dstBranch = d[headers.index('DestinationBranch')]
        declineReason = d[headers.index('DeclineReason')]
        mergeCommit = d[headers.index('MergeCommit')]
        closedBy = d[headers.index('ClosedBy')]

        if repo.lower() != REPO:
            # Block creating another PRs
            continue

        pr = PullRequest(number, user, title, state, createdAt, closedAt, body, bodyHtml, src, dst, srcBranch, dstBranch, declineReason, mergeCommit, closedBy)
        prs[number] = pr

    if CURRENT_MODE == ProcessingMode.LOAD_INFO:
        # Create branches
        await asyncio.gather(*[create_branches_for_pr(session, prs[prId]) for prId in prs])

    # Load already created PRs
    try:
        allPrs = await list_all_prs(session, filterTitle=PR_START_NAME)
    except aiohttp.ClientResponseError as e:
        print(f"Cannot receive list with already created PRs. HTTP error {e.status}, message {e.message}")
    except Exception as e:
        print(f"Cannot receive list with already created PRs. Error message {e}")

    print("PRs before cleanup:", len(prs))

    # Remove already created PRs from creation list
    for p in allPrs:
        prTitle = p["title"]

        # get first number, as described in PR title creation
        numberSearch = re.search(r'\d+', prTitle)
        if not numberSearch:
            continue

        capturedPrId = numberSearch.group()
        # Removing from dict (key may not exists)
        # Based on https://stackoverflow.com/a/11277439/6818663
        prs.pop(capturedPrId, None)

    print("PRs after cleanup:", len(prs))

    # Resave data to usual list
    prsToCheckAgain = list(prs.values())

    prevPrNumber = 0

    # Create pull requests
    # Block for infinite loop
    while prevPrNumber != len(prsToCheckAgain):
        prevPrNumber = len(prsToCheckAgain)
        print(prevPrNumber, "PRs will be checked for loading")

        # Uses for speed up downloading big repo PRs
        # Will be cleaned on every iteration loop for loading new self-cross-references
        prsCache = {}

        loadResults = await asyncio.gather(*[form_single_pr(session, pr, prsCache) for pr in prsToCheckAgain])

        # check what wasn't uploaded
        newPrsToCheckAgain = []
        for i in range(prevPrNumber):
            if not loadResults[i]:
                newPrsToCheckAgain.append(prsToCheckAgain[i])

        # save prev iteration as current
        prsToCheckAgain = newPrsToCheckAgain

    if len(prsToCheckAgain) != 0:
        print("Cannot upload that PRs:")
        for c in prsToCheckAgain:
            print("ID", c.id, "body", c.title)

    return len(prsToCheckAgain)

async def create_branches_for_pr(session, pr):
    try:
        print("Creating branches for PR", pr.id)

        srcCommit = pr.srcCommit
        try:
            res = await get_commit_info(session, srcCommit)

            # force rechecking for empty commits
            res = json.loads(res)
            commitId = res["id"]
        except Exception as e:
            # Commit not found, using merge commit
            srcCommit = pr.mergeCommit

            print("Using merge commit instead of src (second not presented in subtree), PR", pr.id)

        # If merge commit is not presented, we will see that next in create part

        await create_branch(session, formatBranchName(pr.id, SRC_BRANCH_PREFIX, pr.srcBranch), srcCommit)
        await create_branch(session, formatBranchName(pr.id, DST_BRANCH_PREFIX, pr.dstBranch), pr.dstCommit)
    except aiohttp.ClientResponseError as e:
        print(f"HTTP Exception was caught for PR {pr.id} branch creation")
        print(f"HTTP code {e.status}")
        print(e.message)
        print()
        if e.status in HTTP_EXIT_CODES:
            exit(e.status)
    except Exception as e:
        print(f"Exception was caught for PR {pr.id} branch creation")
        print(e)
        print()

async def form_single_pr(session, pr, prsCache={}):
    try:
        # First number in title must be original PR number
        # for correct comments uploading
        # & cross-references creating
        newTitle = f"{PR_START_NAME} {pr.id}, {pr.state}] {pr.title}"

        descriptionParts = [
            f"",
            f"Source commit (from) {pr.srcCommit} (branch ***{pr.srcBranch}***)",
            f"Destination commit (to) {pr.dstCommit} (branch ***{pr.dstBranch}***)",
            f"",
        ]

        if pr.state != OPENED_PR_STATE:
            desc = f"_Closed by **{pr.closedBy}**"
            closeInfo = append_timestamp_string_if_possible(desc, pr.closedAt, errorDescription=f"Bad PR {pr.id} closing date:") + "_"

            descriptionParts.insert(0, closeInfo)

        desc = f"_Created by **{pr.user}**"
        createInfo = append_timestamp_string_if_possible(desc, pr.createdAt, errorDescription=f"Bad PR {pr.id} creation date:") + "_"
        descriptionParts.insert(0, createInfo)

        if pr.declineReason != '':
            descriptionParts.append("Decline message:")
            descriptionParts.append(pr.declineReason)
            descriptionParts.append('')

        if pr.mergeCommit != '':
            descriptionParts.append(f"Merged to commit {pr.mergeCommit}")
            descriptionParts.append('')

        descriptionParts.append("Original description:")
        descriptionParts.append(await pr_all_process_body(session, pr, prsCache))

        newDescription = '\n'.join(descriptionParts)

        print("Creating PR", pr.id)

        await create_pr(session, newTitle, newDescription, formatBranchName(pr.id, SRC_BRANCH_PREFIX, pr.srcBranch), formatBranchName(pr.id, DST_BRANCH_PREFIX, pr.dstBranch))

        return True
    except aiohttp.ClientResponseError as e:
        print(f"HTTP Exception was caught for PR {pr.id} PR creation")
        print(f"HTTP code {e.status}")
        print(e.message)
        print()
        if e.status in HTTP_EXIT_CODES:
            exit(e.status)
    except Exception as e:
        print(f"Exception was caught for PR {pr.id} PR creation")
        print(e)
        print()

    # PR was not created
    return False

async def pr_all_process_body(session, prOrComment, prsCache={}):
    raw = prOrComment.body
    html = prOrComment.bodyHtml

    # Processing username cites
    # They are formatted in raw body as @{GUID} or @{number:GUID}
    matches = re.findall(r'@\{(?:\d+:)?[0-9A-Fa-f-]+\}', raw)
    # Force making matches unique
    matches = set(matches)
    ## need to replace all matches
    for m in matches:
        realId = m[2:-1]
        # '@' not in match for removing unwanted triggers
        idSearch = re.search(re.escape(realId) + r'"[^>]*>@([^<>]*)</', html)
        if not idSearch:
            print(f"Corrupted HTML message for object {prOrComment.id}")
            continue

        realUser = idSearch.group(1)

        # User name will be bold & italic
        raw = raw.replace(m, f"***{realUser}***")

    # Processing absolute URLs for bitbucket
    matches = re.findall(URLS_REGEX, raw)
    for i, m in enumerate(matches):
        if m.endswith(')'):
            # That is regex bug
            matches[i] = m[:-1]

    matches = set(matches)
    for url in matches:
        if not url.startswith(SOURCE_SERVER_ABSOLUTE_URL_PREFIX):
            print(f"Unsupported URL '{url}' was detected for for PR/PR comment {prOrComment.id}. Skipping it")
            continue

        if SOURCE_SERVER_IMAGES_CONTAINS in url:
            # This is image, need to check

            # Based on name encoding on saving
            diskFileName = unquote(url).replace(':', '_').replace('/', '_')
            # IMAGES_ADDITIONAL_INFO_PATH already has last '/'
            fullDiskName = IMAGES_ADDITIONAL_INFO_PATH + diskFileName

            try:
                attachRes = await attach_file(session, fullDiskName)
                attachRes = json.loads(attachRes)

                # Thanks, Postman & reverse
                newUrl = attachRes["attachments"][0]["links"]["attachment"]["href"]

                raw = raw.replace(url, newUrl)
            except aiohttp.ClientResponseError as e:
                print(f"Cannot attach file from old URL '{url}'. HTTP error {e.status}, message {e.message}")
            except Exception as e:
                print(f"Cannot attach file from old URL '{url}'. Error message {e}")

            continue

        prIdMatch = re.search(re.escape(SOURCE_SERVER_ABSOLUTE_URL_PREFIX) + r'.*/([^/]+)/pull-requests/(\d+)', url)
        if prIdMatch:
            # This is PR or PR comment, should process as PR link
            requiredRepoName = prIdMatch.group(1)
            oldPrId = prIdMatch.group(2)

            allPrs = []
            try:
                if requiredRepoName in prsCache:
                    allPrs = prsCache[requiredRepoName]
                else:
                    # Searching in ANOTHER REPOSITORY (possible)
                    allPrs = await list_all_prs(session, filterTitle=PR_START_NAME, repo=requiredRepoName)

                    prsCache[requiredRepoName] = allPrs
            except aiohttp.ClientResponseError as e:
                print(f"Cannot receive list with new PRs for old URL '{url}'. HTTP error {e.status}, message {e.message}")
            except Exception as e:
                print(f"Cannot receive list with new PRs for old URL '{url}'. Error message {e}")

            newPrId = None
            for v in allPrs:
                prId = v["id"]
                prTitle = v["title"]

                # get first number, as described in PR title creation
                numberSearch = re.search(r'\d+', prTitle)
                if not numberSearch:
                    continue

                capturedPrId = numberSearch.group()
                if capturedPrId == oldPrId:
                    newPrId = str(prId)
                    break

            if newPrId == None:
                errorMsg = f"Cannot find new PR id for old URL '{url}'"
                print(errorMsg)
                if not FORCE_CREATE_PRS_WITH_BAD_CROSS_REFS:
                    try:
                        # PR comments always force created, because they must be load after all possible PRs were created
                        prOrComment.prId
                    except AttributeError:
                        # Here => this is PR, not PR comment
                        raise Exception(errorMsg)

                # Set pseudo PR ID
                newPrId = "OLD_" + oldPrId

            newUrl = SERVER + SERVER_PROJECTS_SUBSTRING + PROJECT + '/' + SERVER_REPOS_SUBSTRING + requiredRepoName + '/' + 'pull-requests/' + newPrId

            raw = raw.replace(url, newUrl)

            continue

        # Trying to find old project name & replace it
        # At first position must be max match paths
        possiblePrefixes = [
            SOURCE_SERVER_ABSOLUTE_URL_PREFIX + 'projects/' + OLD_PROJECT_NAME + '/' + 'repos/',
            SOURCE_SERVER_ABSOLUTE_URL_PREFIX + 'projects/' + OLD_PROJECT_NAME + '/',
            SOURCE_SERVER_ABSOLUTE_URL_PREFIX + OLD_PROJECT_NAME + '/',
        ]

        newUrl = None
        for p in possiblePrefixes:
            if url.startswith(p):
                print(f"Unknown project URL type '{url}' was detected for PR/PR comment {prOrComment.id}. Force replacing with current project")

                # Bitbucket server
                newUrl = SERVER + SERVER_PROJECTS_SUBSTRING + PROJECT + '/' + SERVER_REPOS_SUBSTRING + url[len(p):]

                break

        if not newUrl:
            print(f"Unknown URL type '{url}' was detected for PR/PR comment {prOrComment.id}. Force replacing with current root server")

            newUrl = SERVER + url[len(SOURCE_SERVER_ABSOLUTE_URL_PREFIX):]

        raw = raw.replace(url, newUrl)

    return raw

# Returns True if base PR comment exists, else False
async def form_single_pr_comment(session, currComment, newCommentIds, prInfo, diffs={}, prsCache={}):
    # Receiving PR info
    if not currComment.prId in prInfo:
        print("Old PR", currComment.prId, "was not created. Comment", currComment.id, "cannot be created too")
        return True
    newPr = prInfo[currComment.prId]

    parent = None
    if currComment.parentCommentId:
        if not currComment.parentCommentId in newCommentIds:
            return False

        parent = newCommentIds[currComment.parentCommentId]

    textParts = [
        append_timestamp_string_if_possible(f"_Created by **{currComment.user}** for commit {currComment.commit}", currComment.createdAt, errorDescription=f"Bad PR {currComment.prId} comment {currComment.id} creation date:") + '_',
    ]

    if currComment.isDeleted == 'true':
        print(f"Comment {currComment.id} for original PR {currComment.prId} was deleted")
        textParts.append("Message was previously deleted")
    textParts.append("")

    # Printing before diff, because diff may be very long
    textParts.append(f"Original message:")
    textParts.append(await pr_all_process_body(session, currComment, prsCache))
    textParts.append("")

    lineNum = None
    if currComment.file != '':
        if currComment.fromLine != '':
            fileType = 'FROM'
            fileTypeText = 'Source'
            lineType = 'REMOVED'
            lineNum = currComment.fromLine
        else:
            fileType = 'TO'
            fileTypeText = 'Current'
            lineType = 'ADDED'
            lineNum = currComment.toLine

        # Printing source file & diff info only for root comment
        if not parent:
            # Force delim info from source message
            textParts.append("----")
            textParts.append(f"Source file ***{currComment.file}***")
            textParts.append(f"{fileTypeText} commit line {lineNum}")
            textParts.append("")

        if currComment.diffUrl:
            if not parent:
                if currComment.diffUrl in diffs:
                    if PRINT_ATTACHED_DIFFS:
                        textParts.append("Original diff:")
                        textParts.append("```diff")
                        textParts.append(diffs[currComment.diffUrl])
                        textParts.append("```")
                else:
                    textParts.append(f"This comment is for an outdated diff")

    # merge parts to single text
    text = '\n'.join(textParts)

    try:
        print("Uploading comment", currComment.id, "for original PR", currComment.prId)

        res = None
        if parent != None:
            res = await create_pr_comment(session, newPr.id, text, parent)
        else:
            if currComment.file:
                try:
                    # Trying to create file with bitbucket diff, not our
                    res = await create_pr_file_comment(session, newPr.id, text, currComment.file, lineNum, fileType, lineType)
                except aiohttp.ClientResponseError as e:
                    print(f"Creating file comment {currComment.id} from PR {currComment.prId} as usual file. HTTP error {e.status}, message {e.message}")
                except Exception as e:
                    print(f"Creating file comment {currComment.id} from PR {currComment.prId} as usual file. Error message {e}")

            # file comment was not created or that is usual comment
            if not res:
                res = await create_pr_comment(session, newPr.id, text)

        res = json.loads(res)
        newCommentIds[currComment.id] = res["id"]

    except aiohttp.ClientResponseError as e:
        print(f"HTTP Exception was caught for PR {currComment.prId} comment {currComment.id} creation")
        print(f"HTTP code {e.status}")
        print(e.message)
        print()
        if e.status in HTTP_EXIT_CODES:
            exit(e.status)
    except Exception as e:
        print(f"Exception was caught for PR {currComment.prId} comment {currComment.id} creation")
        print(e)
        print()

    return True

async def upload_pr_comments(session, data):
    if CURRENT_MODE != ProcessingMode.LOAD_INFO:
        print("Mode is not supported for comments uploading")
        return 0

    headers = data[0]

    comments = []

    # Parse data
    for d in data[1:]:
        repo = d[headers.index('Repository')]
        prNumber = d[headers.index('PRNumber')]
        user = d[headers.index('User')]
        currType = d[headers.index('CommentType')]
        currId = d[headers.index('CommentID')]
        body = d[headers.index('BodyRaw')]
        bodyHtml = d[headers.index('BodyHTML')]
        createdAt = d[headers.index('CreatedAt')]
        isDeleted = d[headers.index('IsDeleted')]
        toLine = d[headers.index('ToLine')]
        fromLine = d[headers.index('FromLine')]
        file = d[headers.index('FilePath')]
        diffUrl = d[headers.index('Diff')]
        parentComment = d[headers.index('ParentID')]
        commit = d[headers.index('CommitHash')]

        if repo.lower() != REPO:
            # Block creating another PRs
            continue

        comment = PRComment(repo, prNumber, user, currType, currId, body, bodyHtml, createdAt, isDeleted, toLine, fromLine, file, diffUrl, parentComment, commit)
        comments.append(comment)

    prInfo = {}

    # Loading PR info
    try:
        print(f"Loading PR info")

        res = await list_all_prs(session, filterTitle=PR_START_NAME)

        for v in res:
            prId = v["id"]
            prTitle = v["title"]
            prVersion = v["version"]

            # get first number, as described in PR title creation
            numberSearch = re.search(r'\d+', prTitle)
            if not numberSearch:
                print(f"Bad PR {prId}: unsupported title: '{prTitle}'")
                continue

            originalPrId = numberSearch.group()

            prInfo[originalPrId] = PullRequestShort(prId, prVersion)
    except aiohttp.ClientResponseError as e:
        print(f"HTTP Exception was caught while loading PR info")
        print(f"HTTP code {e.status}")
        print(e.message)
        print()
        if e.status in HTTP_EXIT_CODES:
            exit(e.status)
    except Exception as e:
        print(f"Exception was caught while loading PR info")
        print(e)
        print()

    # key will be old comment id, value will be new comment id
    newCommentIds = {}

    commentsToCheckAgain = comments

    prevCommentNumber = 0

    # Block for infinite loop
    while prevCommentNumber != len(commentsToCheckAgain):
        prevCommentNumber = len(commentsToCheckAgain)
        print(prevCommentNumber, "comments will be checked for loading")

        # Uses for speed up downloading big repo PRs
        # Will be cleaned on every iteration loop for loading new self-cross-references
        prsCache = {}

        loadResults = await asyncio.gather(*[form_single_pr_comment(session, c, newCommentIds, prInfo, JSON_ADDITIONAL_INFO, prsCache) for c in commentsToCheckAgain])

        # check what wasn't uploaded
        newCommentsToCheckAgain = []
        for i in range(prevCommentNumber):
            if not loadResults[i]:
                newCommentsToCheckAgain.append(commentsToCheckAgain[i])

        # save prev iteration as current
        commentsToCheckAgain = newCommentsToCheckAgain

    if len(commentsToCheckAgain) != 0:
        print("Cannot find parents for that comments:")
        for c in commentsToCheckAgain:
            print("ID", c.id, "body", c.body)

    return len(commentsToCheckAgain)

async def delete_all_branches(session, filterText=None):
    try:
        while True:
            res = await list_branches(session, filterText)
            res = json.loads(res)

            tasks = []

            for v in res["values"]:
                branchId = v["id"]
                print("Deleting", branchId)
                tasks.append(delete_branch_no_error(session, branchId))

            # Wait all tasks for that iteration
            await asyncio.gather(*tasks)

            if res["isLastPage"]:
                break
    except aiohttp.ClientResponseError as e:
        print(f"HTTP Exception was caught while all branches deleting")
        print(f"HTTP code {e.status}")
        print(e.message)
        print()
        if e.status in HTTP_EXIT_CODES:
            exit(e.status)
    except Exception as e:
        print(f"Exception was caught while all branches deleting")
        print(e)
        print()

async def list_all_prs(session, state=ANY_PR_STATE, filterTitle=None, repo=None):
    allPrs = []

    try:
        start = 0

        while True:
            res = await list_prs(session, start, state, filterTitle, repo)
            res = json.loads(res)

            for v in res["values"]:
                prId = v["id"]
                prTitle = v["title"]
                if filterTitle and not filterTitle in prTitle:
                    start += 1

                    print("Skipping PR", prId, "with title", prTitle)

                    continue

                allPrs.append(v)

            if res["isLastPage"]:
                break

            start = res["nextPageStart"]
    except aiohttp.ClientResponseError as e:
        print(f"HTTP Exception was caught while all PRs listing")
        print(f"HTTP code {e.status}")
        print(e.message)
        print()
        if e.status in HTTP_EXIT_CODES:
            exit(e.status)
    except Exception as e:
        print(f"Exception was caught while all PRs listing")
        print(e)
        print()

    return allPrs

async def close_all_prs(session, filterTitle=None):
    state=OPENED_PR_STATE

    try:
        start = 0

        while True:
            res = await list_prs(session, start, state, filterTitle)
            res = json.loads(res)

            tasks = []

            for v in res["values"]:
                prId = v["id"]
                prTitle = v["title"]
                prVersion = v["version"]
                if filterTitle and not filterTitle in prTitle:
                    start += 1

                    print("Skipping PR", prId, "with title", prTitle)

                    continue

                print("Closing PR", prId, "version", prVersion, "with title", prTitle)
                tasks.append(close_pr_no_error(session, prId, prVersion, "Imported pull request"))

            # Wait all tasks for that iteration
            await asyncio.gather(*tasks)

            if res["isLastPage"]:
                break
    except aiohttp.ClientResponseError as e:
        print(f"HTTP Exception was caught while all PRs closing")
        print(f"HTTP code {e.status}")
        print(e.message)
        print()
        if e.status in HTTP_EXIT_CODES:
            exit(e.status)
    except Exception as e:
        print(f"Exception was caught while all PRs closing")
        print(e)
        print()

async def delete_all_prs(session, filterTitle=None, state=OPENED_PR_STATE):
    try:
        start = 0

        while True:
            res = await list_prs(session, start, state, filterTitle)
            res = json.loads(res)

            tasks = []

            for v in res["values"]:
                prId = v["id"]
                prTitle = v["title"]
                prVersion = v["version"]
                if filterTitle and not filterTitle in prTitle:
                    start += 1

                    print("Skipping PR", prId, "with title", prTitle)

                    continue

                print("Deleting PR", prId, "with title", prTitle)
                tasks.append(delete_pr_no_error(session, prId, prVersion))

            # Wait all tasks for that iteration
            await asyncio.gather(*tasks)

            if res["isLastPage"]:
                break
    except aiohttp.ClientResponseError as e:
        print(f"HTTP Exception was caught while all PRs deleting")
        print(f"HTTP code {e.status}")
        print(e.message)
        print()
        if e.status in HTTP_EXIT_CODES:
            exit(e.status)
    except Exception as e:
        print(f"Exception was caught while all PRs deleting")
        print(e)
        print()

async def delete_branch_no_error(session, branchId):
    try:
        return await delete_branch(session, branchId)
    except aiohttp.ClientResponseError as e:
        print(f"HTTP Exception was caught while deleting branch {branchId}")
        print(f"HTTP code {e.status}")
        print(e.message)
        print()
        if e.status in HTTP_EXIT_CODES:
            exit(e.status)
    except Exception as e:
        print(f"Exception was caught while deleting branch {branchId}")
        print(e)
        print()

async def close_pr_no_error(session, id, version, comment):
    try:
        return await close_pr(session, id, version, comment)
    except aiohttp.ClientResponseError as e:
        print(f"HTTP Exception was caught while PR {id} closing")
        print(f"HTTP code {e.status}")
        print(e.message)
        print()
        if e.status in HTTP_EXIT_CODES:
            exit(e.status)
    except Exception as e:
        print(f"Exception was caught while PR {id} closing")
        print(e)
        print()

async def delete_pr_no_error(session, prId, prVersion):
    try:
        return await delete_pr(session, prId, prVersion)
    except aiohttp.ClientResponseError as e:
        print(f"HTTP Exception was caught while PR {prId} deleting")
        print(f"HTTP code {e.status}")
        print(e.message)
        print()
        if e.status in HTTP_EXIT_CODES:
            exit(e.status)
    except Exception as e:
        print(f"Exception was caught while PR {prId} deleting")
        print(e)
        print()

async def main(argv):
    init()
    args_read(argv)

    if CURRENT_MODE == None:
        print("Current mode was not selected")
        return

    async with aiohttp.ClientSession() as session:
        await main_select_mode(session)

async def main_select_mode(session):
    if CURRENT_MODE == ProcessingMode.DEBUG:
        print("Src file:", SRC_FILE)
        print("Target (new) URL:", SERVER)
        print("API:", SERVER_API_VERSION)
        print("Auth:", AUTH.login, AUTH.password)
        print("Prj:", PROJECT)
        print("Repo:", REPO)
        print("Source server:", SOURCE_SERVER_ABSOLUTE_URL_PREFIX)
        print("Old project name:", OLD_PROJECT_NAME)
        print("Images folder:", IMAGES_ADDITIONAL_INFO_PATH)
        print("JSON file:", JSON_ADDITIONAL_INFO_FILE)

        return

    if CURRENT_MODE == ProcessingMode.CLOSE_PRS:
        await close_all_prs(session, PR_START_NAME)
        return

    if CURRENT_MODE == ProcessingMode.DELETE_BRANCHES_PRS or CURRENT_MODE == ProcessingMode.DELETE_PRS:
        # Must be done before branches removing
        await delete_all_prs(session, PR_START_NAME, ANY_PR_STATE)
    if CURRENT_MODE == ProcessingMode.DELETE_BRANCHES or CURRENT_MODE == ProcessingMode.DELETE_BRANCHES_PRS:
        await delete_all_branches(session, BRANCH_START_NAME)

    if CURRENT_MODE == ProcessingMode.DELETE_BRANCHES or CURRENT_MODE == ProcessingMode.DELETE_BRANCHES_PRS or CURRENT_MODE == ProcessingMode.DELETE_PRS:
        return

    global JSON_ADDITIONAL_INFO
    JSON_ADDITIONAL_INFO = read_json_file(JSON_ADDITIONAL_INFO_FILE)
    data = read_csv_file(SRC_FILE)
    if len(data) == 0:
        print("Data was empty")
    elif len(data[0]) == 0:
        print("Data header was empty")
    elif data[0][-1] == 'ClosedBy':
        print("PRs were found. Uploading them")
        res = await upload_prs(session, data)
        print(res, "PRs were not created")
        return res
    elif data[0][-1] == 'CommitHash':
        print("PRs comments were found. Uploading them")
        res = await upload_pr_comments(session, data)
        print(res, "PR comments were not created")
        return res
    else:
        print("Unknown source file format")

if __name__ == '__main__':
    # force set event loop for execution on python 3.10+
    # Based on https://stackoverflow.com/a/73367187/6818663
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        asyncio.run(main(sys.argv))
    except KeyboardInterrupt:
        pass
