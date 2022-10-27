#!/usr/bin/env python3.10
# Args sequence:
## $1 = source file name
## $2 = server URL (such as 'https://bitbucket.org/')
## $3 = server auth info: "username:password"
## $4 = server project/repo combination (such as 'my-workspace/test-repo')
## $5 = (optional) additional options:
### "" (nothing, not passed or not supported) = load info from file to PRs
### -D = delete all created branches & PRs
### -d = delete all created branches (keep PRs)

import csv
from enum import Enum
import json
import requests
from requests.auth import HTTPBasicAuth
import sys

SRC_BRANCH_PREFIX = 'src'
DST_BRANCH_PREFIX = 'dst'
# Used by creation & filtering too, uses '[' for generating more specific output
PR_START_NAME = "[Bitbucket Import"
BRANCH_START_NAME = "bitbucket/"

class ProcessingMode(Enum):
    LOAD_INFO = 1
    DELETE_BRANCHES = 2
    DELETE_BRANCHES_PRS = 3

CURRENT_MODE = ProcessingMode.LOAD_INFO

class PullRequest:
    def __init__(self, id, user, title, state, body, srcCommit, dstCommit, srcBranch, dstBranch, declineReason, mergeCommit, closedBy):
        self.id = id
        self.user = user
        self.title = title
        self.state = state
        self.body = body
        self.srcCommit = srcCommit
        self.dstCommit = dstCommit
        self.srcBranch = srcBranch
        self.dstBranch = dstBranch
        self.declineReason = declineReason
        self.mergeCommit = mergeCommit
        self.closedBy = closedBy

class PRComment:
    def __init__(self, repo, prNumber, user, currType, currId, body, isDeleted, toLine, fromLine, file, diffUrl, parentComment, commit):
        self.repo = repo
        self.prNumber = prNumber
        self.user = user
        self.currType = currType
        self.currId = currId
        self.body = body
        self.isDeleted = isDeleted
        self.toLine = toLine
        self.fromLine = fromLine
        self.file = file
        self.diffUrl = diffUrl
        self.parentComment = parentComment
        self.commit = commit

def init():
    # see https://stackoverflow.com/a/15063941/6818663
    csv.field_size_limit(sys.maxsize)

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

    # From https://confluence.atlassian.com/cloudkb/xsrf-check-failed-when-calling-cloud-apis-826874382.html
    global POST_HEADERS
    POST_HEADERS = {"X-Atlassian-Token": "no-check"}

def args_read():
    global SRC_FILE
    SRC_FILE = sys.argv[1]

    global SERVER
    SERVER = sys.argv[2]
    if not SERVER.endswith('/'):
        SERVER += '/'

    global SERVER_API_VERSION
    # 1 for custom bitbucket server/datacenter, 2 for cloud
    if 'bitbucket.org' in SERVER:
        SERVER_API_VERSION = 2
    else:
        SERVER_API_VERSION = 1

    SERVER_API_VERSION = f"{SERVER_API_VERSION}.0"

    USER_PASS = sys.argv[3]
    userPassSplit = USER_PASS.split(':')

    global AUTH
    AUTH = HTTPBasicAuth(userPassSplit[0], userPassSplit[1])

    PROJECT_REPO = sys.argv[4]
    prjRepoSplit = PROJECT_REPO.split('/')

    global PROJECT
    global REPO

    PROJECT = prjRepoSplit[0].lower()
    REPO = prjRepoSplit[1].lower()

    if len(sys.argv) > 5:
        global CURRENT_MODE
        mode = sys.argv[5]
        if mode == '-D':
            CURRENT_MODE = ProcessingMode.DELETE_BRANCHES_PRS
        elif mode == '-d':
            CURRENT_MODE = ProcessingMode.DELETE_BRANCHES

def read_file(path):
    rows = []
    with open(path) as src:
        inReader = csv.reader(src)

        for row in inReader:
            rows.append(row)

    return rows

def formatTemplate(template, prId=None, commitId=None):
    return template.format(
        endpoint=SERVER,
        version=SERVER_API_VERSION,
        projectKey=PROJECT,
        repositorySlug=REPO,
        pullRequestId=prId,
        commitId=commitId
    )

def formatBranchName(id, prefix, originalName):
    res = f'{BRANCH_START_NAME}{id}/{prefix}/{originalName}'

    # we have limit of 111 chars
    # but it looks like need to be limited by 100:
    # https://jira.atlassian.com/browse/BSERV-10433
    return res[:100]

def create_pr(title, description = None, srcBranch = "prTest1", dstBranch = "stage"):
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

    res = requests.post(formatTemplate(URL_CREATE_PR), auth=AUTH, headers=POST_HEADERS, json=payload)
    res.raise_for_status()
    return res.text

def list_prs(start=0, state="OPEN"):
    payload = {
        "start": start,
        "state": state,
    }

    res = requests.get(formatTemplate(URL_CREATE_PR), auth=AUTH, params=payload)
    res.raise_for_status()

    return res.text

def delete_pr(id, version):
    payload = {
        "version": version,
    }

    res = requests.delete(formatTemplate(URL_DELETE_PR, prId=id), auth=AUTH, headers=POST_HEADERS, json=payload)
    res.raise_for_status()
    return res.text

def get_commit_info(commitToRead):
    res = requests.get(formatTemplate(URL_GET_COMMIT, commitId = commitToRead), auth=AUTH)
    res.raise_for_status()

    return res.text

def create_branch(name, commit):
    payload = {
        "name": name,
        "startPoint": commit,
    }

    res = requests.post(formatTemplate(URL_CREATE_BRANCH), auth=AUTH, headers=POST_HEADERS, json=payload)
    res.raise_for_status()
    return res.text

def list_branches(filterText=None, start=0):
    payload = {
        "start": start,
    }

    if filterText != None:
        payload["filterText"] = filterText

    res = requests.get(formatTemplate(URL_CREATE_BRANCH), auth=AUTH, params=payload)
    res.raise_for_status()

    return res.text

def delete_branch(id, dryRun=False):
    payload = {
        "name": id,
        "dryRun": dryRun,
    }

    res = requests.delete(formatTemplate(URL_DELETE_BRANCH, prId=id), auth=AUTH, headers=POST_HEADERS, json=payload)
    res.raise_for_status()
    return res.text

def upload_prs(data):
    headers = data[0]

    prs = []

    # Parse data
    for d in data[1:]:
        repo = d[headers.index('Repository')]
        number = d[headers.index('#')]
        user = d[headers.index('User')]
        title = d[headers.index('Title')]
        state = d[headers.index('State')]
        body = d[headers.index('BodyRaw')]
        src = d[headers.index('SourceCommit')]
        dst = d[headers.index('DestinationCommit')]
        srcBranch = d[headers.index('SourceBranch')]
        dstBranch = d[headers.index('DestinationBranch')]
        declineReason = d[headers.index('DeclineReason')]
        mergeCommit = d[headers.index('MergeCommit')]
        closedBy = d[headers.index('ClosedBy')]

        if repo != REPO:
            # Block creating another PRs
            continue

        pr = PullRequest(number, user, title, state, body, src, dst, srcBranch, dstBranch, declineReason, mergeCommit, closedBy)
        prs.append(pr)

    # Create branches
    for pr in prs:
        try:
            srcCommit = pr.srcCommit
            try:
                get_commit_info(srcCommit)
            except requests.exceptions.HTTPError as e:
                # Commit not found, using merge commit
                srcCommit = pr.mergeCommit

            try:
                get_commit_info(srcCommit)
            except requests.exceptions.HTTPError as e:
                # Commit not found, using none commit (next code will generate lots of exceptions)
                srcCommit = None

            print("Creating branches for PR", pr.id)

            create_branch(formatBranchName(pr.id, SRC_BRANCH_PREFIX, pr.srcBranch), srcCommit)
            create_branch(formatBranchName(pr.id, DST_BRANCH_PREFIX, pr.dstBranch), pr.dstCommit)
        except requests.exceptions.HTTPError as e:
            print(f"HTTP Exception was caught for PR {pr.id} branch creation")
            print(f"HTTP code {e.response.status_code}")
            print(e.response.text)
            print()
        except Exception as e:
            print(f"Exception was caught for PR {pr.id} branch creation")
            print(e)
            print()

    # Create pull requests
    for pr in prs:
        try:
            newTitle = f"{PR_START_NAME} {pr.id}, {pr.state}] {pr.title}"
            descriptionParts = [
                f"_Created by {pr.user}_",
                f"_Closed by {pr.closedBy}_",
                f"",
                f"Source commit (from) {pr.srcCommit} (branch ***{srcBranch}***)",
                f"Destination commit (to) {pr.dstCommit} (branch ***{dstBranch}***)",
                f"",
            ]

            if pr.declineReason != '':
                descriptionParts.append("Decline message:")
                descriptionParts.append(pr.declineReason)
                descriptionParts.append('')

            if pr.mergeCommit != '':
                descriptionParts.append(f"Merged to commit {pr.mergeCommit}")
                descriptionParts.append('')

            descriptionParts.append("Original description:")
            descriptionParts.append(pr.body)

            newDescription = '\n'.join(descriptionParts)

            print("Creating PR", pr.id)

            create_pr(newTitle, newDescription, formatBranchName(pr.id, SRC_BRANCH_PREFIX, pr.srcBranch), formatBranchName(pr.id, DST_BRANCH_PREFIX, pr.dstBranch))
        except requests.exceptions.HTTPError as e:
            print(f"HTTP Exception was caught for PR {pr.id} PR creation")
            print(f"HTTP code {e.response.status_code}")
            print(e.response.text)
            print()
        except Exception as e:
            print(f"Exception was caught for PR {pr.id} PR creation")
            print(e)
            print()

def upload_pr_comments(data):
    headers = data[0]

    comments = {}

    # Parse data
    for d in data[1:]:
        repo = d[headers.index('Repository')]
        prNumber = d[headers.index('PRNumber')]
        user = d[headers.index('User')]
        currType = d[headers.index('CommentType')]
        currId = d[headers.index('CommentID')]
        body = d[headers.index('BodyRaw')]
        isDeleted = d[headers.index('IsDeleted')]
        toLine = d[headers.index('ToLine')]
        fromLine = d[headers.index('FromLine')]
        file = d[headers.index('FilePath')]
        diffUrl = d[headers.index('Diff')]
        parentComment = d[headers.index('ParentID')]
        commit = d[headers.index('CommitHash')]

        if repo != REPO:
            # Block creating another PRs
            continue

        comment = PRComment(repo, prNumber, user, currType, currId, body, isDeleted, toLine, fromLine, file, diffUrl, parentComment, commit)
        comments[currId] = comment

    # TODO: implement
    pass

def delete_all_branches(filterText=None):
    try:
        while True:
            res = list_branches(filterText)
            res = json.loads(res)

            for v in res["values"]:
                branchId = v["id"]
                print("Deleting", branchId)
                delete_branch(branchId)

            if res["isLastPage"]:
                break
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Exception was caught while branch deleing")
        print(f"HTTP code {e.response.status_code}")
        print(e.response.text)
        print()
    except Exception as e:
        print(f"Exception was caught while branch deleing")
        print(e)
        print()

def delete_all_prs(filterTitle=None, state="OPEN", prVersion=0):
    try:
        start = 0

        while True:
            res = list_prs(start, state)
            res = json.loads(res)

            for v in res["values"]:
                prId = v["id"]
                prTitle = v["title"]
                if filterTitle and not filterTitle in prTitle:
                    start += 1

                    print("Skipping PR", prId, "with title", prTitle)

                    continue

                print("Deleting PR", prId, "with title", prTitle)
                delete_pr(prId, prVersion)

            if res["isLastPage"]:
                break
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Exception was caught while branch deleing")
        print(f"HTTP code {e.response.status_code}")
        print(e.response.text)
        print()
    except Exception as e:
        print(f"Exception was caught while branch deleing")
        print(e)
        print()

def main():
    init()
    args_read()

    if CURRENT_MODE == ProcessingMode.DELETE_BRANCHES_PRS:
        # Must be done before branches removing
        delete_all_prs(PR_START_NAME, "ALL")
    if CURRENT_MODE == ProcessingMode.DELETE_BRANCHES or CURRENT_MODE == ProcessingMode.DELETE_BRANCHES_PRS:
        delete_all_branches(BRANCH_START_NAME)
    if CURRENT_MODE != ProcessingMode.LOAD_INFO:
        return

    data = read_file(SRC_FILE)
    if len(data) == 0:
        print("Data was empty")
    elif len(data[0]) == 0:
        print("Data header was empty")
    elif data[0][-1] == 'ClosedBy':
        print("PRs were found. Uploading them")
        upload_prs(data)
    elif data[0][-1] == 'CommitHash':
        print("PRs comments were found. Uploading them")
        upload_pr_comments(data)
    else:
        print("Unknown source file format")

if __name__ == '__main__':
    main()
