#!/usr/bin/env python3.10
# Args sequence:
## $1 = source file name (csv-formatted data from ruby)
## $2 = server URL (such as 'https://bitbucket.org/')
## $3 = server auth info: "username:password"
## $4 = server project/repo combination (such as 'my-workspace/test-repo')
## $5 = (optional) additional options:
### "" (nothing, not passed or not supported) = load info from file to PRs
### -uPRs = load info from file to PRs (not recreate branches)
### -dAll = delete all created branches & PRs
### -dBranches = delete all created branches (keep PRs)
### -cPRs = close (decline) all created PRs
### -dPRs = delete all created PRs (keep branches)
### any_filename.json = json file will additional info:
#### - PR comments uses that info in format key:value, where key = diff URL (usually bitbucket API), value = downloaded diff info from that URL

import csv
from enum import Enum
import json
import re
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
    DELETE_PRS = 4
    LOAD_INFO_ONLY_PRS = 5
    CLOSE_PRS = 6

CURRENT_MODE = ProcessingMode.LOAD_INFO
JSON_ADDITIONAL_INFO = {}

class PullRequest:
    def __init__(self, id, user, title, state, body, bodyHtml, srcCommit, dstCommit, srcBranch, dstBranch, declineReason, mergeCommit, closedBy):
        self.id = id
        self.user = user
        self.title = title
        self.state = state
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
    def __init__(self, repo, prNumber, user, currType, currId, body, bodyHtml, isDeleted, toLine, fromLine, file, diffUrl, parentComment, commit):
        self.repo = repo
        self.prId = prNumber
        self.user = user
        self.currType = currType
        self.id = currId
        self.body = body
        self.bodyHtml = bodyHtml
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
        if mode == '-dAll':
            CURRENT_MODE = ProcessingMode.DELETE_BRANCHES_PRS
        elif mode == '-dBranches':
            CURRENT_MODE = ProcessingMode.DELETE_BRANCHES
        elif mode == '-dPRs':
            CURRENT_MODE = ProcessingMode.DELETE_PRS
        elif mode == '-uPRs':
            CURRENT_MODE = ProcessingMode.LOAD_INFO_ONLY_PRS
        elif mode == '-cPRs':
            CURRENT_MODE = ProcessingMode.CLOSE_PRS
        elif mode.endswith('.json'):
            with open(mode, "r") as f:
                global JSON_ADDITIONAL_INFO
                JSON_ADDITIONAL_INFO = json.load(f)

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

def close_pr(id, version):
    payload = {
        "version": version,
    }

    res = requests.post(formatTemplate(URL_CLOSE_PR, prId=id), auth=AUTH, headers=POST_HEADERS, params=payload)
    res.raise_for_status()
    return res.text

def delete_pr(id, version):
    payload = {
        "version": version,
    }

    res = requests.delete(formatTemplate(URL_DELETE_PR, prId=id), auth=AUTH, headers=POST_HEADERS, json=payload)
    res.raise_for_status()
    return res.text

def create_pr_file_comment(prId, text, filePath, lineNum, fileType="TO", lineType="CONTEXT", fromHash=None, toHash=None, diffType="RANGE"):
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

    res = requests.post(formatTemplate(URL_CREATE_PR_COMMENT, prId=prId), auth=AUTH, headers=POST_HEADERS, json=payload)
    res.raise_for_status()
    return res.text

def create_pr_comment(prId, text, parentCommit=None):
    payload = {
        "text": text,
    }

    if parentCommit:
        payload["parent"] = {
            "id": parentCommit,
        }

    res = requests.post(formatTemplate(URL_CREATE_PR_COMMENT, prId=prId), auth=AUTH, headers=POST_HEADERS, json=payload)
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
        bodyHtml = d[headers.index('BodyHTML')]
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

        pr = PullRequest(number, user, title, state, body, bodyHtml, src, dst, srcBranch, dstBranch, declineReason, mergeCommit, closedBy)
        prs.append(pr)

    # Should create old PRs at the beginning
    prs.reverse()

    if CURRENT_MODE != ProcessingMode.LOAD_INFO_ONLY_PRS:
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
            # First number in title must be original PR number
            # for correct comments uploading
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
            descriptionParts.append(pr_all_process_body(pr))

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

def pr_all_process_body(comment):
    raw = comment.body
    html = comment.bodyHtml

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
            print(f"Corrupted HTML message for object {comment.id}")
            continue

        realUser = idSearch.group(1)

        # User name will be bold & italic
        raw = raw.replace(m, f"***{realUser}***")

    return raw

# Returns True if base PR comment exists, else False
def form_single_pr_comment(currComment, newCommentIds, prInfo, diffs={}):
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
        f"Created by _{currComment.user}_ for commit {currComment.commit}",
    ]

    if currComment.isDeleted == 'true':
        print(f"Comment {currComment.id} for original PR {currComment.prId} was deleted")
        textParts.append("Message was previously deleted")
    textParts.append("")

    # Printing before diff, because diff may be very long
    textParts.append(f"Original message:")
    textParts.append(pr_all_process_body(currComment))
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
                    textParts.append("Original diff:")
                    textParts.append("```diff")
                    textParts.append(diffs[currComment.diffUrl])
                    textParts.append("```")
                else:
                    textParts.append(f"Original diff with URL {currComment.diffUrl} was lost")

    # merge parts to single text
    text = '\n'.join(textParts)

    try:
        print("Uploading comment", currComment.id, "for original PR", currComment.prId)

        res = None
        if parent != None:
            res = create_pr_comment(newPr.id, text, parent)
        else:
            if currComment.file:
                try:
                    # Trying to create file with bitbucket diff, not our
                    res = create_pr_file_comment(newPr.id, text, currComment.file, lineNum, fileType, lineType)
                except requests.exceptions.HTTPError as e:
                    print(f"Creating file comment {currComment.id} from PR {currComment.prId} as usual file. HTTP error {e.response.status_code}, message {e.response.text}")
                except Exception as e:
                    print(f"Creating file comment {currComment.id} from PR {currComment.prId} as usual file. Error message {e}")

            # file comment was not created or that is usual comment
            if not res:
                res = create_pr_comment(newPr.id, text)

        res = json.loads(res)
        newCommentIds[currComment.id] = res["id"]

    except requests.exceptions.HTTPError as e:
        print(f"HTTP Exception was caught for PR {currComment.prId} comment {currComment.id} creation")
        print(f"HTTP code {e.response.status_code}")
        print(e.response.text)
        print()
    except Exception as e:
        print(f"Exception was caught for PR {currComment.prId} comment {currComment.id} creation")
        print(e)
        print()

    return True

def upload_pr_comments(data):
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

        comment = PRComment(repo, prNumber, user, currType, currId, body, bodyHtml, isDeleted, toLine, fromLine, file, diffUrl, parentComment, commit)
        comments.append(comment)

    prInfo = {}

    # Loading PR info
    pagingOffset = 0
    while True:
        try:
            print(f"Loading PR info with paging offset {pagingOffset}")

            res = list_prs(pagingOffset, "ALL")
            res = json.loads(res)

            for v in res["values"]:
                prId = v["id"]
                prTitle = v["title"]
                prVersion = v["version"]
                if PR_START_NAME and not PR_START_NAME in prTitle:
                    continue

                # get first number, as described in PR title creation
                numberSearch = re.search(r'\d+', prTitle)
                if not numberSearch:
                    print(f"Bad PR {prId}: unsupported title: '{prTitle}'")
                    continue

                originalPrId = numberSearch.group()

                prInfo[originalPrId] = PullRequestShort(prId, prVersion)

            if res["isLastPage"]:
                break

            pagingOffset = res["nextPageStart"]
        except requests.exceptions.HTTPError as e:
            print(f"HTTP Exception was caught while loading PR info (pagination offset {pagingOffset})")
            print(f"HTTP code {e.response.status_code}")
            print(e.response.text)
            print()
        except Exception as e:
            print(f"Exception was caught while loading PR info (pagination offset {pagingOffset})")
            print(e)
            print()

    # key will be old comment id, value will be new comment id
    newCommentIds = {}

    commentsToCheckAgain = []

    for c in comments:
        if not form_single_pr_comment(c, newCommentIds, prInfo, JSON_ADDITIONAL_INFO):
            commentsToCheckAgain.append(c)

    prevCommentNumber = 0

    print(len(commentsToCheckAgain), "comments will be checked for loading again")

    # Block for infinite loop
    while prevCommentNumber != len(commentsToCheckAgain):
        prevCommentNumber = len(commentsToCheckAgain)

        newCommentsToCheckAgain = []
        for c in commentsToCheckAgain:
            if not form_single_pr_comment(c, newCommentIds, prInfo, JSON_ADDITIONAL_INFO):
                newCommentsToCheckAgain.append(c)

        # save prev iteration as current
        commentsToCheckAgain = newCommentsToCheckAgain

    if len(commentsToCheckAgain) != 0:
        print("Cannot find parents for that comments:")
        for c in commentsToCheckAgain:
            print("ID", c.id, "body", c.body)

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
        print(f"HTTP Exception was caught while all branches deleting")
        print(f"HTTP code {e.response.status_code}")
        print(e.response.text)
        print()
    except Exception as e:
        print(f"Exception was caught while all branches deleting")
        print(e)
        print()

def close_all_prs(filterTitle=None):
    state="OPEN"

    try:
        start = 0

        while True:
            res = list_prs(start, state)
            res = json.loads(res)

            for v in res["values"]:
                prId = v["id"]
                prTitle = v["title"]
                prVersion = v["version"]
                if filterTitle and not filterTitle in prTitle:
                    start += 1

                    print("Skipping PR", prId, "with title", prTitle)

                    continue

                print("Closing PR", prId, "with title", prTitle)
                close_pr(prId, prVersion)

            if res["isLastPage"]:
                break
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Exception was caught while all PRs closing")
        print(f"HTTP code {e.response.status_code}")
        print(e.response.text)
        print()
    except Exception as e:
        print(f"Exception was caught while all PRs closing")
        print(e)
        print()

def delete_all_prs(filterTitle=None, state="OPEN"):
    try:
        start = 0

        while True:
            res = list_prs(start, state)
            res = json.loads(res)

            for v in res["values"]:
                prId = v["id"]
                prTitle = v["title"]
                prVersion = v["version"]
                if filterTitle and not filterTitle in prTitle:
                    start += 1

                    print("Skipping PR", prId, "with title", prTitle)

                    continue

                print("Deleting PR", prId, "with title", prTitle)
                delete_pr(prId, prVersion)

            if res["isLastPage"]:
                break
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Exception was caught while all PRs deleting")
        print(f"HTTP code {e.response.status_code}")
        print(e.response.text)
        print()
    except Exception as e:
        print(f"Exception was caught while all PRs deleting")
        print(e)
        print()

def main():
    init()
    args_read()

    if CURRENT_MODE == ProcessingMode.CLOSE_PRS:
        close_all_prs(PR_START_NAME)
        return

    if CURRENT_MODE == ProcessingMode.DELETE_BRANCHES_PRS or CURRENT_MODE == ProcessingMode.DELETE_PRS:
        # Must be done before branches removing
        delete_all_prs(PR_START_NAME, "ALL")
    if CURRENT_MODE == ProcessingMode.DELETE_BRANCHES or CURRENT_MODE == ProcessingMode.DELETE_BRANCHES_PRS:
        delete_all_branches(BRANCH_START_NAME)
    if CURRENT_MODE != ProcessingMode.LOAD_INFO and CURRENT_MODE != ProcessingMode.LOAD_INFO_ONLY_PRS:
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
