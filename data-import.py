#!/usr/bin/env python3.10
# Args sequence:
## $1 = source file name
## $2 = server URL (such as 'https://bitbucket.org/')
## $3 = server auth info: "username:password"
## $4 = server project/repo combination (such as 'my-workspace/test-repo')

import csv
import sys
import requests
from requests.auth import HTTPBasicAuth

def init():
    # see https://stackoverflow.com/a/15063941/6818663
    csv.field_size_limit(sys.maxsize)

    # Init global templates
    global URL_CREATE_PR
    URL_CREATE_PR = "{endpoint}projects/{projectKey}/repos/{repositorySlug}/pull-requests"

    global URL_CREATE_COMMENT
    URL_CREATE_COMMENT = "{endpoint}projects/{projectKey}/repos/{repositorySlug}/pull-requests/{pullRequestId}/comments"

    global URL_CLOSE_PR
    URL_CLOSE_PR = "{endpoint}projects/{projectKey}/repos/{repositorySlug}/pull-requests/{pullRequestId}/decline"

    # From https://confluence.atlassian.com/cloudkb/xsrf-check-failed-when-calling-cloud-apis-826874382.html
    global POST_HEADERS
    POST_HEADERS = {"X-Atlassian-Token": "no-check"}

def args_read():
    global SRC_FILE
    SRC_FILE = sys.argv[1]

    SERVER = sys.argv[2]
    if not SERVER.endswith('/'):
        SERVER += '/'

    # 1 for custom bitbucket server/datacenter, 2 for cloud
    if 'bitbucket.org' in SERVER:
        SERVER_API_VERSION = 2
    else:
        SERVER_API_VERSION = 1

    global SERVER_API_ENDPOINT
    SERVER_API_ENDPOINT = f"{SERVER}rest/api/{SERVER_API_VERSION}.0/"

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

def read_file(path):
    rows = []
    with open(path) as src:
        inReader = csv.reader(src)


        for row in inReader:
            rows.append(row)

    return rows

def formatTemplate(template, prId=None):
    return template.format(endpoint=SERVER_API_ENDPOINT, projectKey=PROJECT, repositorySlug=REPO, pullRequestId=prId)

def upload_prs(data):
    res = requests.post(formatTemplate(URL_CREATE_PR), auth=AUTH, headers=POST_HEADERS)
    print(res)
    print(res.text)

def upload_pr_comments(data):
    pass

def main():
    init()
    args_read()

    data = read_file(SRC_FILE)
    if len(data) == 0:
        print("Data was empty")
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
