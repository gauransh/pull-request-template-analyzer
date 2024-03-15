import sys
import argparse
import logging
from typing import List, Dict, Optional
import requests
import pandas as pd
import mysql.connector
from mysql.connector import Error

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger()


class GitDataCollector:
    """
    This class offers a tool for fetching
    and analyzing GitHub repository,
    focusing on pull requests (PRs).
    It leverages the GitHub API to
    retrieve information about PRs,
    commits, and comments from either
    all repositories within a specified
    organization or a targeted single repo.

    Key functionalities include:
    - GitHub API token validation.
    - Data retrieval for:
        repositories,
        PRs,
        commits,
        and comments,
        with support for API pagination.
    - Aggregation of fetched
    data into a pandas DataFrame
    for analysis or CSV export.

    Command-line arguments enable users
    to specify details such as the GitHub
    organization, API token, and data fetch
    parameters. The resulting dataset
    provides comprehensive PR details
    suitable for analysis or reporting.

    Dependencies:
    - requests (for API requests)
    - pandas (for data aggregation and CSV export)

    Example usage:
    `python github_collector.py --org exampleOrg --token yourToken --url https://api.github.com --max-pages 5 --per-page 50` # pylint: disable=line-too-long
    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        organization: str,
        token: str,
        url: str,
        max_pages: int = None,
        per_page: int = None,
        repo: str = None,
        db_config: str = None,
    ):
        self.organization = organization
        self.headers = {
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github.v3+json",
        }
        self.base_url = url

        if max_pages:
            self.max_pages = max_pages
        else:
            self.max_pages = 100000  # Setting it to 100000 for having all the data

        if per_page and per_page < 100:
            self.per_page = per_page
        else:
            self.per_page = 100  # Setting it to 100 for current use-case

        self.repo = repo
        self.db_config = db_config

    def is_token_valid(self) -> bool:
        """
        Check if the provided GitHub token is valid by making a request to the user endpoint.
        """
        response = requests.get(self.base_url, headers=self.headers, timeout=50)
        if (  # pylint: disable=no-else-return
            response is not None and response.status_code != 200
        ):
            logger.error(  # pylint: disable=logging-fstring-interpolation
                f"API token is not valid"  # pylint: disable=f-string-without-interpolation
            )
            return False
        else:
            return response is not None and response.status_code == 200

    def make_api_request(self, url: str) -> Optional[requests.Response]:
        """Make an API request and return the response."""
        try:
            response = requests.get(url, headers=self.headers, timeout=200)
            response.raise_for_status()
            return response
        except requests.RequestException as api_err:
            logger.error(  # pylint: disable=logging-fstring-interpolation
                f"API request failed: {api_err}"
            )
            return None

    def get_all_paginated_items(self, url: str) -> List[Dict]:
        """Fetch all items from a paginated API endpoint with a limit on the number of pages."""
        items = []
        page_count = 0

        while url and page_count < self.max_pages:
            response = self.make_api_request(url)
            if response:
                items.extend(response.json())
                link_header = response.headers.get("Link")
                if link_header:
                    links = {
                        rel.split("; ")[1][5:-1]: rel.split("; ")[0][1:-1]
                        for rel in link_header.split(", ")
                    }
                    url = links.get("next")
                    page_count += 1
                else:
                    break
            else:
                break

        return items

    def get_repos(self) -> List[Dict]:
        """Fetch all repositories for the given organization."""
        repos_url = (
            f"{self.base_url}/orgs/{self.organization}/repos?per_page={self.per_page}"
        )
        return self.get_all_paginated_items(repos_url)

    def get_pull_requests(self, owner: str, repo_name: str) -> List[Dict]:
        """Fetch all pull requests for a given repository."""
        prs_url = f"{self.base_url}/repos/{owner}/{repo_name}/pulls?state=all&per_page={self.per_page}"  # pylint: disable=line-too-long
        return self.get_all_paginated_items(prs_url)

    def fetch_comments(self, pr_comments_url: str) -> str:
        """Fetch and concatenate comments for a pull request."""
        response = self.make_api_request(pr_comments_url)
        comments_data = (
            response.json() if response and response.status_code == 200 else []
        )
        return " ".join([comment["body"] for comment in comments_data])

    def fetch_commits(self, commits_url: str) -> List[Dict]:
        """Fetch commit data for a pull request."""
        commits_info = []
        response = self.make_api_request(commits_url)
        commits_data = (
            response.json() if response and response.status_code == 200 else []
        )

        for commit in commits_data:
            commit_info = self.create_commit_info(commit["url"])
            if commit_info:
                commits_info.append(commit_info)
        return commits_info

    def create_commit_info(self, commit_url: str) -> Dict:
        """Create detailed commit info, including diffs."""
        commit_response = self.make_api_request(commit_url)
        if (  # pylint: disable=no-else-return
            commit_response and commit_response.status_code == 200
        ):  # pylint: disable=no-else-return
            commit_data = commit_response.json()

            # Extract additions and deletions
            additions = commit_data["stats"]["additions"]
            deletions = commit_data["stats"]["deletions"]

            # Fetch Diff for each file in the commit
            files_diff = {
                file["filename"]: file.get("patch", "No diff available")
                for file in commit_data.get("files", [])
            }

            return {
                "commit_id": commit_data["sha"],
                "additions": additions,
                "deletions": deletions,
                "files_diff": files_diff,
            }
        else:
            return {"error": f"Failed to fetch commit data for {commit_url}"}

    def build_pr_data_row(self, pr: Dict, repo_name: str) -> Dict:
        """Assemble all pieces of data into a dictionary for a PR row."""
        try:
            comments_str = self.fetch_comments(pr["comments_url"])
            commits_info = self.fetch_commits(pr["commits_url"])
            commits_count = len(commits_info) if commits_info else "N/A"

            pr_data = {
                "repo_name": repo_name,
                "pr_id": pr["id"],
                "pr_state": pr["state"],
                "pr_created_at": pr["created_at"],
                "pr_updated_at": pr["updated_at"],
                "pr_merged_at": pr["merged_at"],
                "pr_title": pr["title"],
                "pr_user_login": pr["user"]["login"],
                "pr_diff_url": pr["diff_url"],
                "pr_body": pr["body"],
                "pr_reviwer": pr.get("requested_reviewers", []),
                "pr_comments_count": pr.get("comments", 0),
                "pr_comments": comments_str,
                "pr_commits_count": commits_count,
                "pr_commits_info": commits_info,
            }
            return pr_data
        except Exception as pr_data_err:  # pylint: disable=broad-exception-caught
            logger.error(  # pylint: disable=logging-fstring-interpolation
                f"Error building PR data row: {pr_data_err}"
            )
            return None

    def create_dataframe_with_prs(self) -> pd.DataFrame:
        """Create a DataFrame from the repository metadata and pull requests, for either all repos or a specific one."""  # pylint: disable=line-too-long
        rows = []

        if self.repo:
            # If a specific repo is provided, use it directly
            prs = self.get_pull_requests(self.organization, self.repo)
            for pr in prs:
                pr_data = self.build_pr_data_row(pr, self.repo)
                if pr_data:
                    rows.append(pr_data)
        else:
            # If no specific repo is provided, fetch data for all repos
            repos = self.get_repos()
            for repo in repos:
                prs = self.get_pull_requests(repo["owner"]["login"], repo["name"])
                for pr in prs:
                    pr_data = self.build_pr_data_row(pr, repo["name"])
                    if pr_data:
                        rows.append(pr_data)

        return pd.DataFrame(rows)

    def connect_to_db(self):  # pylint: disable=inconsistent-return-statements
        """Connect to the MySQL database."""
        try:
            connection = mysql.connector.connect(**self.db_config)
            if connection.is_connected():
                return connection
        except Error as e:
            print(f"Error while connecting to MySQL: {e}")
            return None  # inconsistent-return-statements

    def insert_pr_data(self, pr_data):
        """Insert PR data into the MySQL database."""
        query = """
        INSERT INTO pull_requests (repo_name, pr_id, pr_state, pr_created_at, pr_updated_at, pr_merged_at, pr_title, pr_user_login, pr_diff_url, pr_body, pr_comments_count, pr_commits_count)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        try:
            connection = self.connect_to_db()
            if connection:
                cursor = connection.cursor()
                cursor.execute(
                    query,
                    (
                        pr_data["repo_name"],
                        pr_data["pr_id"],
                        pr_data["pr_state"],
                        pr_data["pr_created_at"],
                        pr_data["pr_updated_at"],
                        pr_data["pr_merged_at"],
                        pr_data["pr_title"],
                        pr_data["pr_user_login"],
                        pr_data["pr_diff_url"],
                        pr_data["pr_body"],
                        pr_data["pr_comments_count"],
                        pr_data["pr_commits_count"],
                    ),
                )
                connection.commit()
                cursor.close()
                connection.close()
        except Error as insert_pr_data_err:
            print(f"Error while inserting pr_data to MySQL: {insert_pr_data_err}")

    def create_and_store_pr_data(self):
        """Fetch PR data and store it in the MySQL database."""
        rows = []
        for pr_data in rows:
            self.insert_pr_data(pr_data)


def parse_args():
    """Parses command-line arguments for the GitHub PR Data Fetcher"""

    parser = argparse.ArgumentParser(description="GitHub PR Data Fetcher")
    parser.add_argument("--token", required=True, help="GitHub API Token")
    parser.add_argument(
        "--org",
        required=True,
        help="GitHub Organization, eg: google, facebook, CPSC310-2022W-T2, CPSC310-2023W-T2",
    )
    parser.add_argument(
        "--url",
        required=True,
        help="GitHub Organization URL, eg: https://github.students.cs.ubc.ca/api/v3, https://api.github.com",  # pylint: disable=line-too-long
    )
    parser.add_argument(
        "--max-pages", type=int, help="Maximum number of pages to fetch (optional)"
    )
    parser.add_argument(
        "--per-page", type=int, help="Number of items per page (max 100, optional)"
    )
    parser.add_argument("--repo", help="Specific repository name (optional)")
    return parser.parse_args()


def create_csv():
    """Runs the main program"""
    args = parse_args()
    gh_data = GitDataCollector(
        organization=args.org,
        token=args.token,
        url=args.url,
        max_pages=args.max_pages,
        per_page=args.per_page,
        repo=args.repo,
    )

    if gh_data.is_token_valid() is False:
        logger.error(
            "Please fix the token for the particular org and url"
        )  # pylint: disable=logging-fstring-interpolation
        sys.exit()

    df_prs = gh_data.create_dataframe_with_prs()
    output_file = f"{args.org}_{args.repo or 'all'}_prs_with_details.csv"
    df_prs.to_csv(output_file, index=False)
    logger.info(  # pylint: disable=logging-fstring-interpolation
        f"Pull request details saved to {output_file}."
    )


def main():
    """Runs the main program."""
    args = parse_args()
    db_config = {
        "host": "localhost",
        "user": "yourusername",
        "password": "yourpassword",
        "database": "yourdatabase",
    }
    gh_data = GitDataCollector(
        organization=args.org,
        token=args.token,
        url=args.url,
        max_pages=args.max_pages,
        per_page=args.per_page,
        repo=args.repo,
        db_config=db_config,
    )

    if not gh_data.is_token_valid():
        logger.error(
            "Please fix the token for the particular org and url"
        )  # pylint: disable=logging-fstring-interpolation
        sys.exit()

    # Call the method to fetch PR data and store it in the MySQL database
    gh_data.create_and_store_pr_data()

    logger.info("Pull request details successfully stored in the database.")


if __name__ == "__main__":
    main()
