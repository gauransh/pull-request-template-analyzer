import requests
import pandas as pd
import argparse
import logging
import sys
from typing import List, Dict, Optional

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

class GitDataCollector:
    def __init__(self, organization: str, token: str, url: str, max_pages: int = None, per_page: int = None, repo: str = None):
        self.organization = organization
        self.headers = {
            'Authorization': f'token {token}',
            'Accept': 'application/vnd.github.v3+json',
        }
        self.base_url = url
        
        if max_pages:
            self.max_pages = max_pages
        else:
            self.max_pages = 100000 # Setting it to 100000 for having all the data
  

        if per_page and per_page < 100:
            self.per_page = per_page
        else:
            self.per_page = 100 # Setting it to 100 for current use-case

        self.repo = repo

    def is_token_valid(self) -> bool:
        """
        Check if the provided GitHub token is valid by making a request to the user endpoint.
        """
        response = requests.get(self.base_url, headers=self.headers)
        if response is not None and response.status_code != 200:
            logger.error(f'API token is not valid')
            return False
        else:
            return response is not None and response.status_code == 200

    
    def make_api_request(self, url: str) -> Optional[requests.Response]:
        """Make an API request and return the response."""
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            logger.error(f'API request failed: {e}')
            return None

    def get_all_paginated_items(self, url: str) -> List[Dict]:
        """Fetch all items from a paginated API endpoint with a limit on the number of pages."""
        items = []
        page_count = 0

        while url and page_count < self.max_pages:
            response = self.make_api_request(url)
            if response:
                items.extend(response.json())
                link_header = response.headers.get('Link')
                if link_header:
                    links = {rel.split('; ')[1][5:-1]: rel.split('; ')[0][1:-1] for rel in link_header.split(', ')}
                    url = links.get('next')
                    page_count += 1
                else:
                    break
            else:
                break

        return items

    def get_repos(self) -> List[Dict]:
        """Fetch all repositories for the given organization."""
        repos_url = f'{self.base_url}/orgs/{self.organization}/repos?per_page={self.per_page}'
        return self.get_all_paginated_items(repos_url)

    def get_pull_requests(self, owner: str, repo_name: str) -> List[Dict]:
        """Fetch all pull requests for a given repository."""
        prs_url = f'{self.base_url}/repos/{owner}/{repo_name}/pulls?state=all&per_page={self.per_page}'
        return self.get_all_paginated_items(prs_url)

    def fetch_comments(self, pr_comments_url: str) -> str:
        """Fetch and concatenate comments for a pull request."""
        response = self.make_api_request(pr_comments_url)
        comments_data = response.json() if response and response.status_code == 200 else []
        return ' '.join([comment["body"] for comment in comments_data])

    def fetch_commits(self, commits_url: str) -> List[Dict]:
        """Fetch commit data for a pull request."""
        commits_info = []
        response = self.make_api_request(commits_url)
        commits_data = response.json() if response and response.status_code == 200 else []

        for commit in commits_data:
            commit_info = self.create_commit_info(commit["url"])
            if commit_info:
                commits_info.append(commit_info)
        return commits_info

    def create_commit_info(self, commit_url: str) -> Dict:
        """Create detailed commit info, including diffs."""
        commit_response = self.make_api_request(commit_url)
        if commit_response and commit_response.status_code == 200:
            commit_data = commit_response.json()

            # Extract additions and deletions
            additions = commit_data["stats"]["additions"]
            deletions = commit_data["stats"]["deletions"]

            # Fetch Diff for each file in the commit
            files_diff = {file["filename"]: file.get("patch", "No diff available") for file in commit_data.get("files", [])}

            return {
                "commit_id": commit_data["sha"],
                "additions": additions,
                "deletions": deletions,
                "files_diff": files_diff
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
                'repo_name': repo_name,
                'pr_id': pr['id'],
                'pr_state': pr['state'],
                'pr_created_at': pr['created_at'],
                'pr_updated_at': pr['updated_at'],
                'pr_merged_at': pr['merged_at'],
                'pr_title': pr['title'],
                'pr_user_login': pr['user']['login'],
                'pr_diff_url': pr['diff_url'],
                'pr_body': pr['body'],
                'pr_reviwer': pr.get('requested_reviewers', []),
                'pr_comments_count': pr.get("comments", 0),
                'pr_comments': comments_str,
                'pr_commits_count': commits_count,
                'pr_commits_info': commits_info
            }
            return pr_data
        except Exception as e:
            logger.error(f"Error building PR data row: {e}")
            return None

    def create_dataframe_with_prs(self) -> pd.DataFrame:
        """Create a DataFrame from the repository metadata and pull requests, for either all repos or a specific one."""
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
                prs = self.get_pull_requests(repo['owner']['login'], repo['name'])
                for pr in prs:
                    pr_data = self.build_pr_data_row(pr, repo['name'])
                    if pr_data:
                        rows.append(pr_data)

        return pd.DataFrame(rows)

def parse_args():
    parser = argparse.ArgumentParser(description='GitHub PR Data Fetcher')
    parser.add_argument('--token', required=True, help='GitHub API Token')
    parser.add_argument('--org', required=True, help='GitHub Organization, eg: google, facebook, CPSC310-2022W-T2, CPSC310-2023W-T2')
    parser.add_argument('--url', required=True, help= 'GitHub Organization URL, eg: https://github.students.cs.ubc.ca/api/v3, https://api.github.com')
    parser.add_argument('--max-pages', type=int, help='Maximum number of pages to fetch (optional)')
    parser.add_argument('--per-page', type=int, help='Number of items per page (max 100, optional)')
    parser.add_argument('--repo', help='Specific repository name (optional)')
    return parser.parse_args()

def main():
    args = parse_args()
    gh_data = GitDataCollector(organization=args.org, token=args.token, url=args.url, 
                       max_pages=args.max_pages, per_page=args.per_page, repo=args.repo)
    
    if gh_data.is_token_valid() is False:
        logger.error('Please fix the token for the particular org and url')
        sys.exit()

    df_prs = gh_data.create_dataframe_with_prs()
    output_file = f"{args.org}_{args.repo or 'all'}_prs_with_details.csv"
    df_prs.to_csv(output_file, index=False)
    logger.info(f'Pull request details saved to {output_file}.')
    
    #Example usage: python3 github_collector.py --org CPSC310-2022W-T2 --url https://github.students.cs.ubc.ca/api/v3 --token {YOUR_TOKEN} --max-pages 2 --per-page 2

if __name__ == '__main__':
    main()