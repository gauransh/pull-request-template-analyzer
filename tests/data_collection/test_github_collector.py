import pytest
import requests_mock
from src.data_collection.github_collector import GitDataCollector
from mysql.connector.errors import Error
from unittest.mock import Mock
import pytest 

@pytest.fixture
def git_data_collector():
    """Fixture to create a GitDataCollector instance for testing."""
    return GitDataCollector(
        organization="testOrg",
        token="testToken",
        url="https://api.github.com",
        max_pages=2,
        per_page=5,
        db_config={
            "host": "localhost",
            "database": "testdb",
            "user": "testuser",
            "password": "testpass",
        },
    )


def test_is_token_valid(git_data_collector):
    """Test token validation."""
    with requests_mock.Mocker() as m:
        m.get("https://api.github.com", status_code=200)
        assert git_data_collector.is_token_valid() is True

        m.get("https://api.github.com", status_code=401)
        assert git_data_collector.is_token_valid() is False


def test_make_api_request_success(git_data_collector):
    """Test successful API request."""
    with requests_mock.Mocker() as m:
        m.get("https://api.github.com/success", json={"success": True}, status_code=200)
        response = git_data_collector.make_api_request("https://api.github.com/success")
        assert response.json() == {"success": True}


def test_get_all_paginated_items(git_data_collector):
    """Test pagination handling."""
    with requests_mock.Mocker() as m:
        m.get(
            "https://api.github.com/items?page=1",
            json=[{"item": "data1"}],
            headers={"Link": '<https://api.github.com/items?page=2>; rel="next"'},
        )
        m.get("https://api.github.com/items?page=2", json=[{"item": "data2"}])

        items = git_data_collector.get_all_paginated_items(
            "https://api.github.com/items?page=1"
        )
        assert len(items) == 2
        assert items[0]["item"] == "data1"
        assert items[1]["item"] == "data2"


def test_create_dataframe_with_prs(git_data_collector):
    """Test DataFrame creation from PR data."""
    with requests_mock.Mocker() as m:
        # Correct the mocking to include both repositories and their PRs
        m.get(
            "https://api.github.com/orgs/testOrg/repos?per_page=5",
            json=[
                {
                    "name": "repo1",
                    "owner": {"login": "testOwner"},
                }
            ],
        )
        m.get(
            "https://api.github.com/repos/testOwner/repo1/pulls?state=all&per_page=5",
            json=[
                {
                    "id": "pr1",
                    "state": "open",
                    "title": "Test PR",
                    "user": {"login": "user1"},
                    "created_at": "2020-01-01T00:00:00Z",
                    "updated_at": "2020-01-02T00:00:00Z",
                    "merged_at": None,
                    "diff_url": "https://api.github.com/repos/testOwner/repo1/pulls/1.diff",
                    "body": "Test body",
                    "comments_url": "https://api.github.com/repos/testOwner/repo1/issues/1/comments",
                    "commits_url": "https://api.github.com/repos/testOwner/repo1/pulls/1/commits",
                }
            ],
        )

        m.get("https://api.github.com/repos/testOwner/repo1/issues/1/comments", json=[])
        m.get("https://api.github.com/repos/testOwner/repo1/pulls/1/commits", json=[])

        df_prs = git_data_collector.create_dataframe_with_prs()

        assert not df_prs.empty
        assert len(df_prs) == 1
        assert df_prs.iloc[0]["repo_name"] == "repo1"
        assert df_prs.iloc[0]["pr_id"] == "pr1"


@pytest.fixture
def mock_db_connection(mocker):
    mock_connection = mocker.MagicMock()
    mock_cursor = mocker.MagicMock()
    mock_connection.cursor.return_value = mock_cursor
    mocker.patch("mysql.connector.connect", return_value=mock_connection)
    return mock_connection, mock_cursor


def test_connect_to_db_success(git_data_collector, mock_db_connection):
    connection, _ = mock_db_connection
    assert git_data_collector.connect_to_db() == connection


def test_connect_to_db_failure(mocker, git_data_collector):
    mocker.patch("mysql.connector.connect", side_effect=Error("Connection failed"))
    assert git_data_collector.connect_to_db() is None


def test_insert_pr_data_success(git_data_collector, mock_db_connection):
    _, mock_cursor = mock_db_connection
    pr_data = {
        "repo_name": "repo1",
        "pr_id": 1,
        "pr_state": "closed",
        "pr_created_at": "2020-01-01T00:00:00Z",
        "pr_updated_at": "2020-01-02T00:00:00Z",
        "pr_merged_at": None,
        "pr_title": "Test PR",
        "pr_user_login": "user1",
        "pr_diff_url": "https://api.github.com/repos/testOwner/repo1/pulls/1.diff",
        "pr_body": "Test body",
        "pr_reviwer": "reviewer1",
        "pr_comments_count": 1,
        "pr_comments": "",
        "pr_commits_count": 1,
        "pr_commits_info": "[{commits_info}]",
    }
    git_data_collector.insert_pr_data(pr_data)
    mock_cursor.execute.assert_called()  # You can add more specific checks here


def test_insert_pr_data_failure(git_data_collector, mocker):
    mocker.patch("mysql.connector.connect", side_effect=Error("Connection failed"))
    pr_data = {
        "repo_name": "repo1",
        "pr_id": 1,
        "pr_state": "closed",
        "pr_created_at": "2020-01-01T00:00:00Z",
        "pr_updated_at": "2020-01-02T00:00:00Z",
        "pr_merged_at": None,
        "pr_title": "Test PR",
        "pr_user_login": "user1",
        "pr_diff_url": "https://api.github.com/repos/testOwner/repo1/pulls/1.diff",
        "pr_body": "Test body",
        "pr_reviwer": "reviewer1",
        "pr_comments_count": 1,
        "pr_comments": "",
        "pr_commits_count": 1,
        "pr_commits_info": "[{commits_info}]",
    }
    git_data_collector.insert_pr_data(pr_data)
