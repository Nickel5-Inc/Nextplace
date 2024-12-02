import subprocess
import sys
from time import sleep
import requests

REPO_PATH = "."
BRANCH = "main"


class AutoUpdater:

    def __init__(self, pm2_process: str) -> None:
        self.pm2_process = pm2_process  # store pm2 process name
        self.github_api_url = f"https://api.github.com/repos/Nickel5-Inc/Nextplace/commits/{BRANCH}"  # store remote repo name

    def get_latest_commit_sha(self) -> str:
        """
        Get the latest commit hash from github for the Nextplace repo
        Returns:
            None
        """
        try:
            response = requests.get(self.github_api_url)  # get from GitHub api
            response.raise_for_status()  # check response status
            latest_commit_sha = response.json().get("sha")  # parse the SHA
            return latest_commit_sha
        except requests.exceptions.RequestException as e:  # handle error
            print(f"Error fetching latest commit from GitHub: {e}")
            sys.exit(1)

    def get_local_commit_sha(self) -> str:
        """
        Get the commit hash for the local repository
        Returns:
            None
        """
        try:
            # get the local git repo info
            result = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                cwd=REPO_PATH,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            result.check_returncode()  # check the return code
            return result.stdout.strip()  # parse output
        except subprocess.CalledProcessError as e:
            print(f"Error fetching local commit SHA: {e.stderr}")
            sys.exit(1)

    def pull_latest_changes(self) -> None:
        """
        Pull the latest version of the main branch from GitHub
        Returns:
            None
        """
        try:
            # run subprocess to pull latest version of main
            result = subprocess.run(
                ["git", "pull", "origin", BRANCH],
                cwd=REPO_PATH,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            result.check_returncode()  # check return code
            print(f"Pulled latest changes: {result.stdout}")
        except subprocess.CalledProcessError as e:  # handle error
            print(f"Error pulling latest changes: {e.stderr}")
            sys.exit(1)

    def restart_pm2_process(self) -> None:
        """
        Restart the PM2 process running the validator
        Returns:
            None
        """
        try:
            # run subprocess to restart pm2
            result = subprocess.run(
                ["pm2", "restart", self.pm2_process],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            result.check_returncode()  # check return code
            print(f"PM2 process restarted: {result.stdout}")
        except subprocess.CalledProcessError as e:  # handle error
            print(f"Error restarting PM2 process: {e.stderr}")
            sys.exit(1)

    def check_github(self) -> None:
        """
        Check if we need to update. If we do, update the local repository.
        Returns:
            None
        """
        latest_sha = self.get_latest_commit_sha()  # get latest commit hash for remote repo
        local_sha = self.get_local_commit_sha()  # get local commit hash

        if latest_sha != local_sha:  # we need to update
            print("Newer version detected. Pulling changes...")
            self.pull_latest_changes()
            print("Restarting PM2 process...")
            self.restart_pm2_process()
        else:  # we don't need to update
            print("Already up to date.")

    def run(self) -> None:
        """
        Run the auto-updater in an infinite loop, sleeping for an hour after each iteration
        Returns:
            None
        """
        while True:  # loop indefinitely
            self.check_github()  # check if we need to update
            sleep(3600)  # sleep for 1 hour


if __name__ == "__main__":
    # check usage
    if len(sys.argv) != 2:
        print(f"Usage: python3 auto_update.py <pm2_process_name>")
        sys.exit(1)

    pm2_process_name = sys.argv[1]  # extract args
    print(f"Running auto-updater with pm2 process name '{pm2_process_name}'")
    auto_updater = AutoUpdater(pm2_process_name)  # instantiate
    auto_updater.run()  # run
