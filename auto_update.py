import subprocess
import sys
from time import sleep
import requests


class AutoUpdater:

    def __init__(self, pm2_process: str) -> None:
        self.pm2_process = pm2_process
        self.github_api_url = "https://api.github.com/repos/Nickel5-Inc/Nextplace/commits/main"

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
                cwd=".",
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            result.check_returncode()  # check the return code
            return result.stdout.strip()  # parse output
        except subprocess.CalledProcessError as e:
            print(f"Error fetching local commit SHA: {e.stderr}")
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
            # pull_latest_changes()
            print("Restarting PM2 process...")
            # restart_pm2_process()
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
