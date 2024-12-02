import subprocess
import sys
import requests


class AutoUpdater:
    def __init__(self, pm2_process: str) -> None:
        self.pm2_process = pm2_process
        self.github_api_url = "https://api.github.com/repos/Nickel5-Inc/Nextplace/commits/main"

    def get_latest_commit_sha(self):
        try:
            response = requests.get(self.github_api_url)
            response.raise_for_status()
            latest_commit_sha = response.json().get("sha")
            return latest_commit_sha
        except requests.exceptions.RequestException as e:
            print(f"Error fetching latest commit from GitHub: {e}")
            sys.exit(1)

    def get_local_commit_sha(self):
        try:
            result = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                cwd=".",
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            result.check_returncode()
            return result.stdout.strip()
        except subprocess.CalledProcessError as e:
            print(f"Error fetching local commit SHA: {e.stderr}")
            sys.exit(1)

    def check_github(self):
        latest_sha = self.get_latest_commit_sha()
        local_sha = self.get_local_commit_sha()

        if latest_sha != local_sha:
            print("Newer version detected. Pulling changes...")
            # pull_latest_changes()
            print("Restarting PM2 process...")
            # restart_pm2_process()
        else:
            print("Already up to date.")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: auto_update.py <pm2_process_name>")
    pm2_process_name = sys.argv[1]
    print(f"Running auto-updater with pm2 process name '{pm2_process_name}'")
    auto_updater = AutoUpdater(pm2_process_name)
