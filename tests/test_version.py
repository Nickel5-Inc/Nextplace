import configparser
import os

if __name__ == "__main__":
    print('hello world!')
    config_file_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'setup.cfg')
    config = configparser.ConfigParser()
    config.read(config_file_path)
    version = config.get('metadata', 'version', fallback=None)
    print(f"Found version: {version}")
