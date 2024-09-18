import bittensor as bt
from requests.exceptions import HTTPError
from typing import Literal
from typing import TypedDict, Optional
import sys
import importlib.util
from huggingface_hub import hf_hub_download
import os

'''
Container class for model arguments. Used to define and enforce data types.
'''


class ModelArgs(TypedDict):
    model_source: Literal["hugging_face", "local"]
    model_path: str
    api_key: Optional[str]
    model_class_filename: str


'''
Helper class for Model
Loads & stores the model & tokenizer
'''


class ModelLoader:

    def __init__(self, model_args: ModelArgs):

        # Useful print statements for the user
        hf_model_access = 'public' if model_args['api_key'] == '' else 'private'
        if model_args['model_source'] == 'local':
            bt.logging.info(f"🛤️ Using local model with path: '{model_args['model_path']}'")
        else:
            bt.logging.info(f"🛤️ Using {hf_model_access} Hugging Face model with path: '{model_args['model_path']}'")

        self.model_args = model_args  # Store the model args

    def load_model(self):
        """
        Load a machine learning model.

        Returns:
            The Model driver class

        Throws:
            Error if we can't find the model to load.
        """

        # Extract args
        model_source = self.model_args['model_source']
        model_path = self.model_args['model_path']
        model_class_filename = self.model_args['model_class_filename']
        api_key = self.model_args['api_key']

        if model_path == '':  # Use public base model.
            bt.logging.info(f"🚀 Using base model.")
            return self._load_hugging_face_model('Nickel5HF/NextPlace', 'StatisticalBaseModel.py', '')

        if model_source == 'hugging_face':  # Load a Hugging Face Python class
            return self._load_hugging_face_model(model_path, model_class_filename, api_key)
        else:  # Load a Python class from the local filesystem
            return self._load_local_model(model_path, model_class_filename)

    def _load_hugging_face_model(self, model_path: str, filename: str, api_key: str):
        """
       Loads a model from the Hugging Face API

       Returns:
           A reference to a model instance.
       """
        try:
            if api_key == '':  # try to load a public model
                bt.logging.info(f"🚀 Loading a public Hugging Face model. No API key was given.")
                driver_class_file = hf_hub_download(repo_id=model_path,
                                                    filename=filename)  # Download the driver class, or get reference to it in cache
            else:  # try to load a private model
                bt.logging.info(f"🚀 Loading a private Hugging Face model.")
                driver_class_file = hf_hub_download(repo_id=model_path, filename=filename, token=self.model_args[
                    'api_key'])  # Download the driver class, or get reference to it in cache
            return self._import_class(driver_class_file,
                                      filename)  # Extract the class, add it to python environment, return instance
        except OSError as e:
            bt.logging.error(f"❗OSError: Failed to load Hugging Face model from '{model_path}/{filename}'. Error: {e}")
            sys.exit(1)
        except ValueError as e:
            bt.logging.error(
                f"❗ValueError: Failed to load Hugging Face model from '{model_path}/{filename}'. Error: {e}")
            sys.exit(1)
        except HTTPError as e:
            bt.logging.error(f"❗HTTPError: Failed to load Hugging Face model from '{model_path}/{filename}'. Error: {e}")
            sys.exit(1)

    def _load_local_model(self, model_path: str, model_class_filename: str) -> None:
        """
        Loads a local model.

        Returns:
           An object reference
        """
        if model_path[-1] != "/": model_path += "/"  # Append '/' to end of model path
        if model_path[0] != "/": model_path = "/" + model_path  # Prepend '/' to beginning of model path
        current_directory = os.getcwd()  # Get cwd
        entire_path = current_directory + model_path + model_class_filename  # Build complete path name
        if not os.path.isfile(entire_path):
            bt.logging.error(f"❗Failed to find file '{entire_path}'")
            sys.exit(1)
        return self._import_class(entire_path, model_class_filename)

    def _import_class(self, driver_class_file, model_class_filename):
        """
        Loads a Python class into the Python environment, instantiates the class, checks if it has the required `run_inference` method

        Args:
            driver_class_file: reference to a downloaded or cached file

        Returns:
            An object reference
        """
        class_name = model_class_filename.split('.py')[0]  # Derive the Python class name from the filename

        try:
            spec = importlib.util.spec_from_file_location(class_name, driver_class_file)  # Create a `spec` reference
            if spec is None:
                raise ImportError(f"❗Cannot find the module spec for {class_name} at {driver_class_file}")

            module = importlib.util.module_from_spec(spec)  # Build a module from the spec
            sys.modules[class_name] = module  # Add the module to the Python environment
            spec.loader.exec_module(module)  # Load the module

        except FileNotFoundError as e:
            bt.logging.error(f"❗File not found: {e.filename}")
            sys.exit(1)
        except ImportError as e:
            bt.logging.error(f"❗Import error: {e}")
            sys.exit(1)
        except Exception as e:
            bt.logging.error(f"❗An unexpected error occurred: {e}")
            sys.exit(1)

        model_class = getattr(module, class_name)  # Create the class from the module
        model_instance = model_class()  # Instantiate the class
        if not hasattr(model_instance, 'run_inference'):  # Check if the instance has a method called `run_inference`
            bt.logging.error(f"❗The class {class_name} does not have a method called 'run_inference'. Terminating...")
            sys.exit(1)  # Exit program if method `run_inference` is not defined in this class

        return model_instance  # Return the object
