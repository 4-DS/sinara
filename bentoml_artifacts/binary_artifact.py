import os
import re

from bentoml.service import BentoServiceArtifact

class BinaryFileArtifact(BentoServiceArtifact):
    """Abstraction for saving/loading content to/from binary files

    Args:
        name (str): Name of the artifact
        file_extension (:obj:`str`, optional): The file extension used for the saved
            binary file. Defaults to ""

    """

    def __init__(self, name, file_extension=""):
        super(BinaryFileArtifact, self).__init__(name)
        self._file_extension = file_extension
        self._content = None

    def _binary_file_path(self, base_path):
        # base_path is a full file path
        if "." in os.path.basename(base_path):
            return os.path.join(
                base_path
            )
        # when packing or saving a bentoartifact without a file extention, generate a full file path
        # full_file_path = base_path/artifact_name.file_extension
        else:
            return os.path.join(
                base_path,
                re.sub('[^-a-zA-Z0-9_.() ]+', '', self.name) + self._file_extension,
            )

    def load(self, path):
        with open(self._binary_file_path(path), "rb") as f:
            content = f.read()
        return self.pack(content)

    def pack(self, content, metadata=None):  # pylint:disable=arguments-renamed
        self._content = content
        return self

    def get(self):
        return self._content

    def save(self, dst):
        with open(self._binary_file_path(dst), 'wb') as f:
            f.write(self._content)
