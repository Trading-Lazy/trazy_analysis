import io
import os
import random
from pathlib import Path
from typing import List, Optional

import numpy as np
import requests
from Crypto.Cipher import AES
from Crypto.Util import Counter
from mega import Mega
from mega.crypto import (
    a32_to_base64,
    a32_to_str,
    base64_url_decode,
    base64_url_encode,
    decrypt_attr,
    encrypt_attr,
    encrypt_key,
    get_chunks,
    makebyte,
    str_to_a32,
)
from mega.errors import RequestError

import trazy_analysis.settings
from trazy_analysis.common.constants import ENCODING
from trazy_analysis.common.decorators import try_until_success
from trazy_analysis.file_storage.common import PATH_SEPARATOR
from trazy_analysis.file_storage.file_storage import FileStorage
from trazy_analysis.logger import logger
from trazy_analysis.settings import MEGA_API_EMAIL, MEGA_API_PASSWORD

LOG = trazy_analysis.logger.get_root_logger(
    __name__, filename=os.path.join(trazy_analysis.settings.ROOT_PATH, "output.log")
)
NODE_ID_KEY = "h"
NODE_PARENT_KEY = "p"
NODE_ATTRIBUTES_KEY = "a"
NODE_NAME_ATTRIBUTE_KEY = "n"


class MegaExtended(Mega):
    def __init__(self, options: None = None) -> None:
        super().__init__(options)
        self.path_cache = {}
        self.id_cache = {}

    @try_until_success
    def login(self, email: Optional[str] = None, password: Optional[str] = None):
        return super().login(email, password)

    @try_until_success
    def get_files(
        self,
    ) -> dict:
        return super().get_files()

    @try_until_success
    def get_files_in_node(self, target: str) -> dict:
        return super().get_files_in_node(target)

    @try_until_success
    def create_folder_helper(self, name: str, dest: Optional[str] = None) -> dict:
        return super().create_folder(name, dest)

    def create_folder(self, name: str, dest: Optional[str] = None) -> dict:
        data = self.create_folder_helper(name, dest)
        folder_name = name.split(PATH_SEPARATOR)[-1]
        path = name if dest is None else Path(dest) / name
        path_str = str(path)
        folder_id = data[folder_name]
        folder = self.find(handle=folder_id)
        self.path_cache[path_str] = folder
        self.id_cache[folder_id] = path_str
        return data

    def find(
        self,
        filename: Optional[str] = None,
        handle: Optional[str] = None,
        exclude_deleted: bool = True,
    ) -> dict:
        """
        Return file object from given filename
        """
        if filename in self.path_cache:
            return self.path_cache[filename]

        files = self.get_files()
        if handle:
            return files[handle]
        path = Path(filename)
        filename = path.name
        parent_path = path.parent
        parent_dir_name = str(parent_path) if parent_path.name else ""
        LOG.info("Searching file %s ...", filename)
        for file in list(files.items()):
            parent_node_id = None
            if parent_dir_name:
                parent_node_id = self.find_path_descriptor(parent_dir_name, files=files)
                if (
                    filename
                    and parent_node_id
                    and type(file[1]) is dict
                    and NODE_ATTRIBUTES_KEY in file[1]
                    and type(file[1][NODE_ATTRIBUTES_KEY]) is dict
                    and NODE_NAME_ATTRIBUTE_KEY in file[1][NODE_ATTRIBUTES_KEY]
                    and file[1][NODE_ATTRIBUTES_KEY][NODE_NAME_ATTRIBUTE_KEY]
                    == filename
                    and parent_node_id == file[1][NODE_PARENT_KEY]
                ):
                    if (
                        exclude_deleted
                        and self._trash_folder_node_id == file[1][NODE_PARENT_KEY]
                    ):
                        continue
                    self.path_cache[filename] = file
                    self.id_cache[file[0]] = filename
                    return file[1]
            elif (
                filename
                and type(file[1]) is dict
                and NODE_ATTRIBUTES_KEY in file[1]
                and type(file[1][NODE_ATTRIBUTES_KEY]) is dict
                and NODE_NAME_ATTRIBUTE_KEY in file[1][NODE_ATTRIBUTES_KEY]
                and file[1][NODE_ATTRIBUTES_KEY][NODE_NAME_ATTRIBUTE_KEY] == filename
            ):
                self.path_cache[filename] = file
                self.id_cache[file[0]] = filename
                return file[1]

    @try_until_success
    def upload_helper(
        self, filename, content, dest=None, dest_filename=None, encoding=ENCODING
    ):  # pragma: no cover
        # determine file_storage node
        if dest is None:
            # if none set, upload to cloud drive node
            if not hasattr(self, "root_id"):
                self.get_files()
            dest = self.root_id

        # request upload url, call 'u' method
        with io.BytesIO(bytes(content, encoding)) as bio:
            file_size = bio.getbuffer().nbytes
            ul_url = self._api_request({"a": "u", "s": file_size})["p"]

            # generate random aes key (128) for file
            ul_key = [random.randint(0, 0xFFFFFFFF) for _ in range(6)]
            k_str = a32_to_str(ul_key[:4])
            count = Counter.new(
                128, initial_value=((ul_key[4] << 32) + ul_key[5]) << 64
            )
            aes = AES.new(k_str, AES.MODE_CTR, counter=count)

            upload_progress = 0
            completion_file_handle = None

            mac_str = "\0" * 16
            mac_encryptor = AES.new(k_str, AES.MODE_CBC, mac_str.encode(encoding))
            iv_str = a32_to_str([ul_key[4], ul_key[5], ul_key[4], ul_key[5]])
            if file_size > 0:
                for chunk_start, chunk_size in get_chunks(file_size):
                    chunk = bio.read(chunk_size)
                    upload_progress += len(chunk)

                    encryptor = AES.new(k_str, AES.MODE_CBC, iv_str)
                    for i in range(0, len(chunk) - 16, 16):
                        block = chunk[i : i + 16]
                        encryptor.encrypt(block)

                    # fix for files under 16 bytes failing
                    if file_size > 16:
                        i += 16
                    else:
                        i = 0

                    block = chunk[i : i + 16]
                    if len(block) % 16:
                        block += makebyte("\0" * (16 - len(block) % 16))
                    mac_str = mac_encryptor.encrypt(encryptor.encrypt(block))

                    # encrypt file and upload
                    chunk = aes.encrypt(chunk)
                    output_file = requests.post(
                        ul_url + PATH_SEPARATOR + str(chunk_start),
                        data=chunk,
                        timeout=self.timeout,
                    )
                    completion_file_handle = output_file.text
                    LOG.info("%s of %s uploaded", upload_progress, file_size)
            else:
                output_file = requests.post(
                    ul_url + "/0", data="", timeout=self.timeout
                )
                completion_file_handle = output_file.text

            LOG.info("Chunks uploaded")
            LOG.info("Setting attributes to complete upload")
            LOG.info("Computing attributes")
            file_mac = str_to_a32(mac_str)

            # determine meta mac
            meta_mac = (file_mac[0] ^ file_mac[1], file_mac[2] ^ file_mac[3])

            dest_filename = dest_filename or os.path.basename(filename)
            attribs = {NODE_NAME_ATTRIBUTE_KEY: dest_filename}

            encrypt_attribs = base64_url_encode(encrypt_attr(attribs, ul_key[:4]))
            key = [
                ul_key[0] ^ ul_key[4],
                ul_key[1] ^ ul_key[5],
                ul_key[2] ^ meta_mac[0],
                ul_key[3] ^ meta_mac[1],
                ul_key[4],
                ul_key[5],
                meta_mac[0],
                meta_mac[1],
            ]
            encrypted_key = a32_to_base64(encrypt_key(key, self.master_key))
            LOG.info("Sending request to update attributes")
            # update attributes
            data = self._api_request(
                {
                    "a": "p",
                    "t": dest,
                    "i": self.request_id,
                    "n": [
                        {
                            "h": completion_file_handle,
                            "t": 0,
                            "a": encrypt_attribs,
                            "k": encrypted_key,
                        }
                    ],
                }
            )
            LOG.info("Upload complete")
            return data

    def upload(self, filename: str, content: str, dest: str, encoding: str = ENCODING):
        data = self.upload_helper(filename, content, dest, None, encoding)
        file_id = data["f"][0]["h"]
        file = self.find(handle=file_id)
        self.path_cache[filename] = file
        self.id_cache[file_id] = filename
        return data

    @try_until_success
    def destroy_helper(self, file_id: str) -> dict:
        return super().destroy(file_id)

    def destroy(self, file_id: str) -> int:
        data = self.destroy_helper(file_id)
        filename = self.id_cache[file_id]
        del self.id_cache[file_id]
        del self.path_cache[filename]
        return data

    @try_until_success
    def get_file_content_helper(self, filename: str) -> str:  # pragma: no cover
        if filename in self.path_cache:
            file = self.path_cache[filename]
        else:
            file = self.find(filename)

            if file is None:
                LOG.error("File %s not found for getting content", filename)
                return ""
            self.path_cache[filename] = file

        file_id = file["h"]
        self.id_cache[file_id] = filename
        file_data = self._api_request({"a": "g", "g": 1, "n": file_id})
        k = file["k"]
        iv = file["iv"]
        meta_mac = file["meta_mac"]

        # Seems to happens sometime... When this occurs, files are
        # inaccessible also in the official also in the official web app.
        # Strangely, files can come back later.
        if "g" not in file_data:
            raise RequestError("File not accessible anymore")
        file_url = file_data["g"]
        file_size = file_data["s"]
        attribs = base64_url_decode(file_data["at"])
        attribs = decrypt_attr(attribs, k)

        input_file = requests.get(file_url, stream=True).raw

        with io.BytesIO() as bio:
            k_str = a32_to_str(k)
            counter = Counter.new(128, initial_value=((iv[0] << 32) + iv[1]) << 64)
            aes = AES.new(k_str, AES.MODE_CTR, counter=counter)

            mac_str = "\0" * 16
            mac_encryptor = AES.new(k_str, AES.MODE_CBC, mac_str.encode("utf8"))
            iv_str = a32_to_str([iv[0], iv[1], iv[0], iv[1]])

            for chunk_start, chunk_size in get_chunks(file_size):
                chunk = input_file.read(chunk_size)
                chunk = aes.decrypt(chunk)
                bio.write(chunk)

                encryptor = AES.new(k_str, AES.MODE_CBC, iv_str)
                for i in range(0, len(chunk) - 16, 16):
                    block = chunk[i : i + 16]
                    encryptor.encrypt(block)

                # fix for files under 16 bytes failing
                if file_size > 16:
                    i += 16
                else:
                    i = 0

                block = chunk[i : i + 16]
                if len(block) % 16:
                    block += b"\0" * (16 - (len(block) % 16))
                mac_str = mac_encryptor.encrypt(encryptor.encrypt(block))
            file_mac = str_to_a32(mac_str)
            # check mac integrity
            if (file_mac[0] ^ file_mac[1], file_mac[2] ^ file_mac[3]) != meta_mac:
                raise ValueError("Mismatched mac")
            bio.seek(0)
            return bio.read().decode("UTF-8")

    def get_file_content(self, filename: str) -> str:
        return self.get_file_content_helper(filename)


class MegaNzFileStorage(FileStorage):
    def __init__(self) -> None:
        # user details
        self.mega = MegaExtended()
        self.mega.login(MEGA_API_EMAIL, MEGA_API_PASSWORD)

    def get_id_from_file(self, file: dict) -> str:
        return file[NODE_ID_KEY]

    def exists(self, path: str) -> bool:
        file = self.mega.find(path, exclude_deleted=True)
        return file is not None

    def ls(self, path: str) -> np.array:  # [str]
        dir_infos: dict = self.mega.find(path, exclude_deleted=True)
        if dir_infos is None:
            return np.array([], dtype="U256")
        dir_id: str = self.get_id_from_file(dir_infos)
        subfolders_infos: dict = self.mega.get_files_in_node(dir_id)
        subfolders_list: np.array = np.empty(shape=len(subfolders_infos), dtype="U256")
        index = 0
        for subfolder_id, subfolder_infos in subfolders_infos.items():
            LOG.info(
                "subfolder info = %s",
                subfolder_infos[NODE_ATTRIBUTES_KEY][NODE_NAME_ATTRIBUTE_KEY],
            )
            subfolders_list[index] = subfolder_infos[NODE_ATTRIBUTES_KEY][
                NODE_NAME_ATTRIBUTE_KEY
            ]
            LOG.info("subfolder list = %s", subfolders_list[index])
            index += 1
        return subfolders_list

    def mkdir(self, path: str) -> None:
        if self.exists(path):
            return
        LOG.info("Creating directory for the path: %s", path)
        dirs_in_path: List[str] = list(filter(None, path.split(PATH_SEPARATOR)))
        current_path: str = ""
        idx: int = 0
        for dir in dirs_in_path:
            extended_current_path = current_path + dir
            if self.exists(extended_current_path):
                current_path = extended_current_path + PATH_SEPARATOR
            else:
                break
            idx += 1
        LOG.info("%s already exists in the path", current_path)
        dest = None
        remaining_path = path
        if current_path != "" and idx < len(dirs_in_path):
            remaining_path = PATH_SEPARATOR.join(dirs_in_path[idx:])
            dest_infos = self.mega.find(current_path, exclude_deleted=True)
            dest = self.get_id_from_file(dest_infos)

        self.mega.create_folder(remaining_path, dest)

    def write(self, file_path: str, content: str) -> None:
        if self.exists(file_path):
            file_infos = self.mega.find(file_path, exclude_deleted=True)
            file_id = self.get_id_from_file(file_infos)
            self.mega.destroy(file_id)
        path = Path(file_path)
        parent_file_path = str(path.parent)
        dest_infos = self.mega.find(parent_file_path, exclude_deleted=True)
        dest = self.get_id_from_file(dest_infos)
        self.mega.upload(file_path, content, dest)

    def get_file_content(self, path: str) -> str:
        return self.mega.get_file_content(path)
