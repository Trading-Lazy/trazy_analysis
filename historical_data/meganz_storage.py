import io
import logging
import os
import random
import requests
from Crypto.Cipher import AES
from Crypto.Util import Counter
from mega import Mega
from mega.crypto import (
    a32_to_base64,
    encrypt_key,
    base64_url_encode,
    encrypt_attr,
    a32_to_str,
    get_chunks,
    str_to_a32,
    makebyte,
)
from pathlib import Path
from historical_data.storage import Storage
from typing import List

from historical_data.common import ENCODING, LOG
from settings import MEGA_API_EMAIL, MEGA_API_PASSWORD

NODE_PARENT_KEY = "p"
NODE_ATTRIBUTES_KEY = "a"
NODE_NAME_ATTRIBUTE_KEY = "n"
PATH_SEPARATOR = "/"


class MegaExtended(Mega):
    def find(self, filename=None, handle=None, exclude_deleted=False):
        """
        Return file object from given filename
        """
        files = super().get_files()
        if handle:
            return files[handle]
        path = Path(filename)
        filename = path.name
        parent_path = path.parent
        parent_dir_name = str(parent_path) if parent_path.name else ""
        LOG.info("Searching file {} ...".format(filename))
        LOG.info("File parent name = {}".format(parent_dir_name))
        for file in list(files.items()):
            parent_node_id = None
            if parent_dir_name:
                parent_node_id = self.find_path_descriptor(parent_dir_name, files=files)
                if (
                    filename
                    and parent_node_id
                    and file[1][NODE_ATTRIBUTES_KEY]
                    and file[1][NODE_ATTRIBUTES_KEY][NODE_NAME_ATTRIBUTE_KEY]
                    == filename
                    and parent_node_id == file[1][NODE_PARENT_KEY]
                ):
                    if (
                        exclude_deleted
                        and self._trash_folder_node_id == file[1][NODE_PARENT_KEY]
                    ):
                        continue
                    return file
            elif (
                filename
                and file[1][NODE_ATTRIBUTES_KEY]
                and file[1][NODE_ATTRIBUTES_KEY][NODE_NAME_ATTRIBUTE_KEY] == filename
            ):
                if (
                    exclude_deleted
                    and self._trash_folder_node_id == file[1][NODE_PARENT_KEY]
                ):
                    continue
                return file

    def upload(
        self, filename, content, dest=None, dest_filename=None, encoding=ENCODING
    ):
        # determine storage node
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


class MegaNzStorage(Storage):
    def __init__(self):
        # user details
        self.mega = MegaExtended()
        self.mega.login(MEGA_API_EMAIL, MEGA_API_PASSWORD)
        self.logger = logging.getLogger("HistoricalDataPipelineLogger")

    def get_id_from_info(self, infos: tuple):
        return infos[0]

    def exists(self, path: str) -> bool:
        file = self.mega.find(path, exclude_deleted=True)
        return file is not None

    def ls(self, path: str) -> List[str]:
        dir_infos: tuple = self.mega.find(path, exclude_deleted=True)
        if dir_infos is None:
            return []
        dir_id: str = self.get_id_from_info(dir_infos)
        subfolders_infos: dict = self.mega.get_files_in_node(dir_id)
        subfolders_list: List[str] = []
        for subfolder_id, subfolder_infos in subfolders_infos.items():
            subfolders_list.append(
                subfolder_infos[NODE_ATTRIBUTES_KEY][NODE_NAME_ATTRIBUTE_KEY]
            )
        return subfolders_list

    def mkdir(self, path: str) -> None:
        if self.exists(path):
            return
        LOG.info("Creating directory for the path: {}".format(path))
        dirs_in_path: List[str] = list(filter(None, path.split(PATH_SEPARATOR)))
        current_path: str = ""
        idx: int = 0
        for dir in dirs_in_path:
            extended_current_path = current_path + dir
            if self.exists(extended_current_path):
                LOG.info("{} already exists in the path".format(extended_current_path))
                current_path = extended_current_path + PATH_SEPARATOR
            else:
                break
            idx += 1
        dest = None
        remaining_path = path
        if current_path != "" and idx < len(dirs_in_path):
            remaining_path = PATH_SEPARATOR.join(dirs_in_path[idx:])
            dest_infos = self.mega.find(current_path, exclude_deleted=True)
            dest = self.get_id_from_info(dest_infos)

        self.mega.create_folder(remaining_path, dest)

    def write(self, file_path: str, content: str) -> None:
        if self.exists(file_path):
            file_infos = self.mega.find(file_path, exclude_deleted=True)
            file_id = self.get_id_from_info(file_infos)
            self.mega.destroy(file_id)
        path = Path(file_path)
        parent_file_path = str(path.parent)
        dest_infos = self.mega.find(parent_file_path, exclude_deleted=True)
        dest = self.get_id_from_info(dest_infos)
        self.mega.upload(file_path, content, dest)
