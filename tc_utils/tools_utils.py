import random
import string
import yaml
import ujson as json
from typing import Optional


def from_yaml(yaml_file: str) -> dict:
    """
    Parse YAML in a stream, produce the corresponding Python obj
    :param yaml_file:
    :return: yml obj
    """
    with open(yaml_file, "r") as cf:
        yaml_data = yaml.safe_load(cf)
    return yaml_data


def write_into_json(json_file: str, data_dict: dict = None) -> None:
    """
    Serialize obj as a JSON formatted stream to fn
    :param json_file:
    :param data_dict:
    """
    if not data_dict:
        raise Exception("Pls specify data dictionary.")

    with open(json_file, "w", encoding="utf-8") as fp:
        json.dump(data_dict, fp, indent=2)


def read_from_json(json_file: str) -> dict:
    """
    Deserialize fn - from JSON to Python obj
    :param json_file:
    :return:
    """
    with open(json_file, "r") as f:
        json_data = json.load(f)
    return json_data


def name_gen(type_flag: str, pre_name: Optional[str] = None) -> str:
    """
    :param type_flag: dts | cos
    :param pre_name: dts job or bucket name prefix
    :return: Twelve length list of unique elements chosen from the sequence
    name_gen("dts") -> dts-inxhcol1
    name_gen("dts", "bin") -> bin-47yzxk9q
    name_gen("cos") -> cos-3120958674
    name_gen("cos", "bin") -> bin-4179036825
    """
    dts_tf, cos_tf = "dts", "cos"

    assert (type_flag == dts_tf or type_flag == cos_tf), \
        f"You have to specify type_flag value as either {dts_tf!r} or {cos_tf!r}"

    if type_flag == dts_tf:
        if not pre_name:
            pre_name = dts_tf
        return f"{pre_name}-{''.join(random.sample(string.ascii_lowercase + string.digits, 8))}"

    if type_flag == cos_tf:
        if not pre_name:
            pre_name = cos_tf
        return f"{pre_name}-{''.join(''.join(random.sample(string.digits, 10)))}"
