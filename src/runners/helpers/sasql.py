import re
from os import path


def read_template(file_path):
    return open(file_path).read()


def load(file_path):
    """load a sasql file as sql"""
    file_dir = path.dirname(file_path)
    tmpl = open(file_path).read()

    tmpl = re.sub(
        r"USING TEMPLATE '([a-z_\.]+)'",
        lambda m: f"AS $$\n{read_template(path.join(file_dir, m.group(1)))}$$",
        tmpl,
        flags=re.M,
    )

    return tmpl
