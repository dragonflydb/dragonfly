import os.path

"""
Generates index.html files recursively in all directories. Note that this is strictly an optional step.
Both YUM and APT repositories work fine without index listing, but this is useful for debugging a broken
site.
"""

HEADER = """<!DOCTYPE html>
<html>
<body>
"""

FOOTER = """</body>
</html>
"""


def build_index(dirpath):
    print(f"building index.html for {dirpath}")
    target = os.path.join(dirpath, "index.html")
    with open(target, "w") as f:
        f.write(HEADER.format(dir=dirpath))
        for item in sorted(os.listdir(dirpath)):
            if item == "index.html":
                continue
            name = item + "/" if os.path.isdir(os.path.join(dirpath, item)) else item
            f.write(f"""<a href="{item}">{name}</a><br>\n""")
        f.write(FOOTER)


def recurse_dir(root):
    for root, dirs, _ in os.walk(root):
        build_index(root)


if __name__ == "__main__":
    import sys

    if len(sys.argv) == 1:
        print(f"Usage: {sys.argv[0]} <site folder>")
        sys.exit(1)

    recurse_dir(sys.argv[1])
