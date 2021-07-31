import os
import subprocess
import hashlib

def md5(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def lexicographical_sort_lile(input_file, output_file):

    with open(input_file, "rt") as f:
        lines = f.readlines()
        print(":".join("{:02x}".format(ord(c)) for c in lines[0]))
        #lines = lines.split("\r\n")
        lines.sort()
    with open(output_file, 'wb') as f:
        #f.write(lines)
        for item in lines:
            item = item.rstrip("\n")
            f.write(f"{item}\r\n".encode("utf-8"))

if __name__ == "__main__":
    dirname = os.path.dirname(os.path.realpath(__file__))
    intput_file = os.path.join(dirname, "testcase", "world192_100.txt")
    actual_file = os.path.join(dirname, "testcase", "world192_100_out.txt")
    expected_file = os.path.join(dirname, "testcase", "world192_100_expected.txt")
    lexicographical_sort_lile(intput_file, expected_file)
    if md5(expected_file) == md5(actual_file):
        print(True)
    else:
        print(False)