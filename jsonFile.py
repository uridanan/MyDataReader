import json
from pprint import pprint


def loadjsondata(filename):
    with open(filename) as data_file:
        data = json.load(data_file)
    return data


def main():
    cfg = loadjsondata('data.json')

main()