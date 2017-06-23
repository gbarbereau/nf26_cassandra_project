#!/usr/bin/env python
from database import *
from kmeans import *
from analyze import *
from tools import *
import handle_file
import math


def main():
    bdd = Bdd()
    bdd.connect("e13")
    bdd.change_default_timeout(3600)
    #handle_file.file_insertion_handler(Bdd)
    x = kmeans(bdd, "facts","end", 5)
    print(x)
    # answer(bdd)
    bdd.disconnect()


if __name__ == '__main__':
    main()
