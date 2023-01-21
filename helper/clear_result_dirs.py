import os
from glob import glob

from helper.methods import WHICH


def clear_old_results():
    files = glob('/results/' + WHICH + '/qm/dates/**/*.*', recursive=True)

    for f in files:
        try:
            os.remove(f)
        except OSError as e:
            print("Error: %s : %s" % (f, e.strerror))
