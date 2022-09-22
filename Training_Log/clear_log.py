import glob

class truncate_file:

    def __init__(self):
        pass

    def truncate_content(self):

        for file in glob.glob("Training_Log/*.txt"):
            with open(file, 'r+') as f:
                f.truncate(0)