import glob

class truncate_file:

    def __init__(self):
        pass

    def truncate_content(self):
        for dir in ["Training_Log", "Model_Log"]:
            for file in glob.glob(dir+"/*.txt"):
                with open(file, 'r+') as f:
                    f.truncate(0)