from util.scd_type2 import scd_type2
from util.config_reader import config_reader
import os
import time
import glob


class file_watcher:
    def __init__(self):
        self.config_reader = config_reader()
        self.config_pipeline = self.config_reader.get_config_pipeline()

    def watcher(self):
        while True:
            print("Monitoring .. .. ..")
            if os.path.exists(self.config_pipeline.get('CONFIG_DETAIL', 'SOURCE_PATH')):
                for files in glob.glob(self.config_pipeline.get('CONFIG_DETAIL', 'SOURCE_PATH') + '\*.csv',
                                       recursive=False):
                    file_name = os.path.basename(files)
                    if "_first" in file_name:
                        print("First load:{}".format(file_name))
                        if scd_type2().scd_type2_implementation("first",file_name):
                            os.remove(files)
                    elif scd_type2().scd_type2_implementation("inc",file_name):
                        os.remove(files)
                    else:
                        break
            else:
                print("Path does not exists")
            time.sleep(10)


if __name__ == '__main__':
    file_watcher().watcher()
