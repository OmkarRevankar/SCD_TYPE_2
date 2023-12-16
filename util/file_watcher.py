from util.scd_type2 import scd_type2
from util.config_reader import config_reader
import os
import time
import argparse

class file_watcher:
    def __init__(self):
        self.config_reader = config_reader()
        self.config_pipeline = self.config_reader.get_config_pipeline()
        self.parser = argparse.ArgumentParser()
        self.parser.add_argument('--batch', type=str, required=True)
        self.args = self.parser.parse_args()

    def watcher(self):
        while True:
            print("Monitoring .. .. ..")
            if os.path.exists(self.config_pipeline.get('CONFIG_DETAIL', 'SOURCE_PATH')) or os.path.exists(self.config_pipeline.get('CONFIG_DETAIL','SOURCE_PATH_NEW_LOAD')):
                if os.path.exists(self.config_pipeline.get('CONFIG_DETAIL', 'SOURCE_PATH_NEW_LOAD')) == True:
                    file_name = self.config_pipeline.get('CONFIG_DETAIL', 'SOURCE_PATH_NEW_LOAD')
                elif os.path.exists(self.config_pipeline.get('CONFIG_DETAIL', 'SOURCE_PATH'))== True:
                    file_name = self.config_pipeline.get('CONFIG_DETAIL', 'SOURCE_PATH')
                else:
                    exit()
                print("File Present:{}".format(file_name))
                if scd_type2().scd_type2_implementation(self.args.batch):
                    os.remove(file_name)
                else:
                    print("Some Error Occured, not processed")
            else:
                print("File Not Present")
            time.sleep(10)

if __name__ == '__main__':
    file_watcher().watcher()