__author__ = 'ashish'

import pandas as pd
import shutil
import sys
import os
import re
import gc

text_features = {'donations.csv': ['donation_message'],
                 'essays.csv': ['title', 'short_description', 'need_statement', 'essay'],
                 'resources.csv': ["vendor_name", "project_resource_type", "item_name", "item_number"]}

in_dir = ".\data\data_files\\"
out_dir = ".\data\input\\"
pattern = re.compile(r'[^\w.% /-]')


def clean_text(x):
    """
    Cleans the chunk of text by removing '\r\n' and other characters defined by pattern.
    :param x: text to clean
    :return: cleaned text
    """
    return re.sub(r'\s+', ' ', pattern.sub(' ', re.sub(r"\r\\n", "", str(x))).strip())


def clean_kdd_files():
    """
    Cleans the KDD data files and writes them to a new folder.
    1. Original KDD file must be present in .\data\data_files directory
    2. Output files will be written to .\data\input directory
    :raise: any exception is raised
    """
    try:
        data_files = os.listdir(in_dir)
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)

        files_to_clean = set(data_files).intersection(text_features.keys())
        files_to_copy_only = set(data_files).difference(text_features.keys())
        print "Files to Clean: ", files_to_clean
        print "Files to Copy: ", files_to_copy_only

        for file_name in files_to_clean:
            in_file = in_dir + file_name
            out_file = out_dir + file_name
            print "\nIn File: %s \t----\t Out File: %s " % (in_file, out_file)
            print "Reading %s ..." % in_file

            df = pd.read_csv(in_file)
            print "Data dimensions: ", df.shape

            cols_to_clean = text_features[file_name]
            print "Applying clean_text to feature(s): %s ..." % ", ".join(cols_to_clean)
            df[cols_to_clean] = df[cols_to_clean].applymap(clean_text)
            print "Saving cleaned file to %s ..." % out_file
            df.to_csv(out_file, sep=',', encoding='utf-8', header=True, index=False)
            print "Done"
            del df
            gc.collect()

        print "\n"

        for file_name in files_to_copy_only:
            in_file = in_dir + file_name
            out_file = out_dir + file_name
            print "Copying %s AS IS, to %s ..." % (in_file, out_file)
            shutil.copy(in_file, out_file)
            print "Done"

        print "\nHave fun!"

    except Exception, e:
        print e.message
        raise e


if __name__ == "__main__":

    try:
        clean_kdd_files()

    except Exception, e:
        sys.exit(-1)
