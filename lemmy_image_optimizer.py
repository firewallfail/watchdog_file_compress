import time
import os
import sys
import argparse
import logging
import sqlite3
import datetime

from PIL import Image


class Database():
    def __init__(self):
        self.db_conn = sqlite3.connect("optimized.db")
        # ensure table exists
        cur = self.db_conn.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS images (path VARCHAR(255) PRIMARY KEY);")
        self.db_conn.commit()
        cur.close()

    def already_exists(self, file):
        cur = self.db_conn.cursor()
        try:
            res = cur.execute("SELECT path FROM images WHERE path = (?)", (file,))
            exists = res.fetchone()
            cur.close()
            return exists
        except Exception as e:
            cur.close()
            return True
        
    def insert_path(self, file):
        cur = self.db_conn.cursor()
        try:
            cur.execute("INSERT INTO images VALUES (?)", (file,))
            self.db_conn.commit()
            cur.close()
        except Exception as e:
            cur.close()


class ImageHandler():
    def __init__(self, path, quality=70, should_optimize=True):
        self.path = path
        self.quality = quality
        self.should_optimize = should_optimize

    def optimize(self):
        with Image.open(self.path) as image:
            image.save(self.path, format=image.format, optimize=self.should_optimize, quality=self.quality)


class ImageSearch():
    def __init__(self, path):
        self.path = path

    def find_files(self, db, bulk=True):
        found = []
        # os.walk returns (current directory, contained directories, contained files)
        for directory in os.walk(self.path):
            # skip if no files in directories
            if not directory[2]:
                continue
            for file in directory[2]:
                full_path = f"{directory[0]}/{file}"
                exists = db.already_exists(full_path)
                # bulk attempts to optimize any file found
                # fast only attempts to optimize files that haven't been found before
                if bulk or not exists :
                    found.append(full_path)
        return found
    
    def show_progress(self, total, complete):
        percent_complete = int(complete / total * 100)
        print(f"File: {complete} [{percent_complete}%]", "[" + "="*percent_complete + " "*(100 - percent_complete) + "]", end='\r')

    def optimize_images(self, files, db):
        complete, total, success, failed = 0, len(files), 0, 0
        for file in files:
            complete += 1
            self.show_progress(total, complete)
            try:
                db.insert_path(file)
                ImageHandler(file).optimize()
                success += 1
            except Exception as e:
                failed += 1
                pass
        return success, failed


def dir_path(string):
    if os.path.isdir(string):
        return string
    else:
        logging.error(f"'{string}' is not a valid directory")
        raise NotADirectoryError(string)


if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(prog="Lemmy Image Compress")
        parser.add_argument("-b", help="Optimize all images in directory", metavar="All", type=dir_path)
        parser.add_argument("-f", help="Optimize only new files", metavar="Fast", type=dir_path)
        args = parser.parse_args()
        db = Database()
    except NotADirectoryError as e:
        sys.exit()
    except Exception as e:
        logging.error("Unknown Error", e)
        sys.exit()

    args = vars(args)
    fast_path, bulk_path = args['f'], args['b']

    if bulk_path:
        bulk_start = time.time()
        b = ImageSearch(bulk_path)
        files = b.find_files(db)
        if not files:
            print('No files found')
            sys.exit()
        success, failed = b.optimize_images(files, db)
        total_time = str(datetime.timedelta(seconds=int(time.time() - bulk_start)))
        print()
        print(f"Total:{len(files)} Optimized:{success} Failed:{failed} Time:{total_time}")
    
    if fast_path:
        fast_start = time.time()
        b = ImageSearch(fast_path)
        files = b.find_files(db, bulk=False)
        if not len(files):
            print('No new files found')
            sys.exit()
        success, failed = b.optimize_images(files, db)
        total_time = str(datetime.timedelta(seconds=int(time.time() - fast_start)))
        print()
        print(f"Total:{len(files)} Optimized:{success} Failed:{failed} Time:{total_time} seconds")
