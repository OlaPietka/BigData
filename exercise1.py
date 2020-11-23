# Program zlicza ilość wszystkich słów w każdej książce Shakespeare'a.
# Autorzy:
# Ola Piętka
# Robert Deyk

import requests
import zipfile
import os


def download_url(url, save_path, chunk_size=128):
    r = requests.get(url, stream=True)
    with open(save_path, 'wb') as fd:
        for chunk in r.iter_content(chunk_size=chunk_size):
            fd.write(chunk)


def unzip(target_path, unpacking_path):
    with zipfile.ZipFile(target_path, 'r') as zip_ref:
        zip_ref.extractall(unpacking_path)


def create_dir(dir_name):
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)


def count_words(file):
    with open(file, 'r') as artwork:
        data = artwork.readlines()  # read lines
        data = data[8:]  # remove first 7 line
        data = ''.join(data)  # convert to string

        words = [word for word in data.split() if '=' not in word]  # get list of all words without "="
    return len(words)


txt_dir_path = "shakespeare-txt/"
zip_dir_path = "shakespeare-zip/"
zip_path = zip_dir_path + "artwork.zip"
zip_url = "https://shakespeare.folger.edu/downloads/txt/shakespeares-works_TXT_FolgerShakespeare.zip"

create_dir(zip_dir_path)
create_dir(txt_dir_path)

download_url(zip_url, zip_path)
unzip(zip_path, txt_dir_path)

files = filter(lambda file: file.endswith(".txt"), os.listdir(txt_dir_path))  # filter all files with txt extension

words_count = [count_words(txt_dir_path + file) for file in files]  # count words in all books

print("Sum of all words:", sum(words_count))
