import os
import PyPDF2
import requests
from bs4 import BeautifulSoup
from collections import Counter
from nltk.corpus import wordnet as wn
import nltk


def print_message(i, n, mess):
    print("{}/{} - {}".format(i, n, mess))


def get_pdf_links(url):
    html = requests.get(url, stream=True).content

    parsed_html = BeautifulSoup(html, features="html.parser")

    links = ["https://freeclassicebooks.com/" + a['href'] for a in parsed_html.findAll("a") if "pdf" in str(a)]
    return links


def save_pdf(url, file_path):
    r = requests.get(url, stream=True)

    with open(file_path, "wb") as pdf:
        pdf.write(r.content)


def download_all(urls, dir):
    for i, url in enumerate(urls):
        print_message(i+1, len(urls), "downloaded")
        save_pdf(url, "{}{}.pdf".format(dir, i))


def create_dir(dir_name):
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)


def extract_text(pdf):
    reader = PyPDF2.PdfFileReader(pdf)

    text = ' '.join([page.extractText() for page in reader.pages])
    return text


def get_all_words(dir):
    pdfs = os.listdir(dir)

    text = ""
    for i, pdf in enumerate(pdfs):
        print_message(i + 1, len(pdfs), "extracted")
        text += extract_text(dir + pdf)

    words = [word for word in text.split() if "www.freeclassicebooks.com" not in word]
    return words


def count_occurrence(list, top=None):
    return Counter(list).most_common(top)


def print_occurrence(dic, mess):
    print("----------")
    print("Most used {}:".format(mess))
    for i, x in enumerate(dic, 1):
        key, value = x
        print("{}. {} - {}".format(i, key, value))
    print("----------")


def get_nouns():
    nltk.download('brown')
    nltk.download('punkt')
    nouns = {x.name().split('.', 1)[0] for x in wn.all_synsets('n')}
    return nouns


def top_nouns(dic, top=3):
    nouns = get_nouns()

    most_common = []
    for x in dic:
        key, _ = x

        if key in nouns:
            most_common.append(x)
        if len(most_common) == top:
            break
    return most_common


url = "https://freeclassicebooks.com/Mark%20Twain.htm"
dir = "twain-pdf/"

create_dir(dir)

links = get_pdf_links(url)

download_all(links, dir)

words = get_all_words(dir)

occurrence = count_occurrence(words)

most_common_words = occurrence[:11]
print_occurrence(most_common_words, "words")

most_common_nouns = top_nouns(occurrence)
print_occurrence(most_common_nouns, "nouns")



