# Program szuka najczęsciej używanych słów i rzeczowników przez Mark'a Twain'a.
# Autorzy:
# Ola Piętka
# Robert Deyk

import os
import sys

import PyPDF2
import requests
from bs4 import BeautifulSoup
from collections import Counter
from nltk.corpus import wordnet as wn
import nltk


class MessageManager:
    @staticmethod
    def message(mess):
        print(mess)

    @staticmethod
    def info_message(mess):
        print("----------{}----------".format(mess.upper()))

    @staticmethod
    def status_message(i, n, mess):
        print("{}/{} - {}".format(i, n, mess))

    @staticmethod
    def occurrence_message(dic, mess):
        print("----------")
        print("Most used {}:".format(mess))
        for i, x in enumerate(dic, 1):
            key, value = x
            print("{}. {} - {}".format(i, key, value))
        print("----------")


class BookWebScraper(MessageManager):
    def __init__(self, author_url, dir):
        self.author_url = author_url
        self.dir = dir

        self.create_dir(dir)

        self.links = self.get_links()[:1]
        self.words = self.get_all_words()

        self.words_occ = self.count_occurrence(self.words)
        self.words_num = self.count_words()
        self.top_words = self.top_words()
        self.top_nouns = self.top_nouns()

    # Get downloadable links for all listed books
    def get_links(self):
        self.info_message("extracting links")

        html = requests.get(self.author_url, stream=True).content
        parsed_html = BeautifulSoup(html, features="html.parser")

        links = ["https://freeclassicebooks.com/" + a['href'] for a in parsed_html.findAll("a") if "online" in str(a)]
        return links

    # Extract text as string from file
    def extract_text(self, file):
        def file_content(url):
            return requests.get(url, stream=True).content

        def is_text(text):
            dic = "<>&/"
            return not any(d in str(text) for d in dic)

        file_content = file_content(file)

        parsed_html = BeautifulSoup(file_content, features="html.parser")
        div = parsed_html.find("div")
        spans = div.find_all('span')

        text = [str(text) for span in spans for text in span.text if is_text(text)]
        return ''.join(text)

    def get_all_words(self):
        self.info_message("extracting words")

        text = ""
        for i, book in enumerate(self.links):
            self.status_message(i, len(self.links), "text extracted")
            text += self.extract_text(book)

        return self.filter_words(text.split())

    def count_words(self):
        self.info_message("counting words")
        return len(self.words)

    def top_words(self, top=10):
        self.info_message("getting top words")
        return self.words_occ[:top+1]

    def top_nouns(self, top=3):
        def get_nouns():
            self.info_message("downloading nouns")
            nltk.download('brown')
            nltk.download('punkt')
            nouns = {x.name().split('.', 1)[0] for x in wn.all_synsets('n')}
            return nouns
        nouns = get_nouns()

        self.info_message("getting top nouns")

        most_common = []
        for x in self.words_occ:
            key, _ = x

            if key in nouns:
                most_common.append(x)
            if len(most_common) == top:
                break
        return most_common

    def print_inf(self):
        self.message("Words number: {}".format(self.words_num))
        self.occurrence_message(self.top_words, "words")
        self.occurrence_message(self.top_nouns, "nouns")

    # Filter list of words
    @staticmethod
    def filter_words(words):
        def to_lower(words):
            return list(map(lambda x: x.lower(), words))

        def to_alphanum(words):
            return list(map(lambda x: ''.join(e for e in x if e.isalnum()), words))

        def del_empty(words):
            return list(filter(lambda x: x != '', words))

        words = to_lower(words)
        words = to_alphanum(words)
        words = del_empty(words)

        return words

    @staticmethod
    def count_occurrence(list):
        return Counter(list).most_common()

    @staticmethod
    def create_dir(dir_name):
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)


twain_url = "https://freeclassicebooks.com/Mark%20Twain.htm"
twain_dir = "twain-mht/"

twain = BookWebScraper(twain_url, twain_dir)

twain.print_inf()
