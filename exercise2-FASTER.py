# Program szuka najczęsciej używanych słów i rzeczowników przez Mark'a Twain'a.
# Autorzy:
# Ola Piętka
# Robert Deyk

import os
from collections import Counter

import nltk
import requests
from bs4 import BeautifulSoup
from nltk.corpus import wordnet as wn


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
    def __init__(self, author_url, tag="span"):
        self.author_url = author_url
        self.tag = tag

        self.links = self.get_links()
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

        file_content1 = file_content(file)

        parsed_html = BeautifulSoup(file_content1, features="html.parser")
        div = parsed_html.find("div")
        tags = div.find_all(self.tag)

        text = [re.sub(r'[^A-Za-z0-9 ]+', '', str(tag.text).lower()) for tag in tags if is_text(tag.text)]
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
        return self.words_occ[:top]

    def top_nouns(self, top=3):
        def get_nouns():
            self.info_message("downloading nouns")
            nltk.download('wordnet')
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
        return list(filter(lambda word: word.isalnum() and word != '', words))

    @staticmethod
    def count_occurrence(list):
        return Counter(list).most_common()


twain_url = "https://freeclassicebooks.com/Mark%20Twain.htm"
shakespeare_url = "https://freeclassicebooks.com/william_shakespeare.htm"

twain = BookWebScraper(twain_url, tag='span')
shakespeare = BookWebScraper(twain_url, tag='a')

twain.print_inf()
shakespeare.print_inf()
