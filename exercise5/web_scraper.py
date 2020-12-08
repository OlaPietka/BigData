import re

import requests
from bs4 import BeautifulSoup


class MessageManager:
    @staticmethod
    def message(mess):
        """
        Prints simple message
        Args:
            mess: message to print

        Returns:
            None
        """
        print(mess)

    @staticmethod
    def info_message(mess):
        """
        Prints info message (used to show what is running in the moment)
        Args:
            mess: message to print

        Returns:
            None
        """
        print("----------{}----------".format(mess.upper()))

    @staticmethod
    def status_message(i, n, mess):
        """
        Prints status message about job already done
        Args:
            i: number of things done
            n: number of all things to do
            mess: message to print

        Returns:
            None
        """
        print("{}/{} - {}".format(i, n, mess))

    @staticmethod
    def occurrence_message(dic, mess):
        """
        Prints list with values and their occurrence
        Args:
            dic: list of tuples with value and his occurrence
            mess: message to print

        Returns:
            None
        """
        print("----------")
        print("Most used {}:".format(mess))
        for i, x in enumerate(dic, 1):
            key, value = x
            print("{}. {} - {}".format(i, value, key))
        print("----------")

    @staticmethod
    def time_message(time, func):
        """
        Prints message with execution time
        Args:
            time: seconds
            func: name of the function

        Returns:
            None
        """
        print("{}: {} seconds".format(func, round(time, 5)))


class BookWebScraper(MessageManager):
    def __init__(self, author_url, tag='span'):
        """
        Args:
            author_url: url to author page on freeclassicebooks.com site
            tag: name of the tag where text is located in mht files
        """
        self.author_url = author_url
        self.tag = tag

        self.links = self.get_links()[:5]
        self.text = self.get_text()
        self.words = self.text.split()

    def get_links(self):
        """
        Get downloadable links for all listed books
        Returns:
            list of downloadable links
        """
        self.info_message("extracting links")

        html = requests.get(self.author_url, stream=True).content
        parsed_html = BeautifulSoup(html, features="html.parser")

        links = ["https://freeclassicebooks.com/" + a['href'] for a in parsed_html.findAll("a") if "online" in str(a)]
        return links

    def extract_text(self, file):
        """
        Extract text as string from file
        Args:
            file: link to file

        Returns:
            string containing text from file
        """
        def file_content(url):
            """
            Get content of mht file
            Args:
                url: url of the file

            Returns:
                string containing content of mht file
            """
            return requests.get(url, stream=True).content

        def is_text(text):
            """
            Check whether text does not contains special characters
            Args:
                text: text to verify

            Returns:
                True if text does not contains special characters, False otherwise
            """
            dic = "<>&/"
            return not any(d in str(text) for d in dic)

        file_content1 = file_content(file)

        parsed_html = BeautifulSoup(file_content1, features="html.parser")
        div = parsed_html.find("div")
        tags = div.find_all(self.tag)

        text = [re.sub(r'[^A-Za-z0-9 ]+', '', str(tag.text).lower()) for tag in tags if is_text(tag.text)]
        return ''.join(text)

    def get_text(self):
        """
        Get all text from files
        Returns:
            string of text from all files
        """
        self.info_message("extracting text")

        text = ""
        for i, book in enumerate(self.links):
            self.status_message(i, len(self.links), "text extracted")
            text += self.extract_text(book)

        return text
