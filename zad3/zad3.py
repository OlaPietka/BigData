# Program zlicza ilość wszystkich słów w każdej książce Shakespeare'a i Mark'a Twain'a, wykorzystując bibliotekę pyspark.
# Autorzy:
# Ola Piętka
# Robert Deyk

from timeit import default_timer as time
import nltk
from nltk.corpus import wordnet as wn
from pyspark import SparkContext
from web_scraper import BookWebScraper, MessageManager


class SparkWordsCounter(MessageManager):
    def __init__(self, url, sc, tag):
        """
        Args:
            url: url to author page on freeclassicebooks.com site
            sc: SparkContext object
            tag: name of the tag where text is located in mht files
        """
        self.url = url
        self.sc = sc
        self.tag = tag

        t0 = time()
        self.words = self.get_words()
        self.time_message(time() - t0, "get_words")

        t0 = time()
        self.words_num = self.count_words()
        self.time_message(time() - t0, "count_words")

        t0 = time()
        self.words_occ = self.count_words_occurrence()
        self.time_message(time() - t0, "count_words_occurrence")

        t0 = time()
        self.top_words = self.top_words()
        self.time_message(time() - t0, "top_words")

        t0 = time()
        self.top_nouns = self.top_nouns()
        self.time_message(time() - t0, "top_nouns")

    def get_words(self):
        """
        Get all words using BookWebScraper
        Returns:
            RDD object containing list of collected words
        """
        book_web_scraper = BookWebScraper(self.url, tag=self.tag)

        words = book_web_scraper.words
        words = self.sc.parallelize(words)
        return words

    def count_words(self):
        """
        Count number of words
        Returns:
            int number of words
        """
        self.info_message("counting words")
        return self.words.count()

    def count_words_occurrence(self):
        """
        Count occurrence of all words
        Returns:
            RDD object containing parsed number of words and their occurrence
        """
        self.info_message("counting words occurrence")
        words_count = self.words.map(lambda word: (word, 1)) \
            .reduceByKey(lambda v1, v2: v1 + v2) \
            .map(lambda x: (x[1], x[0])) \
            .sortByKey(ascending=False) \
            .persist()

        return words_count

    def top_words(self, top=10):
        """
        Get top words
        Args:
            top: number of top words we want to get

        Returns:
            list of most common words
        """
        self.info_message("getting top words")
        top_words = self.words_occ.take(top)

        return top_words

    def top_nouns(self, top=3):
        """
        Get top nouns
        Args:
            top: number of top words we want to get

        Returns:
            list of most common nouns
        """
        self.info_message("getting top nouns")

        def get_nouns():
            """
            Download nltk packages and get all english nouns
            Returns:
                list of all english nouns
            """
            nltk.download('wordnet')
            nltk.download('brown')
            nltk.download('punkt')
            nouns = {x.name().split('.', 1)[0] for x in wn.all_synsets('n')}
            return nouns
        nouns = get_nouns()

        most_common = []
        for word_occ in self.words_occ.collect():
            _, val = word_occ
            if val in nouns:
                most_common.append(word_occ)
            if len(most_common) == top:
                break

        return most_common

    def print_info(self):
        """
        Prints all information
        Returns:
            None
        """
        self.message("Words number: {}".format(self.words_num))
        self.occurrence_message(self.top_words, "words")
        self.occurrence_message(self.top_nouns, "nouns")


if __name__ == "__main__":
    twain_url = "https://freeclassicebooks.com/Mark%20Twain.htm"
    shakespeare_url = "https://freeclassicebooks.com/william_shakespeare.htm"

    sc = SparkContext("local", "first app")

    start_time = time()
    twain_words_counter = SparkWordsCounter(twain_url, sc, tag='span')
    shakespeare_words_counter = SparkWordsCounter(shakespeare_url, sc, tag='a')
    print("all: %s seconds" % (time() - start_time))

    twain_words_counter.print_info()
    shakespeare_words_counter.print_info()

