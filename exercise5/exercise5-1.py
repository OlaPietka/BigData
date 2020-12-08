# Program z zadania 4, rozszerzony o kategoryzację słów oraz wykrywanie języka na podstawie częstości występowania liter
# w każdej książce Shakespeare'a i Mark'a Twain'a, wykorzystując bibliotekę pyspark.
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
        self.words, self.letters = self.get_text()
        self.words_time = time() - t0

        t0 = time()
        self.words_num = self.count_words()
        self.words_num_time = time() - t0

        t0 = time()
        self.words_occ = self.count_words_occurrence()
        self.words_occ_time = time() - t0

        t0 = time()
        self.letters_occ = self.count_letters_occurrence()
        self.letters_occ_time = time() - t0

        t0 = time()
        self.words_cat = self.words_category()
        self.words_cat_time = time() - t0

        t0 = time()
        self.top_words = self.top_words()
        self.top_words_time = time() - t0

        t0 = time()
        self.top_nouns = self.top_nouns()
        self.top_nouns_time = time() - t0

        self.language = self.get_language()

    def get_text(self):
        """
        Get all words using BookWebScraper
        Returns:
            RDD object containing list of collected words
        """
        book_web_scraper = BookWebScraper(self.url, tag=self.tag)

        words = book_web_scraper.words
        text = book_web_scraper.text

        words = self.sc.parallelize(words)
        text = self.sc.parallelize(list(text.replace(' ', '')))
        return words, text

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

    def count_letters_occurrence(self):
        self.info_message("counting letters occurrence")
        letters_occ = self.letters.map(lambda letter: (letter, 1)) \
            .reduceByKey(lambda v1, v2: v1 + v2) \
            .map(lambda x: (x[1], x[0])) \
            .sortByKey(ascending=False) \
            .persist()

        return letters_occ.collect()

    def get_language(self):
        self.info_message("getting language")
        pl = "iaeozn"
        en = "etaoin"

        most_common = [letter for _, letter in self.letters_occ[:6]]

        pl_diff_num = sum(1 for a, b in zip(most_common, pl) if a != b)
        en_diff_num = sum(1 for a, b in zip(most_common, en) if a != b)

        return "English" if pl_diff_num > en_diff_num else "Polish"

    def words_category(self):
        """
        Count words categories
        Returns:
            list of categories and their number of occurrence
        """
        def get_category(word):
            """
            Get name of category based on word length
            Args:
                word: word to check

            Returns:
                string category
            """
            n = len(word)
            return "Tiny" if n == 1 else "Small" if n <= 4 else "Medium" if n <= 9 else "Big"

        self.info_message("counting words category")
        words_cat = self.words.map(lambda word: (get_category(word), 1)) \
            .reduceByKey(lambda v1, v2: v1 + v2) \
            .map(lambda x: (x[1], x[0])) \
            .sortByKey(ascending=False) \
            .persist()

        return words_cat.collect()

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
        self.print_time()
        self.occurrence_message(self.top_words, "words")
        self.occurrence_message(self.top_nouns, "nouns")
        self.occurrence_message(self.words_cat, "categories")
        self.occurrence_message(self.letters_occ, "letters")
        self.message("Words number: {}".format(self.words_num))
        self.message("Language: {}".format(self.language))

    def print_time(self):
        """
        Prints time of execution of each function
        Returns:
            None
        """
        self.time_message(self.words_time, "get_words")
        self.time_message(self.words_num_time, "count_words")
        self.time_message(self.words_occ_time, "count_words_occurrence")
        self.time_message(self.letters_occ_time, "count_letters_occurrence")
        self.time_message(self.words_cat_time, "words_category")
        self.time_message(self.top_words_time, "top_words")
        self.time_message(self.top_nouns_time, "top_nouns")


if __name__ == "__main__":
    twain_url = "https://freeclassicebooks.com/Mark%20Twain.htm"

    sc = SparkContext("local", "first app")

    twain_words_counter = SparkWordsCounter(twain_url, sc, tag='span')

    twain_words_counter.print_info()
