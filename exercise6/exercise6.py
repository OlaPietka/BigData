# Program generuje rekomendacje filmów na podstawie wczesniej zebranych danych
# Autorzy:
# Ola Piętka
# Robert Deyk

from math import sqrt

from omdbapi.movie_search import GetMovie
from parse_csv import parse_csv
from pyspark.sql import SparkSession


class Recommendations:
    def __init__(self, filename):
        """
        Args:
            filename: Filename to csv data
        """
        self.filename = filename

        parse_csv(self.filename)

        self.data = self.get_data_frame()

    def get_data_frame(self):
        """
        Create spark session and read csv as data frame
        Returns:
            Dataframe with collected data
        """
        spark_session = SparkSession.builder.appName("app").getOrCreate()
        df = spark_session.read.csv("parse_{}.csv".format(self.filename), header=True, inferSchema=True)
        return df

    def users_data(self, user1, user2):
        """
        Get filtered data by u1 and u2
        Args:
            user1: Name 1st of the user
            user2: Name 2nd of the user

        Returns:
            Dataframe with filtered data
        """
        return self.data.filter((self.data["Osoba"] == user1) | (self.data["Osoba"] == user2))

    def user_data(self, user):
        """
        Get filtered data by user
        Args:
            user: Name of the user

        Returns:
            Dataframe with filtered data
        """
        return self.data.filter((self.data["Osoba"] == user))

    def filter_diff_titles(self, user1, user2):
        """
            Filter data by different titles
            Args:
                user1: Name 1st of the user
                user2: Name 2nd of the user

            Returns:
                List of different titles of both users
        """
        # Get data for u1 and u2
        users_data = self.users_data(user1, user2)
        # Group data by "Tytuł" and count occurrence
        group_title = users_data.groupBy("Tytuł").count()
        # Get only data where count == 1 (different titles for both users)
        titles_diff = group_title.filter(group_title["count"] == 1)

        return [i[0] for i in titles_diff.collect()]

    def get_data_by_titles(self, titles_list, user):
        """
        Get common data in titles_list and user titles
        Args:
            titles_list: lList of titles
            user: Name of teh user

        Returns:
            List of common data
        """
        # Get data for u1
        user_data = self.user_data(user)

        # Get user "Tytuł" data that are in "titles"
        data = user_data.filter(user_data["Tytuł"].isin(titles_list)).select("Tytuł", "Ocena")

        return {title: grade for title, grade in data.collect()}

    def get_common_data(self, user1, user2):
        """
        Get grades and titles for both users' common movies
        Args:
            user1: Name 1st of the user
            user2: Name 2nd of the user

        Returns:
            List of common titles and their grades
        """
        def filter_common_titles(user1, user2):
            """
            Filter data by common titles
            Args:
                user1: Name 1st of the user
                user2: Name 2nd of the user

            Returns:
                List of common titles
            """
            users_data = self.users_data(user1, user2)
            group_title = users_data.groupBy("Tytuł").count()
            common_titles = group_title.filter(group_title["count"] > 1)

            return [i[0] for i in common_titles.collect()]
        common_titles = filter_common_titles(user1, user2)

        users_data = self.users_data(user1, user2)

        return users_data.filter(users_data["Tytuł"].isin(common_titles)).select("Tytuł", "Ocena").collect()

    def compute(self, user1, user2):
        """
        Calculate euclidean distance for common titles
        Args:
            user1: Name of 1st user
            user2: Name of 2nd user

        Returns:
            Float score
        """
        common_data = self.get_common_data(user1, user2)

        if len(common_data) == 0:
            return 0

        compute_data = {}
        for row in common_data:
            title, grade = row
            print(row)

            if title in compute_data.keys():
                compute_data[title] -= grade
                compute_data[title] **= 2
            else:
                compute_data[title] = grade

        return 1 / (1 + sqrt(sum(compute_data.values())))

    def best_match(self, input_user):
        """
        Get best match for urse
        Args:
            input_user: Name of teh user

        Returns:
            String name of best matching user
        """
        def get_users_expect(user):
            """
            Get all users in data, expect input user
            Returns:
                List of all users
            """
            return [u[0] for u in self.data.groupBy("Osoba").count().select("Osoba").collect() if u[0] != user]
        users = get_users_expect(input_user)

        scores = {user: self.compute(user, input_user) for user in users}

        return list(sorted(scores.items(), key=lambda item: item[1]))[-1]

    def get_recommendations(self, input_user):
        """
        Get best recommended titles for user
        Args:
            input_user: Nameof teh user

        Returns:
            List of best titles
        """
        best_user, _ = self.best_match(input_user)
        print("Best matching user:", best_user)

        diff_titles = self.filter_diff_titles(best_user, input_user)
        diff_data = self.get_data_by_titles(diff_titles, best_user)

        top_recomm = [title for title, _ in list(sorted(diff_data.items(), key=lambda item: item[1], reverse=True))[:5]]
        return top_recomm

    def print_recommendations(self, input_user):
        """
        Prints all information
        Args:
            input_user: Name of the user

        Returns:
            None
        """
        def print_info(data, i):
            """
            Print information for single data
            Args:
                data: Sict with all information
                i: Number on list

            Returns:
                None
            """
            print("------{}------".format(i))
            print(data["Title"].upper())
            print("Rating:", data["imdbRating"])
            print("Genre:", data["Genre"])
            print("Plot:", data["Plot"])
            print("Actors:", data["Actors"])
            print("\n")

        titles = self.get_recommendations(input_user)

        for i, title in enumerate(titles, 1):
            movie = GetMovie(title=title, api_key='29e4e98a', plot='full')
            data = movie.get_all_data()
            print_info(data, i)


if __name__ == "__main__":
    recommendations = Recommendations("data")
    recommendations.print_recommendations("Paweł Czapiewski")
