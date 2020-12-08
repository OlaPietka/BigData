# Program analizuje sytuację z Covid19 w konkretnych stanach w USA
# Autorzy:
# Ola Piętka
# Robert Deyk

from io import StringIO

import requests
from pandas.io.json import build_table_schema
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType


class Covid19USA:
    url = "https://api.covidtracking.com/v1/states/current.csv"

    def __init__(self, filename="covid19", write=False):
        """
        Args:
            filename: name of the file to save on hdfs
            write: whether to write file on hdfs. If false, assumes file already exists
        """
        self.filename = filename
        self.cases = ["positive", "negative", "death", "totalTestResults"]
        self.spark_session = SparkSession.builder.appName("app").getOrCreate()

        self.csv = self.get_csv()

        if write:
            self.write_to_hdfs()
        self.data = self.get_data()

        self.min_cases = self.all_min()
        self.max_cases = self.all_max()
        self.mean_cases = self.all_mean()
        self.std_cases = self.all_std()
        self.var_cases = self.all_var()

        self.where_state, self.where_prob = self.where_to_go()

    def get_csv(self):
        """
        Get csv from site: https://covidtracking.com/data/api/
        Returns:
            string object with collected data
        """
        csv = requests.get(self.url, stream=True).content.decode("utf-8")
        csv = pd.read_csv(StringIO(csv))

        return csv

    def write_to_hdfs(self):
        """
        Writes file to hdfs
        Returns:
            None
        """
        df = self.spark_session.createDataFrame(data=self.csv)
        df.write.csv("hdfs://localhost:9000/{}.csv".format(self.filename), header=True)

    def get_data(self, show=False):
        """
        Get dataframe from hdfs
        Args:
            show: whether to show collected table

        Returns:
            Dataframe object with collected data
        """
        def get_schema(csv):
            """
            Get schema for collected csv
            Args:
                csv: csv string file

            Returns:
                StructType with schemas
            """
            schemas = StructType()
            for field in build_table_schema(csv, index=False)["fields"]:
                type = StringType() if field["type"] == "string" \
                    else FloatType() if field["type"] == "number" \
                    else IntegerType()
                schemas.add(StructField(field["name"], type, True))
            return schemas

        data = self.spark_session.read.csv("hdfs://localhost:9000/{}.csv".format(self.filename),
                                           header=True, schema=get_schema(self.csv))
        if show:
            data.show(truncate=False)
        return data

    def min(self, case):
        """
        Get minimum for column
        Args:
            case: column name

        Returns:
            float min value
        """
        return self.data.select(F.min(case)).collect()[0][0]

    def max(self, case):
        """
        Get maximum for column
        Args:
            case: column name

        Returns:
            float max value
        """
        return self.data.select(F.max(case)).collect()[0][0]

    def mean(self, case):
        """
        Get mean for column
        Args:
            case: column name

        Returns:
            float mean value
        """
        return self.data.select(F.mean(case)).collect()[0][0]

    def std(self, case):
        """
        Get standard deviation for column
        Args:
            case: column name

        Returns:
            float standard deviation value
        """
        return self.data.select(F.stddev(case)).collect()[0][0]

    def var(self, case):
        """
        Get variation for column
        Args:
            case: column name

        Returns:
            float variation value
        """
        return self.data.select(F.variance(case)).collect()[0][0]

    def state_where(self, case, con):
        """
        Get state where the named case fulfills the condition
        Args:
            case: name of column
            con: condition

        Returns:
            string state
        """
        return self.data.filter(self.data[case] == con).select("state").collect()[0][0]

    def all_min(self):
        """
        Get all min values
        Returns:
            dict with all min values for every cases
        """
        return {case: self.min(case) for case in self.cases}

    def all_max(self):
        """
        Get all max values
        Returns:
            dict with all max values for every cases
        """
        return {case: round(self.max(case), 2) for case in self.cases}

    def all_mean(self):
        """
        Get all mean values
        Returns:
            dict with all mean values for every cases
        """
        return {case: round(self.mean(case), 2) for case in self.cases}

    def all_std(self):
        """
        Get all standard deviation values
        Returns:
            dict with all standard deviation values for every cases
        """
        return {case: round(self.std(case), 2) for case in self.cases}

    def all_var(self):
        """
        Get all variation values
        Returns:
            dict with all variation values for every cases
        """
        return {case: round(self.var(case), 2) for case in self.cases}

    def where_to_go(self):
        """
        Determines where to go, based on total tests, death and positive number of cases
        Returns:
            list with string name of the state and float death probability
        """
        def ratio(pos, total):
            return round(pos / total * 100, 4)

        # Filter states with data quality more than A
        filter_data = self.data.filter((self.data["dataQualityGrade"] == "A") |
                                       (self.data["dataQualityGrade"] == "A+")).select(*(self.cases + ["state"]))

        # Get ratio for every state
        data = {state: ratio(ratio(death, tests), ratio(death, pos))
                for pos, _, death, tests, state in filter_data.collect()}

        return list(sorted(data.items(), key=lambda item: item[1]))[0]

    def print_inf(self):
        """
        Print all collected information
        Returns:
            None
        """
        def print_mess(num, case, mess, state):
            """
            Prints message about min/max cases
            Args:
                num: number of min
                case: case name
                mess: message to print
                state: state name

            Returns:
                None
            """
            print("Number of {} {} cases: {} {}".format(mess, case, num, state))

        for case in self.cases:
            print("-----{}-----".format(case.upper()))
            print_mess(self.min_cases[case], "minimum", case, self.state_where(case, self.min_cases[case]))
            print_mess(self.max_cases[case], "maximum", case, self.state_where(case, self.max_cases[case]))
            print("Mean:", self.mean_cases[case])
            print("Standard deviation:", self.std_cases[case])
            print("Variance:", self.var_cases[case])
            print()

        print("Best place to go is: {} (probability of death: {}%)".format(self.where_state, self.where_prob))


if __name__ == "__main__":
    covid19 = Covid19USA()

    covid19.print_inf()
