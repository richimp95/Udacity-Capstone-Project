import configparser
import os
import logging
import glob
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType
from pyspark.sql.functions import udf, col, lit, year, month, upper, to_date, size
from pyspark.sql.functions import monotonically_increasing_id
pd.options.mode.chained_assignment = None 

# setup logging 
logger = logging.getLogger()
logger.setLevel(logging.INFO)

files = glob.glob("../../data/18-83510-I94-Data-2016/*.sas7bdat")

def create_spark_session():
    
    """
    Description: This function creates the Spark Session that will be used to process all the Sparkify data. 
    Arguments:
        None
    Returns:
        Spark Session
    """
    
    spark = SparkSession.builder.\
        config("spark.jars.repositories", "https://repos.spark-packages.org/").\
        config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11").\
        enableHiveSupport().getOrCreate()
    return spark

def to_date_sas(date):
    
    """UDF function that transforms the sas files date columns to timestamp
    
        Arguments:
            date: pandas column
        Returns:
            Pandas column as type Timestamp with correct date.
    """
    
    if date is not None:
        return pd.to_timedelta(date, unit='d') + pd.Timestamp('1960-1-1')
    
to_date_udf = udf(to_date_sas, DateType())

def process_immigration_data(spark, input_data):
    
    """ Process immigration data to get the fact table fact_immig and the dimensional tables dim_immig_info and dim_immig_airline.
    
        Arguments:
            spark: Spark Session.
            input_data: data files
        Returns:
            None
    """
    
    logging.info("Processing immigration data")
    
    flag = True
    
    for file in input_data:
        if flag:
            df_temp = spark.read.format('com.github.saurfang.sas.spark').load(file)
            df_spark = spark.read.format('com.github.saurfang.sas.spark').load(file)
        else:
            df_spark = df_spark.union(df_temp)

    df_spark = spark.read.parquet("sas_data")

    fact_immig = df_spark.select('cicid', 'i94yr', 'i94mon', 'i94port', 'i94addr', 'arrdate', 'depdate', 'i94mode')\
                         .distinct().withColumn("immigration_id", monotonically_increasing_id())

    columns = ['cic_id', 'year', 'month', 'city_code', 'state_code', 'arrive_date', 'departure_date', 'mode']

    for original, new in zip(fact_immig.columns, columns):
        fact_immig = fact_immig.withColumnRenamed(original, new)

    fact_immig = fact_immig.withColumn('country', lit('United States'))
    fact_immig = fact_immig.withColumn('arrive_date', to_date_udf(col('arrive_date')))
    fact_immig = fact_immig.withColumn('departure_date', to_date_udf(col('departure_date')))
    
    logging.info("Start processing dim_immig_info")
    
    # Select the specific columns for the dimensional table.
    dim_immig_info = df_spark.select('cicid', 'i94cit', 'i94res', 'biryear', 'gender', 'i94visa')\
                       .distinct().withColumn("dim_immig_info", monotonically_increasing_id())
    
    # Change the column names.
    columns = ['cic_id', 'citizen_country', 'residence_country', 'birth_year', 'gender', 'visa']
    
    for original, new in zip(dim_immig_info.columns, columns):
        dim_immig_info = dim_immig_info.withColumnRenamed(original, new)

    logging.info("Processing dim_immig_airline")
    
    # Select the specific columns for the dimensional table.
    dim_immig_airline = df_spark.select('cicid', 'airline', 'admnum', 'fltno', 'visatype').distinct()\
                         .withColumn("immi_airline_id", monotonically_increasing_id())
    
    # Change the column names.
    columns = ['cic_id', 'airline', 'admin_num', 'flight_number', 'visa_type']
    
    for original, new in zip(dim_immig_airline.columns, columns):
        dim_immig_airline = dim_immig_airline.withColumnRenamed(original, new)
        
def process_labels_data(spark):
    
    """ Proccessing the labels file to get the country, city, state codes.
        Arguments:
            spark: Spark Session.
        Returns:
            None
    """

    logging.info("Processing label data")
    
    # Reading File
    label_file = os.path.join("I94_SAS_Labels_Descriptions.SAS")
    with open(label_file) as f:
        contents = f.readlines()

    country_code = {}
    for countries in contents[10:298]:
        pair = countries.split('=')
        code, country = pair[0].strip(), pair[1].strip().strip("'")
        country_code[code] = country
    
    # Creating Spark dataframe and writing the dimensional table for countries as parquet
    #spark.createDataFrame(country_code.items(), ['country_code', 'country']).write.mode("overwrite").parquet(path=output_data + 'dim_country_code')

    
    # Parsing the city codes
    city_code = {}
    for cities in contents[303:962]:
        pair = cities.split('=')
        code, city = pair[0].strip("\t").strip().strip("'"),\
                     pair[1].strip('\t').strip().strip("''")
        city_code[code] = city
        
    # Creating Spark dataframe for the city codes
    df_city_code = pd.DataFrame(list(city_code.items()), columns=['city_code', 'city'])
    df_city_code.city = df_city_code.city.str.split(',', expand = True)[0]
    
    # Writing the dimensional table for cities as parquet
    #df_city_code.write.mode("overwrite").parquet(path=output_data + 'dim_city_code')

    
    # Parsing the state codes
    state_code = {}
    for states in contents[982:1036]:
        pair = states.split('=')
        code, state = pair[0].strip('\t').strip("'"), pair[1].strip().strip("'")
        state_code[code] = state
        
     # Creating Spark dataframe and writing the dimensional table for states as parquet
    #spark.createDataFrame(state_code.items(), ['state_code', 'state']).write.mode("overwrite").parquet(path=output_data + 'dim_state_code')


def process_airport_data(spark):
    
    """ Process airport data csv to get the dimensional dim_us_airport table
        Arguments:
            spark: Spark Session.
        Returns:
            None
    """

    logging.info("Processing dim_us_airport")
    
    # Reading File
    us_airport = os.path.join('airport-codes_csv.csv')
    df = spark.read.csv(us_airport, header=True)

    # Select the specific columns for the dimensional table.
    df = df.where(df['iso_country'] == 'US')
    dim_us_airport = df.select(['ident', 'name', 'elevation_ft', 'iso_region', 'municipality', 'coordinates']).distinct()

    # Change the column names.
    columns = ['airline_id', 'description', 'elev_ft', 'iso_2_region', 'municipality', 'coordinates']
    
    for original, new in zip(dim_us_airport.columns, columns):
        dim_us_airport = dim_us_airport.withColumnRenamed(original, new)
 
    # Writing the dimensional table for airports as parquet
    #dim_us_airport.write.mode("overwrite").parquet(path=output_data + 'dim_us_airport')


def process_population_data(spark):
    """ Process demograpy data to get dim_us_cities_pop table
        Arguments:
            spark: Spark Session.
        Returns:
            None
    """

    logging.info("Processing dim_us_cities_pop")
    
    # Reading File
    pop_data = os.path.join('us-cities-demographics.csv')
    df = spark.read.format('csv').options(header=True, delimiter=';').load(pop_data)

    # Select the specific columns for the dimensional table.
    dim_us_cities_pop = df.select(['State Code', 'City', 'State', 'Male Population', 'Female Population', 'Total Population', 'Foreign-born', 'Race', 'Count'])\
                          .distinct().withColumn("pop_id", monotonically_increasing_id())

    # Change the column names.
    columns = ['state_code', 'city', 'state', 'male_pop', 'fem_pop', 'tot_pop', 'foreign_born', 'race', 'cout']
    
    for original, new in zip(dim_us_cities_pop.columns, columns):
        dim_us_cities_pop = dim_us_cities_pop.withColumnRenamed(original, new)

    # Writing the dimensional table for cities population as parquet
    #dim_us_cities_pop.write.mode("overwrite").parquet(path=output_data + 'dim_us_cities_pop')

    
def main():
    
    """
    Description: This is the main function where everything is executed. 
    Arguments:
        None
    Returns:
        None
    """
    
    spark = create_spark_session()
    input_data = files
    #output_data = "s3a://udacity-rmp-capstone/"
    
    process_immigration_data(spark, input_data)    
    process_labels_data(spark)
    process_airport_data(spark)
    process_population_data(spark)
    
    logging.info("The process has successfully ended")


if __name__ == "__main__":
    main()    
    