Movie Ratings Analysis with PySpark
This project leverages PySpark to analyze movie ratings data and perform various tasks to derive meaningful insights. The dataset includes user movie ratings along with information such as UserID, MovieID, Rating, and additional metadata like AgeGroup, SubscriptionStatus, WatchTime, and IsBingeWatched.

Task Breakdown
Binge-Watching Patterns: Determining the percentage of users in each age group who binge-watch movies.
Churn Risk Users: Identifying users at risk of churn by filtering those with canceled subscriptions and low watch time.
Movie Watching Trends: Analyzing movie watching patterns over the years to identify peak viewing years.
Prerequisites
Make sure the following dependencies are installed before running the code:

PySpark (version 3.x or higher)
Python (version 3.6 or higher)
To install PySpark, run:

 ⁠bash pip install pyspark

Input Data
The project uses a CSV file, movie_ratings_data.csv, with the following columns:

UserID: Unique identifier for users
MovieID: Unique identifier for movies
MovieTitle: Title of the movie
Genre: Genre of the movie
Rating: Rating given by the user
ReviewCount: Number of reviews for the movie
WatchedYear: Year when the movie was watched
UserLocation: User's location
AgeGroup: Age group of the user
StreamingPlatform: Platform used for streaming
WatchTime: Time in minutes spent watching the movie
IsBingeWatched: Indicator of whether the movie was binge-watched
SubscriptionStatus: User's subscription status (e.g., active, canceled)
Output
The results of each task will be saved in the Outputs directory as CSV files:

binge_watching_patterns.csv: Binge-watching statistics by age group.
churn_risk_users.csv: Count of users at risk of churn.
movie_watching_trends.csv: Movie-watching trends over the years.
Task 1: Binge-Watching Patterns Analysis
Overview
This task identifies binge-watching trends by calculating the percentage of users in each age group who binge-watch movies.

Steps
Filters users who binge-watch movies.
Groups users by AgeGroup.
Computes the percentage of binge-watchers in each age group.
Code Example
python
def detect_binge_watching_patterns(df):
    binge_watchers_df = df.filter(col("IsBingeWatched") == True)
    binge_watchers_count_df = binge_watchers_df.groupBy("AgeGroup").agg(count("UserID").alias("BingeWatchers"))
    total_users_count_df = df.groupBy("AgeGroup").agg(count("UserID").alias("TotalUsers"))
    joined_df = binge_watchers_count_df.join(total_users_count_df, on="AgeGroup")
    result_df = joined_df.withColumn("BingeWatchPercentage", spark_round((col("BingeWatchers") / col("TotalUsers")) * 100, 2))
    result_df = result_df.select("AgeGroup", "BingeWatchers", "BingeWatchPercentage")
    return result_df
Output
A CSV file with binge-watching patterns: binge_watching_patterns.csv
Task 2: Churn Risk Users
Overview
This task identifies users who are at risk of churn by filtering for users with canceled subscriptions and low watch time (less than 100 minutes).

Steps
Filters users with SubscriptionStatus as 'Canceled' and WatchTime under 100 minutes.
Counts the number of such users.
Code Example
python
def identify_churn_risk_users(df):
    churn_risk_df = df.filter((col("SubscriptionStatus") == "Canceled") & (col("WatchTime") < 100))
    churn_risk_count = churn_risk_df.count()
    result_df = df.sparkSession.createDataFrame([("Users with low watch time & canceled subscriptions", churn_risk_count)], ["Churn Risk Users", "Total Users"])
    return result_df
Output
A CSV file with churn risk data: churn_risk_users.csv
Task 3: Movie Watching Trends
Overview
This task analyzes movie-watching trends over time, highlighting peak years of movie-watching activity.

Steps
Groups data by WatchedYear and counts the number of movies watched each year.
Orders the results by WatchedYear.
Code Example
python
def analyze_movie_watching_trends(df):
    trend_df = df.groupBy("WatchedYear").agg(count("MovieID").alias("Movies Watched"))
    result_df = trend_df.orderBy("WatchedYear")
    return result_df
Output
A CSV file with trends in movie-watching: movie_watching_trends.csv
Running the Code
Place the movie_ratings_data.csv file in the correct directory (e.g., /input).
Execute each task by running the corresponding script (task1.py, task2.py, or task3.py).
The results will be saved in the Outputs folder.
Example command to run Task 1:

 ⁠bash python task1.py

Directory Structure
/project_root │ ├── /input │ └── movie_ratings_data.csv │ ├── /Outputs │ ├── binge_watching_patterns.csv │ ├── churn_risk_users.csv │ └── movie_watching_trends.csv │ └── task1.py └── task2.py └── task3.py

Conclusion
This project showcases how to analyze movie ratings data using PySpark. By applying various data transformations and aggregations, valuable insights are generated on binge-watching behavior, churn risk, and movie-watching trends over time.