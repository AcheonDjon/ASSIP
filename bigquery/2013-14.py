import os
from google.cloud import bigquery
import pandas as pd
import logging
import time
# Initialize the client
client = bigquery.Client()

# Define the query with a named parameter

query = """
SELECT repo_name_full AS GhRepo,
CAST(date1 AS DATE) AS date1,
actor_login AS actor_login,
COUNT(actor_login) AS num_activities,
COUNT(DISTINCT CASE WHEN type = 'PushEvent' THEN activity_id ELSE NULL END) AS num_dist_commits,
COUNT(DISTINCT CASE WHEN type = 'CommitCommentEvent' THEN activity_id ELSE NULL END) AS num_dist_commitcomments,
COUNT(DISTINCT CASE WHEN type = 'PushEvent' THEN actor_login ELSE NULL END) AS num_actors_pushevents,
COUNT(DISTINCT CASE WHEN type = 'CommitCommentEvent' THEN actor_login ELSE NULL END) AS num_actors_pusheventscomment,
COUNT(DISTINCT CASE WHEN type = 'PullRequestEvent' AND JSON_EXTRACT(payload, '$.action') = '"opened"' THEN JSON_EXTRACT(payload, '$.number') ELSE NULL END) AS num_dist_pullreqopened, 
COUNT(DISTINCT CASE WHEN type = 'PullRequestEvent' AND JSON_EXTRACT(payload, '$.action') = '"closed"' THEN JSON_EXTRACT(payload, '$.number') ELSE NULL END) AS num_dist_pullreqclosed,
COUNT(DISTINCT CASE WHEN type = 'PullRequestEvent' THEN JSON_EXTRACT(payload, '$.number') ELSE NULL END) AS num_dist_pullreqAll,
COUNT(DISTINCT CASE WHEN type = 'PullRequestReviewCommentEvent' THEN activity_id ELSE NULL END) AS num_dist_pullreqcomments,
COUNT(DISTINCT CASE WHEN type = 'PullRequestEvent' THEN actor_login ELSE NULL END) AS num_actors_pullreq,
COUNT(DISTINCT CASE WHEN type = 'PullRequestReviewCommentEvent' THEN actor_login ELSE NULL END) AS num_actors_pullreqcomment,
COUNT(DISTINCT CASE WHEN type = 'PullRequestEvent' AND JSON_EXTRACT(payload, '$.action') = '"opened"' THEN actor_login ELSE NULL END) AS num_actors_pullreq_opened,
COUNT(DISTINCT CASE WHEN type = 'PullRequestEvent' AND JSON_EXTRACT(payload, '$.action') = '"closed"' THEN actor_login ELSE NULL END) AS num_actors_pullreq_closed,
COUNT(DISTINCT CASE WHEN type = 'IssuesEvent' AND JSON_EXTRACT(payload, '$.action') = '"opened"' THEN activity_id ELSE NULL END) AS num_dist_issuesopened,
COUNT(DISTINCT CASE WHEN type = 'IssuesEvent' AND JSON_EXTRACT(payload, '$.action') = '"closed"' THEN activity_id ELSE NULL END) AS num_dist_issuesclosed,
COUNT(DISTINCT CASE WHEN type = 'IssuesEvent' THEN activity_id ELSE NULL END) AS num_dist_issuesAll,
COUNT(DISTINCT CASE WHEN type = 'IssueCommentEvent' THEN activity_id ELSE NULL END) AS num_dist_issuecomments,
COUNT(DISTINCT CASE WHEN type = 'IssuesEvent' THEN actor_login ELSE NULL END) AS num_actors_issues,
COUNT(DISTINCT CASE WHEN type = 'IssueCommentEvent' THEN actor_login ELSE NULL END) AS num_actors_issuescomment,
COUNT(DISTINCT actor_login ) AS num_actors_allevents,
COUNT(DISTINCT CASE WHEN type = 'IssuesEvent' AND JSON_EXTRACT(payload, '$.action') = '"opened"' THEN actor_login ELSE NULL END) AS num_actors_issues_opened,
COUNT(DISTINCT CASE WHEN type = 'IssuesEvent' AND JSON_EXTRACT(payload, '$.action') = '"closed"' THEN actor_login ELSE NULL END) AS num_actors_issues_closed,
COUNT(DISTINCT CASE WHEN type = 'ForkEvent' THEN activity_id ELSE NULL END) AS num_forks_event,
COUNT(DISTINCT CASE WHEN type = 'ForkEvent' THEN actor_login ELSE NULL END) AS num_actors_forks,
COUNT(DISTINCT CASE WHEN type = 'WatchEvent' THEN activity_id ELSE NULL END) AS num_watch_event,
COUNT(DISTINCT CASE WHEN type = 'WatchEvent' THEN actor_login ELSE NULL END) AS num_actors_watch,
COUNT(DISTINCT CASE WHEN type = 'ReleaseEvent' THEN activity_id ELSE NULL END) AS num_releases,
(CASE WHEN type = 'ReleaseEvent' THEN payload ELSE NULL END) AS release_payload
FROM (
SELECT 
ROW_NUMBER () OVER (ORDER BY created_at, repo.name, actor.login) AS activity_id,
repo.name AS repo_name,
created_at AS date1,
actor.login AS actor_login,
type,
payload,
SUBSTR(repo.url,20) as repo_name_full
FROM `githubarchive.month.*` WHERE _TABLE_SUFFIX BETWEEN @StartYear AND @EndYear
and repo.url LIKE (@company_url))
GROUP BY GhRepo, date1, actor_login, release_payload
ORDER BY GhRepo, date1, actor_login, release_payload;
"""
bitcoin = ['0xcregis', 'dethertech']
# # # List of years you want to query
# # years = [2015,2016,2017,2018,2019,2020, 2021,2022, 2023]
# # List of years you want to query
year_start = 2013
year_end = 2014

for b in bitcoin:
    # company_job_config = bigquery.QueryJobConfig(
    #         query_parameters = [
    #              bigquery.ScalarQueryParameter("company_name", "STRING", b + '/%'),
    #         ]
    #     )
    start_time = time.time()
    for year in range(year_start, year_end+1):
            year_job_config = bigquery.QueryJobConfig(
                 
                query_parameters=[
                    bigquery.ScalarQueryParameter("StartYear", "STRING", str(year) + '01'),
                    bigquery.ScalarQueryParameter("EndYear", "STRING", str(year) + '12'),
                    bigquery.ScalarQueryParameter("company_url", "STRING", "https://github.com/" + b + '/%'),
                ]
            )
            query_job = client.query(query, job_config=year_job_config)

            results = query_job.result()
            end_time = time.time()
            duration = end_time - start_time
            print("processing results for", b, year)

            if results.total_rows == 0:
                print(f"No data found for {b} in {year}")
                # Configure logger
                logging.basicConfig(filename='logfile.log', level=logging.INFO)
                logging.info(f"Processing results for {b} {year}")
            
                continue
            else :
                print(f"Found {results.total_rows} rows of data for {b} in {year}")
                logging.info(f"Found {results.total_rows} rows of data for {b} in {year}")

            #create directory if not exist
            if not os.path.exists(f"/Users/zebra/Documents/projects/ASSIP_Data/New_Data/{b}"):   
                os.makedirs(f"/Users/zebra/Documents/projects/ASSIP_Data/New_Data/{b}")    
                 

            # Define the CSV file path
            csv_file_path = f"/Users/zebra/Documents/projects/ASSIP_Data/New_Data/{b}/{b}_{year}_{int(duration)}.csv"

            df = results.to_dataframe()

            df.to_csv(csv_file_path, index=False)

            # # Export results to CSV
            # with open(csv_file_path, "w") as csv_file:
            # # Write headers
            #     csv_file.write(",".join([field.name for field in results.schema]) + "\n")
        
            # # Write data rows
            # for row in results:
            #     csv_file.write(",".join([str(value) for value in row]) + "\n")





