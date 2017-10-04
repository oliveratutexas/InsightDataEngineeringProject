# LINK JOIN!

## Slides

### [Link to my slides](https://docs.google.com/presentation/d/1KdAnZx_1cPQSH-Cb50H46OR3RDVuO6AvGmev0U1t_M0/edit#slide=id.gc6f9e470d_0_0)

## Goal
An advertiser, data-scientist, politician, or casual user might find the feature of knowing who goes into one subreddit from another useful for the purpose of targeted marketing, insights, knowing how which reddit users feel about which political ideologies, or straightup 'cool' factor. My project was joining all the conversations that happen across reddit that lead to other subreddits. For instance, if there were two conversations that ended in direct recommendations to /r/politics, I'd put them in a graph together where both conversations point to the reference to /r/politics.

## Implementation
I downloaded and extracted the files [provided by a friendly reddit user](https://files.pushshift.io/reddit/comments/) onto S3 as my source of truth. Then I [processed the files into the joined graphs](https://github.com/oliveratutexas/LinkJoin/blob/master/src/spark_scripts/spark_run.py) pumped them into Redis, got em out and [made them into svgs](https://github.com/oliveratutexas/LinkJoin/blob/master/src/frontend/flask/app.py). 





