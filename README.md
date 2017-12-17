Q1
Write a MapReduce program in Hadoop that implements a simple
“Mutual/Common friend list of two friends". The key idea is that if two people
are friend then they have a lot of mutual/common friends. This question will give
any two Users as input, output the list of the user id of their mutual friends.
For example,
Alice’s friends are Bob, Sam, Sara, Nancy
Bob’s friends are Alice, Sam, Clara, Nancy
Sara’s friends are Alice, Sam, Clara, Nancy
As Alice and Bob are friend and so, their mutual friend list is [Sam, Nancy]
As Sara and Bob are not friend and so, their mutual friend list is empty. (In this
case you may exclude them from your output).

Q2.
Please answer this question by using dataset from Q1.
Find friend pairs whose common friend number are within the top-10 in all the pairs. Please
output them in decreasing order.
Output Format:
<User_A>, <User_B><TAB><Mutual/Common Friend Number>

Q3.
List the business_id, full address and categories of the Top 10 businesses using the
average ratings.
This will require you to use review.csv and business.csv files.
Please use reduce side join and job chaining technique to answer this problem.
Sample output:
business id full address categories avg rating
xdf12344444444, CA 91711 List['Local Services', 'Carpet Cleaning'] 5.0

Q4.
Use Yelp Dataset
List the 'user id' and 'rating' of users that reviewed businesses located in “Palo
Alto”
Required files are 'business' and 'review'.
Please use In Memory Join technique to answer this problem.
