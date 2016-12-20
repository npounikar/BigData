To run each problem, Run the following .scala files from spark-shell one by one with respect to their questions :
Quesiton1  : Q1_MutualFriends.scala
Quesiton2  : Q2_MutualFriends.scala
Quesiton3  : Q3_MutualFriends.scala
Quesiton4  : Q4_MutualFriends.scala
Quesiton5  : Q5_Bigram.scala


Quesiton1  : Q1_MutualFriends.scala
-------------------------------------------------------------------------------------------------------------------------------------
Open the "Q1_MutualFriends.scala" in notepad, and run every line one by one
Run each line in scala to check output. 
For HW2_Q1 we can run the program in yarn environment. 
Command to Run Yarn => spark-shell --master yarn-client --executor-memory 2G --executor-cores 6 --num-executors 6

Output Sample : Mutual Friends for the users with following user IDs:

0,1	5, 20
20,28193	1 
1, 29826	
6222,19272	19263, 19280, 19281, 19282
28041,28056	6245, 28054, 28061



Quesiton2  : Q2_MutualFriends.scala
Quesiton3  : Q3_MutualFriends.scala
-------------------------------------------------------------------------------------------------------------------------------------
Take the user input for UserA and UserB and then run remaining file for the output.
Command To Run (For Question2):
spark-shell i Q2_MutualFriends.scala

Command To Run (For Question3):
spark-shell i Q3_MutualFriends.scala



Quesiton4  : Q4_MutualFriends.scala
-------------------------------------------------------------------------------------------------------------------------------------
Command To Run (For Question4):
spark-shell i Q4_MutualFriends.scala



Quesiton5  : Q5_Bigram.scala
-------------------------------------------------------------------------------------------------------------------------------------
Command To Run (For Question5):
spark-shell i Q5_Bigram.scala




Note : If you have any other path to input files, please open the .scala files in notepad and mention the input absolute path accordingly.
