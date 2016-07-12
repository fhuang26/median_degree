Venmo transaction graph and rolling median			Felix Huang

(1) ./src/median_degree.java : main java program

(2) "venmo_input/Out of order-6-2-two fifths chance" : input test case up to 640k transactions
                        Roughly there are 760 payments in every 60-sec window.

# of total payments and runtime in ms
1k :    233
2k :    374
5k :    429
10k :   641
20k :   922
40k :  1312
80k :  1814
160k : 2697
320k : 4173
640k : 6263

Runtime scales up close to linear.
This program takes O(N*(log K)) time, where N is number of total payment transactions and
                                            K is max # of payments in every 60-sec window.
                                            
(3) ./src/GenerateTestCase.java : to generate test cases by random variable
       To create and be able to use various types of test cases,
       we can specify number of total transactions, number of users, and
       average number of payments in every 60-sec window.
       It can also move timestamps of payments forward and backward to make some payments
       out of order.
