### Venmo transaction graph and rolling median

(1) ./src/median_degree.java : main java program

(2) "venmo_input/Out of order-6-2-two fifths chance" : input test case up to 640k transactions
                        Roughly there are 760 payments in every 60-sec window.

number of total payments and runtime in ms --
* 1k :    233 ms
* 2k :    374 ms
* 5k :    429 ms
* 10k :   641 ms
* 20k :   922 ms
* 40k :  1312 ms
* 80k :  1814 ms
* 160k : 2697 ms
* 320k : 4173 ms
* 640k : 6263 ms

Runtime scales up close to linear.
This program takes O(N*(log K)) time, where N is number of total payment transactions and
                                            K is max number of payments in every 60-sec window.
                                            
(3) ./src/GenerateTestCase.java : to generate test cases by random variable
       To create and be able to use various types of test cases,
       we can specify number of total transactions, number of users, and
       average number of payments in every 60-sec window.
       It can also move timestamps of payments forward and backward to make some payments
       out of order.
