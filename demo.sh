#!/bin/bash

DATA="ventas_ficticias.csv"

# ================= FUNCTIONS =================

run_pig_query() {
    pig -x local <<EOF
$1
EOF
}

# ================= MAIN LOOP =================

while true
do
    clear
    echo "===================================="
    echo "     BIG DATA ANALYTICS PROJECT"
    echo "===================================="
    echo "1. Hadoop Operations"
    echo "2. Pig Data Analysis"
    echo "3. Exit"
    echo "===================================="

    read -p "Enter your choice: " main_choice

    case $main_choice in

# ================= HADOOP =================
    1)
        while true
        do
            echo ""
            echo "====== HADOOP OPERATIONS ======"
            echo "1. Start Hadoop"
            echo "2. Stop Hadoop"
            echo "3. Upload File to HDFS"
            echo "4. View HDFS Files"
            echo "5. Remove File from HDFS"
            echo "6. Back"

            read -p "Enter choice: " h

            case $h in
                1) start-dfs.sh; start-yarn.sh ;;
                2) stop-dfs.sh; stop-yarn.sh ;;
                3)
                    read -p "Local file: " lf
                    read -p "HDFS path: " hf
                    hdfs dfs -put $lf $hf
                    ;;
                4) hdfs dfs -ls / ;;
                5)
                    read -p "File to delete: " df
                    hdfs dfs -rm $df
                    ;;
                6) break ;;
                *) echo "Invalid choice" ;;
            esac
        done
        ;;

# ================= PIG =================
    2)
        while true
        do
            echo ""
            echo "====== PIG DATA ANALYSIS ======"
            echo "1. Sample Data"
            echo "2. Count Records"
            echo "3. Total Revenue"
            echo "4. Top Products"
            echo "5. Sales by Category"
            echo "6. Avg Price per Category"
            echo "7. High Value Transactions"
            echo "8. Distinct Customers"
            echo "9. Sort by Price"
            echo "10. Items per Customer"
            echo "11. Back"

            read -p "Enter choice: " p

            case $p in

                1)
run_pig_query "
data = LOAD '$DATA' USING PigStorage(',') 
AS (Order_ID:int, Customer_ID:int, Store_ID:int, Date:chararray, 
Product_Category:chararray, Price:float, Quantity:int, Total_Sales:float);
DUMP data;
"
                ;;

                2)
run_pig_query "
data = LOAD '$DATA' USING PigStorage(',') 
AS (Order_ID:int, Customer_ID:int, Store_ID:int, Date:chararray, 
Product_Category:chararray, Price:float, Quantity:int, Total_Sales:float);
cnt = FOREACH (GROUP data ALL) GENERATE COUNT(data);
DUMP cnt;
"
                ;;

                3)
run_pig_query "
data = LOAD '$DATA' USING PigStorage(',') 
AS (Order_ID:int, Customer_ID:int, Store_ID:int, Date:chararray, 
Product_Category:chararray, Price:float, Quantity:int, Total_Sales:float);
total = FOREACH (GROUP data ALL) GENERATE SUM(data.Total_Sales);
DUMP total;
"
                ;;

                4)
run_pig_query "
data = LOAD '$DATA' USING PigStorage(',') 
AS (Order_ID:int, Customer_ID:int, Store_ID:int, Date:chararray, 
Product_Category:chararray, Price:float, Quantity:int, Total_Sales:float);
grp = GROUP data BY Product_Category;
sumq = FOREACH grp GENERATE group, SUM(data.Quantity) AS total;
sorted = ORDER sumq BY total DESC;
top = LIMIT sorted 5;
DUMP sorted;
"
                ;;

                5)
run_pig_query "
data = LOAD '$DATA' USING PigStorage(',') 
AS (Order_ID:int, Customer_ID:int, Store_ID:int, Date:chararray, 
Product_Category:chararray, Price:float, Quantity:int, Total_Sales:float);
grp = GROUP data BY Product_Category;
res = FOREACH grp GENERATE group, SUM(data.Total_Sales);
DUMP res;
"
                ;;

                6)
run_pig_query "
data = LOAD '$DATA' USING PigStorage(',') 
AS (Order_ID:int, Customer_ID:int, Store_ID:int, Date:chararray, 
Product_Category:chararray, Price:float, Quantity:int, Total_Sales:float);
grp = GROUP data BY Product_Category;
res = FOREACH grp GENERATE group, AVG(data.Price);
DUMP res;
"
                ;;

                7)
run_pig_query "
data = LOAD '$DATA' USING PigStorage(',') 
AS (Order_ID:int, Customer_ID:int, Store_ID:int, Date:chararray, 
Product_Category:chararray, Price:float, Quantity:int, Total_Sales:float);
res = FILTER data BY Total_Sales > 1000;
DUMP res;
"
                ;;

                8)
run_pig_query "
data = LOAD '$DATA' USING PigStorage(',') 
AS (Order_ID:int, Customer_ID:int, Store_ID:int, Date:chararray, 
Product_Category:chararray, Price:float, Quantity:int, Total_Sales:float);
grp = GROUP data BY Customer_ID;
res = FOREACH grp GENERATE group;
DUMP res;
"
                ;;

                9)
run_pig_query "
data = LOAD '$DATA' USING PigStorage(',') 
AS (Order_ID:int, Customer_ID:int, Store_ID:int, Date:chararray, 
Product_Category:chararray, Price:float, Quantity:int, Total_Sales:float);
sorted = ORDER data BY Price DESC;
DUMP sorted;
"
                ;;

                10)
run_pig_query "
data = LOAD '$DATA' USING PigStorage(',') 
AS (Order_ID:int, Customer_ID:int, Store_ID:int, Date:chararray, 
Product_Category:chararray, Price:float, Quantity:int, Total_Sales:float);
grp = GROUP data BY Customer_ID;
res = FOREACH grp GENERATE group, SUM(data.Quantity);
DUMP res;
"
                ;;

                11) break ;;
                *) echo "Invalid choice" ;;
            esac
        done
        ;;

# ================= EXIT =================
    3)
        echo "Exiting..."
        exit
        ;;

    *)
        echo "Invalid choice"
        ;;
    esac

done
