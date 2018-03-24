from dse.cluster import Cluster
from dse.cluster import ConsistencyLevel
from cassandra.query import SimpleStatement
import sys
def main():
    cluster = Cluster(['34.213.255.215'])
    session = cluster.connect()

    # Insert 50,000 rows into Cassandra for this test
    if sys.argv[1] == "write":
        for i in range(0, 50000):
            session.execute("INSERT INTO test.paging_test (last_id, description) VALUES (%s, %s)",(i, str(i)))
    # Pass in the read argument to manually scan the token range
    # 50 tokens at a time up until the limit of 40,000 records
    elif sys.argv[1] == "read":
        # Opens the results csv
        r_file = open("results.csv", "w")
        # Loop for N number of passes, this records the range N times and allows you to run decommissions/bootstrapping
        for i in range(0, 40):
            # Recursive function which starts at the beginning of the token range and scans the token range until
            # 40,000 records have been read.
            results = recursive_page(40000, 0, None, session, [])
            # Once 40,000 records have been collected, write them to results.csv
            for result_set in results:
                for row in result_set:
                    r_file.write(str(row[0]) + ",")
            r_file.write("\n")
            print("Finished Pass " + str(i))
            # Flush the line and loop until the total number of passes has been completed
            r_file.flush()
        r_file.close()
    # Pass in the read_driver_paging argument to utilize the DSE Driver paging which will iteratively pull back
    # the total number of records requested until 40,000
    elif sys.argv[1] == "read_driver_paging":
        # Open results_driver_paging csv
        r_file = open("results_driver_paging.csv", "w")
        for i in range(0, 500):
            # Issue a large query, in this case grab 40,000 rows from Cassandra by having it scan the token range
            statement = SimpleStatement("SELECT * FROM test.paging_test LIMIT 40000",
                                        consistency_level=ConsistencyLevel.QUORUM)
            results = session.execute(statement)
            # While the driver has more pages, continue to pull the records from Cassandra and write them to the file
            while results.has_more_pages:
                for row in results.current_rows:
                    r_file.write(str(row[0]) + ",")
                results.fetch_next_page()
            r_file.write("\n")
            print("Finished Pass " + str(i))
            # Flush the row and loop until the maximum of tries has been exceeded.
            r_file.flush()


        r_file.close()

    # Pass in the validate argument and this will read through the CSV generated, comparing line 0 to line N.
    # This function is designed to check for inconsistencies in the scans.
    elif sys.argv[1] == "validate":
        r_file = open(sys.argv[2], "r")
        lines = r_file.readlines()

        for i in range(1, len(lines)):
            print("Line " + str(i))

            for j in range(0, len(lines[0])):
                # If line 0 element j differs from line N element j, throw a token fault and jump to the next line
                if lines[0][j] != lines[i][j]:
                    print("Token Fault Line 0 and Line " + str(i))
                    break

        r_file.close()

def recursive_page(limit, current, last_id, session, finalA):
    resultsA = None
    # If you are at token range 0, grab the first 50
    if current == 0:
        statement = SimpleStatement("SELECT * FROM test.paging_test LIMIT 50", consistency_level=ConsistencyLevel.QUORUM)
        rowsA = session.execute_async(statement)
    # If you are not at token range 0, but less than the limit, fetch the next 50
    elif current < limit:
        statement = SimpleStatement("SELECT * FROM test.paging_test WHERE token(id) >= token(%s) LIMIT 50", consistency_level=ConsistencyLevel.QUORUM)
        rowsA = session.execute_async(statement, (last_id,))
    # Else if you are at the limit, return the collected list
    else:
        return finalA
    # Append the requested rows to the final request array
    resultsA = rowsA.result().current_rows

    last_id = resultsA[len(resultsA) - 1][0]
    finalA.append(resultsA)
    # Recursively iterate the token range
    return recursive_page(limit, current + 50, last_id, session, finalA)

if __name__ == "__main__":
    main()