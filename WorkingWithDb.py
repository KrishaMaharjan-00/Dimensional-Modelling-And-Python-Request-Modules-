import pymysql

#database connection
connection = pymysql.connect(host="localhost", user="root", passwd="", database="mysql_learners")
cursor = connection.cursor()

# query for problem1
query1 = "select count(*) from chicago_public_schools where `Elementary, Middle, or High School` ='ES'";

#executing the problem1 query
cursor.execute(query1)
result1 = cursor.fetchall()
print(f"Result1: %s" % (result1,))

# query for problem2
query2 = "select MAX(Safety_Score) from chicago_public_schools";

#executing the problem2 query
cursor.execute(query2)
result2 = cursor.fetchall()
print(f"Result2: %s" % (result2,))

# query for problem3
query3 = "select Name_of_School, Safety_Score from chicago_public_schools where Safety_Score = 99";

#executing the problem3 query
cursor.execute(query3)
result3 = cursor.fetchall()
print(f"Result3: %s" % (result3,))

# query for problem4
query4 = "select Name_of_School, Average_Student_Attendance from chicago_public_schools order by Average_Student_Attendance desc limit 10";

#executing the problem4 query
cursor.execute(query4)
result4 = cursor.fetchall()
print(f"Result4: %s" % (result4,))

# query for problem5
query5 = "SELECT Name_of_School, Average_Student_Attendance from chicago_public_schools order by Average_Student_Attendance LIMIT 5";

#executing the problem5 query
cursor.execute(query5)
result5 = cursor.fetchall()
print(f"Result5: %s" % (result5,))

# query for problem6
query6 = "SELECT Name_of_School, REPLACE(Average_Student_Attendance, '%', '') from chicago_public_schools order by Average_Student_Attendance LIMIT 5";

#executing the problem6 query
cursor.execute(query6)
result6 = cursor.fetchall()
print(f"Result1: %s" % (result6,))

# query for problem7
query7 = "SELECT Name_of_School, Average_Student_Attendance from chicago_public_schools where CAST(REPLACE(Average_Student_Attendance, '%', '')  AS DOUBLE) < 70 order by Average_Student_Attendance";

#executing the problem7 query
cursor.execute(query7)
result7 = cursor.fetchall()
print(f"Result7: %s" % (result7,))

# query for problem8
query8 = "select Community_Area_Name, sum(College_Enrollment) AS TOTAL_ENROLLMENT from chicago_public_schools group by Community_Area_Name";

#executing the problem8 query
cursor.execute(query8)
result8 = cursor.fetchall()
print(f"Result8: %s" % (result8,))

# query for problem9
query9 = "select Community_Area_Name, sum(College_Enrollment) AS TOTAL_ENROLLMENT from chicago_public_schools group by Community_Area_Name order by TOTAL_ENROLLMENT asc LIMIT 5";

#executing the problem9 query
cursor.execute(query9)
result9 = cursor.fetchall()
print(f"Result9: %s" % (result9,))

# query for problem10
query10 = "SELECT name_of_school, safety_score FROM chicago_public_schools ORDER BY safety_score LIMIT 5";

#executing the problem10 query
cursor.execute(query10)
result10 = cursor.fetchall()
print(f"Result10: %s" % (result10,))

# query for problem11
query11 = "select hardship_index from chicago_socioeconomic_data CD, chicago_public_schools CPS where CD.community_area_number = CPS.community_area_number and college_enrollment = 4368";

#executing the problem9 query
cursor.execute(query11)
result11 = cursor.fetchall()
print(f"Result11: %s" % (result11,))

# query for problem12
query12 = "select community_area_number, community_area_name, hardship_index from chicago_socioeconomic_data where community_area_number in ( select community_area_number from chicago_public_schools where COLLEGE_ENROLLMENT = (select MAX(COLLEGE_ENROLLMENT) from chicago_public_schools)  ) ";

#executing the problem12 query
cursor.execute(query12)
result12 = cursor.fetchall()
print(f"Result12: %s" % (result12,))


#commiting the connection then closing it.
connection.commit()
connection.close()