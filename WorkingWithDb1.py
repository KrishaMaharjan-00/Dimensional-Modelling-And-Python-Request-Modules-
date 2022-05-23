import pymysql
from pymysql.constants import CLIENT

def create_db_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = pymysql.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name,
            client_flag=CLIENT.MULTI_STATEMENTS
        )
    except Exception as err:
        print(f"Error: '{err}'")

    return connection

def execute_query(connection):
    try:
        query = "select count(*) from chicago_public_schools where `Elementary, Middle, or High School` ='ES'; " \
                "select MAX(Safety_Score) from chicago_public_schools;" \
                "select Name_of_School, Safety_Score from chicago_public_schools where Safety_Score = 99;" \
                "select Name_of_School, Average_Student_Attendance from chicago_public_schools order by Average_Student_Attendance desc limit 10;" \
                "SELECT Name_of_School, Average_Student_Attendance from chicago_public_schools order by Average_Student_Attendance LIMIT 5;" \
                "SELECT Name_of_School, REPLACE(Average_Student_Attendance, '%', '') from chicago_public_schools order by Average_Student_Attendance LIMIT 5;" \
                "SELECT Name_of_School, Average_Student_Attendance from chicago_public_schools where CAST(REPLACE(Average_Student_Attendance, '%', '')  AS DOUBLE) < 70 order by Average_Student_Attendance;" \
                "select Community_Area_Name, sum(College_Enrollment) AS TOTAL_ENROLLMENT from chicago_public_schools group by Community_Area_Name;" \
                "select Community_Area_Name, sum(College_Enrollment) AS TOTAL_ENROLLMENT from chicago_public_schools group by Community_Area_Name order by TOTAL_ENROLLMENT asc LIMIT 5;" \
                "SELECT name_of_school, safety_score FROM chicago_public_schools ORDER BY safety_score LIMIT 5;" \
                "select hardship_index from chicago_socioeconomic_data CD, chicago_public_schools CPS where CD.community_area_number = CPS.community_area_number and college_enrollment = 4368;" \
                "select community_area_number, community_area_name, hardship_index from chicago_socioeconomic_data where community_area_number in ( select community_area_number from chicago_public_schools where COLLEGE_ENROLLMENT = (select MAX(COLLEGE_ENROLLMENT) from chicago_public_schools)  );"
        cursor.execute(query)
        connection.commit()
    except Exception as err:
        print(f"Error: '{err}'")

def print_result(connection):
    try:
        result = cursor.fetchall()
        for rows in result:
            print(rows)
    except Exception as err:
        print(f"Error: '{err}'")

if __name__ == "__main__":
    connection = create_db_connection('localhost', 'root', '', 'mysql_learners')
    cursor = connection.cursor()
    execute_query(connection)
    print_result(connection)