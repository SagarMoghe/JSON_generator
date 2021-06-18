from dbHandler import dbHandler
import numpy as np
import pandas as pd
import json


class coreLogic():

    def __init__(self, courses: str, student: str, tests: str, marks: str, output: str):
        self.db = dbHandler()
        self.db.createConnection('DataBase.db')
        self.response = None
        self.courses = courses
        self.student = student
        self.tests = tests
        self.marks = marks
        self.output = output

    def start(self):
        self.populateTable()
        self.generateMergedTable()
        if self.weightCheck():
            self.generateCourseAverage()
            mergedDF = self.db.convertTableIntoPandasDataFrame('addCourseAverage').drop(['test_id', 'mark', 'weight'],
                                                                                        axis=1).drop_duplicates()
            mergedDF['totalAverage'] = mergedDF.groupby('student_id', as_index=True).courseAverage.transform(
                np.average).round(2)

            transformed = self.generateJSON(mergedDF)
            self.response = "[{\"students\":" + transformed + "}]"
        else:
            self.response = "[{\"errors\":\"Invalid course weights\"}]"

        self.returnResponse(self.response)

    def populateTable(self):
        self.db.createTableFromCsv("courses", "(id int PRIMARY KEY, name text, teacher text)", self.courses)
        self.db.fix_table("courses", "id", "course_id")
        self.db.fix_table("courses", "name", "course_name")

        self.db.createTableFromCsv("marks", "(test_id int , student_id int, mark int, PRIMARY KEY(test_id,student_id))",
                                   self.marks)

        self.db.createTableFromCsv("students", "(id int PRIMARY KEY, name text)", self.student)
        self.db.fix_table("students", "id", "student_id")
        self.db.fix_table("students", "name", "student_name")

        self.db.createTableFromCsv("tests", "(id int, name text)", self.tests)
        self.db.fix_table("tests", "id", "test_id")

    def generateMergedTable(self):
        query1 = """DROP TABLE IF EXISTS 'merged'"""
        query2 = """CREATE TABLE merged AS 
            SELECT * 
            FROM students 
            JOIN marks 
            USING (student_id)
            JOIN tests
            USING (test_id)
            JOIN courses
            USING (course_id)
            """
        self.db.executeQueryWithoutResult(query1)
        self.db.executeQueryWithoutResult(query2)

    def is_unique(self, df):
        a = df.to_numpy()
        return (a[0] == a).all()

    def weightCheck(self):
        query1 = """
        select course_id, sum(weight) as totalWeight
        from tests
        group by course_id
        """

        result = self.db.executeQueryWithResult(query1)
        return self.is_unique(result['totalWeight'])

    def courseListCorrection(self, row):
        temp = row.courses
        temp2 = []
        for dictionary in temp:
            dictionary['id'] = dictionary.pop('course_id')
            dictionary['name'] = dictionary.pop('course_name')
            temp2.append(dictionary)
        row.courses = temp2
        return row

    def generateJSON(self, pandasDF):
        JSON = pandasDF.groupby(['student_id', 'student_name', 'totalAverage'], as_index=True).apply(
            lambda line: line.groupby('course_id', as_index=True)
                .apply(lambda x: x[['course_id', 'course_name', 'teacher', 'courseAverage']]
                       .to_dict('records'))).reset_index().rename(columns={0: "courses"}).drop('course_id',
                                                                                               axis=1).groupby(
            ['student_id', 'student_name', 'totalAverage'], as_index=True).sum('courses').reset_index().rename(
            columns={0: "courses"}).rename(columns={'student_id': 'id', 'student_name': 'name'}).apply(
            self.courseListCorrection,
            axis=1).to_json(
            orient='records')
        return JSON

    def generateCourseAverage(self):
        self.db.executeQueryWithoutResult('DROP VIEW IF EXISTS addCourseAverage')
        self.db.executeQueryWithoutResult("""
                create view addCourseAverage
                as SELECT *, sum((mark*weight*1.0)/100) over (partition by student_id,course_id) as courseAverage from merged
                """)

    def returnResponse(self, response):
        response = response.replace('\\', '')
        print(response)
        with open(self.output, 'w', encoding='utf-8') as f:
            # json.dump(response, f, ensure_ascii=False, sort_keys=True, indent=2)
            f.write(response)
        self.db.cleanUp()
