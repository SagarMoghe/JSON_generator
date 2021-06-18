import great_expectations as ge
import logging

class SanityChecks:
    """
    This class performs sanity checks on the input CSV using great_expectations package.
    Assumptions: Assumptions I made while designing validator are
     1: numbers are int64.
     2: ID are of type numbers.
    In case of error with while performing test cases due to this assumption
    """

    def __init__(self, courses: str, student: str, tests: str, marks: str):
        if not self.validateCourses(courses):
            raise Exception("Courses file failed validation")
        if not self.validateStudent(student):
            raise Exception("student file failed validation")
        if not self.validatemarks(marks):
            raise Exception("marks file failed validation")
        if not self.validateTests(tests):
            raise Exception("tests file failed validation")
        logging.info("Validation Success!")
    def validateCourses(self, courses: str) -> bool:
        if courses is None:
            raise ValueError("Path to Courses is None")
        with open(courses) as file:
            data = ge.read_csv(file)
            data.expect_column_to_exist('id')
            data.expect_column_to_exist('name')
            data.expect_column_to_exist('teacher')
            data.expect_column_values_to_be_of_type('id', 'int64')
            data.expect_column_values_to_be_of_type('name', 'str')
            data.expect_column_values_to_be_of_type('teacher', 'str')

        return data.validate(result_format='BOOLEAN_ONLY').success

    def validateStudent(self, student: str) -> bool:
        if student is None:
            raise ValueError("Path to Courses is None")
        with open(student) as file:
            data = ge.read_csv(file)
            data.expect_column_to_exist('id')
            data.expect_column_to_exist('name')
            data.expect_column_values_to_be_of_type('id', 'int64')
            data.expect_column_values_to_be_of_type('name', 'str')

        return data.validate(result_format='BOOLEAN_ONLY').success

    def validateTests(self, tests: str) -> bool:
        if tests is None:
            raise ValueError("Path to Courses is None")
        with open(tests) as file:
            data = ge.read_csv(file)
            data.expect_column_to_exist('id')
            data.expect_column_to_exist('course_id')
            data.expect_column_to_exist('weight')
            data.expect_column_values_to_be_of_type('id', 'int64')
            data.expect_column_values_to_be_of_type('course_id', 'int64')
            data.expect_column_values_to_be_of_type('weight', 'int64')

        return data.validate(result_format='BOOLEAN_ONLY').success

    def validatemarks(self, marks: str) -> bool:
        if marks is None:
            raise ValueError("Path to Courses is None")
        with open(marks) as file:
            data = ge.read_csv(file)
            data.expect_column_to_exist('test_id')
            data.expect_column_to_exist('student_id')
            data.expect_column_to_exist('mark')
            data.expect_column_values_to_be_of_type('test_id', 'int64')
            data.expect_column_values_to_be_of_type('student_id', 'int64')
            data.expect_column_values_to_be_of_type('mark', 'int64')

        return data.validate(result_format='BOOLEAN_ONLY').success


SanityChecks("courses.csv", 'students.csv', 'tests.csv', 'marks.csv')
