import SanityChecks
from coreLogic import coreLogic
import pandas as pd


def __init__(courses: str, student: str, tests: str, marks: str, output: str):
    courses = courses
    student = student
    tests = tests
    marks = marks
    output = output
    if SanityChecks.SanityChecks(courses, student, tests, marks):
        coreLogic(courses, student, tests, marks, output).start()


def test():
    courses = "courses.csv"
    student = "students.csv"
    tests = "tests.csv"
    marks = "marks.csv"
    output = "output.json"
    if SanityChecks.SanityChecks(courses, student, tests, marks):
        coreLogic(courses, student, tests, marks, output).start()

test()